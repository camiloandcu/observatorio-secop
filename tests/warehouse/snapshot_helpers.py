"""Exact snapshot evidence for restoration and legacy migration tests."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql

from observatorio_secop.warehouse.activation import ActivationStore
from observatorio_secop.warehouse.bundle import (
    EXPECTED_SCHEMA,
    IDENTITY_SCHEMA,
    VerifiedSilverBundle,
    verify_current_bundle,
)
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.loader import WarehouseLoader
from observatorio_secop.warehouse.migration import migrate_identity

from .conftest import silver_row, write_bundle


def canonical(value: Any) -> str:
    def typed(item: Any) -> Any:
        if isinstance(item, datetime | date):
            if isinstance(item, datetime) and item.tzinfo:
                item = item.astimezone(UTC)
            return [type(item).__name__, item.isoformat()]
        if isinstance(item, Decimal):
            return ["decimal", str(item)]
        if isinstance(item, dict):
            return {key: typed(value) for key, value in sorted(item.items())}
        if isinstance(item, list | tuple):
            return [typed(value) for value in item]
        return item

    return json.dumps(typed(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def snapshot(config: WarehouseConfig) -> dict[str, Any]:
    result = {}
    with psycopg.connect(**config.connection_kwargs) as database:
        for name in [
            config.contract_table,
            config.audit_table,
            config.audit_table + "_activation",
            config.audit_table + "_active",
            config.audit_table + "_schema",
        ]:
            relation = sql.Identifier(config.schema, name)
            if database.execute("SELECT to_regclass(%s)", (relation.as_string(),)).fetchone()[0]:
                # Each relation has its stable primary key as column 1, except
                # contracts whose first two columns are version and contract key.
                columns = sql.SQL("*")
                if name == config.contract_table:
                    has_identity = database.execute(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema=%s AND table_name=%s "
                        "AND column_name='identity_rule_version')",
                        (config.schema, name),
                    ).fetchone()[0]
                    if has_identity:
                        columns = sql.SQL(",").join(
                            map(
                                sql.Identifier,
                                IDENTITY_SCHEMA.names + ["silver_manifest_sha256", "loaded_at"],
                            )
                        )
                result[name] = database.execute(
                    sql.SQL("SELECT {} FROM {} ORDER BY 1, 2").format(columns, relation)
                ).fetchall()
            else:
                result[name] = None
    return result


def bundles(config: WarehouseConfig) -> tuple[VerifiedSilverBundle, VerifiedSilverBundle, bytes]:
    root = config.silver_root
    a_shared = silver_row("A", "shared", amount="100.01", updated_at=None, entity_name=None)
    a_shared.update(bronze_run_id="bronze-A", quality_warning_codes=["EXAMPLE"])
    a_only = silver_row("A", "only-A", amount="23.45", contract_id="CO1.A")
    write_bundle(root, "A", [a_shared, a_only])
    a = verify_current_bundle(root)
    pointer_a = (root / "current.json").read_bytes()
    b_shared = silver_row("B", "shared", amount="200.02")
    b_shared.update(bronze_run_id="bronze-B", bronze_row_ordinal=12)
    write_bundle(
        root, "B", [b_shared, silver_row("B", "only-B", contract_id="CO1.B", amount="50.00")]
    )
    return a, verify_current_bundle(root), pointer_a


def expected_rows(bundle: VerifiedSilverBundle) -> list[tuple[Any, ...]]:
    return [
        tuple(row[name] for name in bundle.table.column_names)
        + (bundle.manifest_sha256, bundle.manifest_finished_at)
        for row in sorted(bundle.table.to_pylist(), key=lambda row: row["contract_key"])
    ]


def seed_legacy(
    config: WarehouseConfig,
    a: VerifiedSilverBundle,
    b: VerifiedSilverBundle,
    *,
    registered_history: bool = False,
    registered_bundles: tuple[VerifiedSilverBundle, ...] | None = None,
) -> None:
    """Use the unchanged pre-metadata DDL and old first-load audit shape."""
    with (
        psycopg.connect(**config.connection_kwargs) as database,
        database.cursor() as cursor,
    ):
        WarehouseLoader._ensure_relations(
            cursor,
            sql.Identifier(config.schema),
            sql.Identifier(config.contract_table),
            sql.Identifier(config.audit_table),
        )
        registered = (
            list(registered_bundles)
            if registered_bundles
            else ([a] if a.version_id == b.version_id else [a, b])
        )
        for index, bundle in enumerate(registered):
            counts = (bundle.row_count, 0, 0) if index == 0 else (1, 1, 1)
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'applied')").format(
                    sql.Identifier(config.schema, config.audit_table)
                ),
                (
                    bundle.version_id,
                    bundle.manifest_sha256,
                    bundle.manifest_finished_at,
                    datetime(2026, 1, 5, tzinfo=UTC),
                    bundle.row_count,
                    *counts,
                ),
            )
        metadata = ActivationStore(config)
        if registered_history:
            assert metadata.ensure(cursor) is None
            previous = None
            for index, bundle in enumerate(registered):
                identity = (bundle.version_id, bundle.manifest_sha256)
                counts = (bundle.row_count, 0, 0) if index == 0 else (1, 1, 1)
                metadata.activate(
                    cursor, identity, previous, "activation", bundle.row_count, *counts
                )
                previous = identity
        schema = EXPECTED_SCHEMA
        if b.schema_version == 2:
            migrate_identity(cursor, metadata.current, metadata.schema_revision)
            schema = IDENTITY_SCHEMA
        columns = schema.names + ["silver_manifest_sha256", "loaded_at"]
        with cursor.copy(
            sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(config.schema, config.contract_table),
                sql.SQL(",").join(map(sql.Identifier, columns)),
            )
        ) as copy:
            for row in sorted(b.table.to_pylist(), key=lambda row: row["contract_key"]):
                copy.write_row(
                    tuple(row[name] for name in schema.names)
                    + (b.manifest_sha256, b.manifest_finished_at)
                )


def seed_registered(
    config: WarehouseConfig,
    a: VerifiedSilverBundle,
    *subsequent: VerifiedSilverBundle,
) -> None:
    """Authentic pre-EN02 SQL snapshot with immutable audit/history, active B.

    This fixture never calls the new loader to perform a forbidden initial legacy
    activation. Tests exercise its actual no-op/migration/restoration afterwards.
    """
    seed_legacy(
        config,
        a,
        subsequent[-1] if subsequent else a,
        registered_history=True,
        registered_bundles=(a, *subsequent),
    )


def advance_registered(config: WarehouseConfig, bundle: VerifiedSilverBundle) -> None:
    """Append a pre-EN02 historical fixture without replacing derived test schemas.

    This is setup for inherited unscoped pipeline scenarios, never evidence that
    the current loader permits new unscoped publications.
    """
    with psycopg.connect(**config.connection_kwargs) as database, database.cursor() as cursor:
        metadata = ActivationStore(config)
        active = metadata.ensure(cursor)
        identity = (bundle.version_id, bundle.manifest_sha256)
        if active == identity:
            return
        migrate_identity(cursor, metadata.current, metadata.schema_revision)
        cursor.execute(sql.SQL("SELECT contract_key FROM {}").format(metadata.current))
        before = {row[0] for row in cursor.fetchall()}
        after = set(bundle.table["contract_key"].to_pylist())
        changes = (len(after - before), len(after & before), len(before - after))
        cursor.execute(
            sql.SQL("SELECT manifest_sha256 FROM {} WHERE silver_version_id=%s").format(
                metadata.audit
            ),
            (bundle.version_id,),
        )
        prior = cursor.fetchone()
        assert prior is None or prior == (bundle.manifest_sha256,)
        if prior is None:
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'applied')").format(
                    metadata.audit
                ),
                (
                    bundle.version_id,
                    bundle.manifest_sha256,
                    bundle.manifest_finished_at,
                    datetime(2026, 1, 5, tzinfo=UTC),
                    bundle.row_count,
                    *changes,
                ),
            )
        cursor.execute(sql.SQL("DELETE FROM {}").format(metadata.current))
        columns = IDENTITY_SCHEMA.names + ["silver_manifest_sha256", "loaded_at"]
        with cursor.copy(
            sql.SQL("COPY {} ({}) FROM STDIN").format(
                metadata.current,
                sql.SQL(",").join(map(sql.Identifier, columns)),
            )
        ) as copy:
            for row in bundle.table.to_pylist():
                copy.write_row(
                    tuple(row[name] for name in IDENTITY_SCHEMA.names)
                    + (bundle.manifest_sha256, bundle.manifest_finished_at)
                )
        metadata.activate(
            cursor,
            identity,
            active,
            "restoration" if prior else "activation",
            bundle.row_count,
            *changes,
        )
