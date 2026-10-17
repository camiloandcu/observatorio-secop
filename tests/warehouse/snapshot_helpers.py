"""Exact snapshot evidence for restoration and legacy migration tests."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql

from observatorio_secop.warehouse.bundle import VerifiedSilverBundle, verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.loader import WarehouseLoader

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
        ]:
            relation = sql.Identifier(config.schema, name)
            if database.execute("SELECT to_regclass(%s)", (relation.as_string(),)).fetchone()[0]:
                # Each relation has its stable primary key as column 1, except
                # contracts whose first two columns are version and contract key.
                result[name] = database.execute(
                    sql.SQL("SELECT * FROM {} ORDER BY 1, 2").format(relation)
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


def seed_legacy(config: WarehouseConfig, a: VerifiedSilverBundle, b: VerifiedSilverBundle) -> None:
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
        columns = b.table.column_names + ["silver_manifest_sha256", "loaded_at"]
        with cursor.copy(
            sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(config.schema, config.contract_table),
                sql.SQL(",").join(map(sql.Identifier, columns)),
            )
        ) as copy:
            for row in expected_rows(b):
                copy.write_row(row)
        for bundle, counts in [(a, (2, 0, 0)), (b, (1, 1, 1))]:
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
