"""Capture one verified warehouse snapshot without bypassing population ancestry."""

from __future__ import annotations

import json
import re
import time
from datetime import UTC
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from observatorio_secop.identity import IDENTITY_RULE_VERSION
from observatorio_secop.population import PopulationScope, digest_json
from observatorio_secop.publication.models import candidate_lock_id
from observatorio_secop.warehouse.activation import ActivationStore
from observatorio_secop.warehouse.bundle import (
    IDENTITY_SCHEMA,
    VerifiedSilverBundle,
    verify_manifest_bundle,
)
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.population import PopulationStore


def _identity(publication_id: str) -> str:
    if not isinstance(publication_id, str) or not re.fullmatch(r"[0-9a-f]{32}", publication_id):
        raise ValueError("PUBLICATION_INVALID_ID")
    return publication_id


def _positive(value: int, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"PUBLICATION_INVALID_{name}")
    return value


def _connect(
    config: WarehouseConfig, *, autocommit: bool = False
) -> psycopg.Connection[tuple[Any, ...]]:
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.dbname,
        user=config.user,
        password=config.password,
        connect_timeout=10,
        autocommit=autocommit,
    )


def _one(cursor: psycopg.Cursor[Any]) -> tuple[Any, ...]:
    row = cursor.fetchone()
    if row is None:
        raise ValueError("PUBLICATION_QUERY_RESULT_MISSING")
    return tuple(row)


def capture_inputs(
    config: WarehouseConfig,
    bundle: VerifiedSilverBundle,
    publication_id: str,
    *,
    storage_budget_bytes: int,
    lock_timeout_ms: int = 5000,
    statement_timeout_ms: int = 30000,
) -> dict[str, Any]:
    """Verify before connecting, then copy all six inputs under the loader lock.

    The caller reserves the publication slot first. Only an existing governed active
    snapshot is eligible; this never activates a child in an empty workspace.
    """
    publication_id = _identity(publication_id)
    _positive(storage_budget_bytes, "STORAGE_BUDGET")
    _positive(lock_timeout_ms, "LOCK_TIMEOUT")
    _positive(statement_timeout_ms, "STATEMENT_TIMEOUT")
    bundle = verify_manifest_bundle(bundle.manifest_path, expected_hash=bundle.manifest_sha256)
    started = time.monotonic()
    manifest = json.loads(bundle.manifest_path.read_bytes())
    identity = (bundle.version_id, bundle.manifest_sha256)
    namespace = f"candidate_{publication_id}_input"
    suffixes = (
        "",
        "_population",
        "_population_binding",
        "_observation_condition",
        "_observation_link",
        "_quality",
    )
    with _connect(config) as database, database.cursor() as cursor:
        cursor.execute("SELECT set_config('lock_timeout',%s,true)", (f"{lock_timeout_ms}ms",))
        cursor.execute(
            "SELECT set_config('statement_timeout',%s,true)", (f"{statement_timeout_ms}ms",)
        )
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (candidate_lock_id(publication_id),))
        if cursor.fetchone() != (True,):
            raise ValueError("PUBLICATION_BUSY")
        lock_started = time.monotonic()
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", (config.advisory_lock_id,))
        source_lock_wait = time.monotonic() - lock_started
        reserved_metadata = _check_reserved(cursor, publication_id, identity)
        if ActivationStore(config)._read_active(cursor) != identity:
            raise ValueError("PUBLICATION_SOURCE_CHANGED")
        population = PopulationStore(config)
        population.guard(cursor, bundle, identity, registered=True)
        population._verify_existing(cursor, bundle)
        _check_contract_rows(cursor, config, bundle)
        _check_quality(cursor, config, bundle)
        cursor.execute(
            sql.SQL("SELECT scope_id,scope FROM {} WHERE singleton").format(population.binding)
        )
        scope = cursor.fetchone()
        scope_id = scope[0] if scope else None
        if scope and PopulationScope.from_dict(scope[1]).scope_id != scope_id:
            raise ValueError("PUBLICATION_SCOPE_CHANGED")
        if bundle.coverage and scope_id != bundle.coverage["scope_id"]:
            raise ValueError("PUBLICATION_SCOPE_CHANGED")
        cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname=%s", (namespace,))
        if cursor.fetchone():
            raise ValueError("PUBLICATION_CAPTURE_ALREADY_EXISTS")
        # Conservatively count all source storage at preflight, including historical
        # rows. The copy itself is filtered to this exact snapshot identity.
        source_bytes = 0
        for suffix in suffixes:
            relation = sql.Identifier(config.schema, config.contract_table + suffix)
            cursor.execute("SELECT pg_total_relation_size(%s::regclass)", (relation.as_string(),))
            source_bytes += int(_one(cursor)[0])
        if source_bytes > storage_budget_bytes:
            raise ValueError("PUBLICATION_STORAGE_CAPACITY")
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
        copied_bytes = 0
        for suffix in suffixes:
            source = sql.Identifier(config.schema, config.contract_table + suffix)
            target = sql.Identifier(namespace, "contract_current" + suffix)
            where = sql.SQL("")
            parameters: tuple[Any, ...] = ()
            if suffix not in ("", "_population_binding"):
                where = sql.SQL(" WHERE silver_version_id=%s AND manifest_sha256=%s")
                parameters = identity
            cursor.execute(
                sql.SQL("CREATE TABLE {} AS SELECT * FROM {}{}").format(target, source, where),
                parameters,
            )
            cursor.execute(sql.SQL("REVOKE ALL ON {} FROM PUBLIC").format(target))
            cursor.execute(
                sql.SQL("GRANT SELECT ON {} TO secop_publication_builder").format(target)
            )
            cursor.execute("SELECT pg_total_relation_size(%s::regclass)", (target.as_string(),))
            copied_bytes += int(_one(cursor)[0])
        if copied_bytes > storage_budget_bytes:
            raise ValueError("PUBLICATION_STORAGE_CAPACITY")
        cursor.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(namespace))
        )
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO secop_publication_builder").format(
                sql.Identifier(namespace)
            )
        )
        cursor.execute("SELECT clock_timestamp()")
        captured_at = _one(cursor)[0].isoformat()
        cursor.execute("SELECT pg_database_size(current_database())")
        database_bytes = int(_one(cursor)[0])
        coverage = bundle.coverage or {}
        metadata = {
            "silver_version_id": bundle.version_id,
            "silver_manifest_sha256": bundle.manifest_sha256,
            "scope_id": scope_id,
            "scope": scope[1] if scope else None,
            "source_schema_version": bundle.schema_version,
            "identity_rule_version": IDENTITY_RULE_VERSION,
            "source_rows": bundle.row_count,
            "captured_at": captured_at,
            "observed_at": max(bundle.table["bronze_fetched_at_utc"].to_pylist())
            .replace(tzinfo=UTC)
            .isoformat()
            if bundle.row_count
            else None,
            "pipeline_completed_at": bundle.manifest_finished_at.astimezone(UTC).isoformat(),
            "coverage_state": coverage.get("state", "unknown"),
            "coverage_known": coverage.get("coverage_known", bool(bundle.coverage)),
            "source_dataset_id": manifest.get("source_dataset_id", "jbjy-vk9h"),
            "copied_input_bytes": copied_bytes,
            "source_storage_bytes": source_bytes,
            "capture_seconds": round(time.monotonic() - started, 6),
            "source_lock_wait_seconds": round(source_lock_wait, 6),
            "database_bytes_after_capture": database_bytes,
            "storage_budget_bytes": storage_budget_bytes,
        }
        if any(
            key in reserved_metadata and reserved_metadata[key] != value
            for key, value in metadata.items()
        ):
            raise ValueError("PUBLICATION_SOURCE_CONFLICT")
        # A crash after capture must leave both the exact copied namespace and its
        # resume receipt, never a committed namespace that retries try to recapture.
        cursor.execute(
            "UPDATE publication_control.publications SET metadata=%s WHERE publication_id=%s",
            (Jsonb(reserved_metadata | metadata), publication_id),
        )
    return metadata


def _check_reserved(
    cursor: psycopg.Cursor[Any], publication_id: str, identity: tuple[str, str]
) -> dict[str, Any]:
    cursor.execute(
        "SELECT state,metadata FROM publication_control.publications "
        "WHERE publication_id=%s FOR SHARE",
        (publication_id,),
    )
    row = cursor.fetchone()
    if row is None or row[0] != "building":
        raise ValueError("PUBLICATION_NOT_RESERVED")
    metadata = row[1]
    if any(
        metadata.get(key) is not None and metadata.get(key) != value
        for key, value in zip(
            ("silver_version_id", "silver_manifest_sha256"), identity, strict=True
        )
    ):
        raise ValueError("PUBLICATION_SOURCE_CONFLICT")
    return dict(metadata)


def _check_contract_rows(
    cursor: psycopg.Cursor[Any], config: WarehouseConfig, bundle: VerifiedSilverBundle
) -> None:
    names = IDENTITY_SCHEMA.names
    cursor.execute(
        sql.SQL("SELECT {},silver_manifest_sha256,loaded_at FROM {} ORDER BY contract_key").format(
            sql.SQL(",").join(map(sql.Identifier, names)),
            sql.Identifier(config.schema, config.contract_table),
        )
    )
    rows = cursor.fetchall()
    expected = [
        tuple(row[name] for name in names) + (bundle.manifest_sha256, bundle.manifest_finished_at)
        for row in sorted(bundle.table.to_pylist(), key=lambda row: row["contract_key"])
    ]
    if rows != expected:
        raise ValueError("PUBLICATION_SOURCE_CONTENT_CHANGED")


def _check_quality(
    cursor: psycopg.Cursor[Any], config: WarehouseConfig, bundle: VerifiedSilverBundle
) -> None:
    expected = bundle.quality_summary or {
        "schema_version": "silver-quality-summary-v1",
        "state": "UNAVAILABLE_LEGACY",
        "reason": "LEGACY_EVIDENCE_ABSENT",
        "counts": None,
        "controls": [],
        "quality_results_sha256": None,
        "bronze_runs": [],
        "pipeline_completed_at_utc": bundle.manifest_finished_at.isoformat(),
    }
    cursor.execute(
        sql.SQL(
            "SELECT scope_id,summary,evidence_sha256 FROM {} "
            "WHERE silver_version_id=%s AND manifest_sha256=%s"
        ).format(sql.Identifier(config.schema, config.contract_table + "_quality")),
        (bundle.version_id, bundle.manifest_sha256),
    )
    scope_id = bundle.coverage["scope_id"] if bundle.coverage else None
    if cursor.fetchone() != (scope_id, expected, digest_json(expected)):
        raise ValueError("PUBLICATION_QUALITY_CHANGED")
