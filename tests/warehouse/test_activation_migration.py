from __future__ import annotations

from dataclasses import replace

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA, IDENTITY_SCHEMA
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .snapshot_helpers import bundles, expected_rows, seed_legacy, seed_registered, snapshot


def test_legacy_baseline_uses_rows_and_preserves_audit(postgres_config: WarehouseConfig) -> None:
    a, b, _ = bundles(postgres_config)
    seed_legacy(postgres_config, a, b)
    original = snapshot(postgres_config)
    loader = WarehouseLoader(postgres_config)
    assert loader.load(b).status == "noop"
    migrated = snapshot(postgres_config)
    assert migrated["load_audit"] == original["load_audit"]
    new_order = IDENTITY_SCHEMA.names + ["silver_manifest_sha256", "loaded_at"]
    old_order = EXPECTED_SCHEMA.names + ["silver_manifest_sha256", "loaded_at"]
    assert [
        tuple(row[new_order.index(name)] for name in old_order)
        for row in migrated["contract_current"]
    ] == original["contract_current"]
    baseline = migrated["load_audit_activation"]
    assert len(baseline) == 1
    assert baseline[0][1:6] == ("B", b.manifest_sha256, None, None, "legacy_baseline")
    assert baseline[0][8:] == (None, None, None)
    assert loader.load(b).status == "noop"
    assert snapshot(postgres_config) == migrated
    assert (
        loader.load(a, restore=True, expected_current=("B", b.manifest_sha256)).status == "applied"
    )
    restored = snapshot(postgres_config)
    assert restored["load_audit"] == original["load_audit"]
    assert restored["contract_current"] == expected_rows(a)
    assert [event[5] for event in restored["load_audit_activation"]] == [
        "legacy_baseline",
        "restoration",
    ]
    assert loader.load(a).status == "noop"
    assert snapshot(postgres_config) == restored


def test_empty_legacy_current_does_not_infer_active(postgres_config: WarehouseConfig) -> None:
    a, b, _ = bundles(postgres_config)
    seed_legacy(postgres_config, a, b)
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        database.execute(
            sql.SQL("DELETE FROM {}").format(
                sql.Identifier(postgres_config.schema, postgres_config.contract_table)
            )
        )
    original = snapshot(postgres_config)
    assert (
        WarehouseLoader(postgres_config).load(a, restore=True, expected_current=None).status
        == "applied"
    )
    after = snapshot(postgres_config)
    assert after["load_audit"] == original["load_audit"]
    assert after["contract_current"] == expected_rows(a)
    assert len(after["load_audit_activation"]) == 1
    assert after["load_audit_activation"][0][3:6] == (None, None, "restoration")


@pytest.mark.parametrize(
    "change",
    [
        "UPDATE {current} SET silver_version_id = 'A' WHERE contract_key = 'shared'",
        "UPDATE {current} SET silver_manifest_sha256 = 'wrong' WHERE contract_key = 'shared'",
        "UPDATE {current} SET loaded_at = loaded_at + interval '1 second'",
        "DELETE FROM {current} WHERE contract_key = 'shared'",
        "DELETE FROM {audit} WHERE silver_version_id = 'B'",
        "UPDATE {audit} SET manifest_sha256 = 'wrong' WHERE silver_version_id = 'B'",
        "UPDATE {audit} SET source_rows = 999 WHERE silver_version_id = 'B'",
    ],
)
def test_inconsistent_legacy_state_rolls_back_migration(
    postgres_config: WarehouseConfig, change: str
) -> None:
    a, b, _ = bundles(postgres_config)
    seed_legacy(postgres_config, a, b)
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        database.execute(
            sql.SQL(change).format(
                current=sql.Identifier(postgres_config.schema, postgres_config.contract_table),
                audit=sql.Identifier(postgres_config.schema, postgres_config.audit_table),
            )
        )
    original = snapshot(postgres_config)
    with pytest.raises(WarehouseLoadError, match="Legacy warehouse snapshot"):
        WarehouseLoader(postgres_config).load(a)
    assert snapshot(postgres_config) == original
    # Transactional migration also leaves no new index or activation relations.
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        assert (
            database.execute(
                "SELECT to_regclass(%s)", (postgres_config.schema + ".load_audit_identity",)
            ).fetchone()[0]
            is None
        )


def test_legacy_hash_conflict_does_not_migrate(postgres_config: WarehouseConfig) -> None:
    a, b, _ = bundles(postgres_config)
    seed_legacy(postgres_config, a, b)
    original = snapshot(postgres_config)
    with pytest.raises(WarehouseConflictError):
        WarehouseLoader(postgres_config).load(replace(a, manifest_sha256="f" * 64))
    assert snapshot(postgres_config) == original


@pytest.mark.parametrize("missing", ["active", "activation"])
def test_partial_metadata_fails_safely(postgres_config: WarehouseConfig, missing: str) -> None:
    a, _, _ = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    seed_registered(postgres_config, a)
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        database.execute(
            sql.SQL("DROP TABLE {} CASCADE").format(
                sql.Identifier(postgres_config.schema, "load_audit_" + missing)
            )
        )
    original = snapshot(postgres_config)
    with pytest.raises(WarehouseLoadError, match="metadata is incomplete"):
        loader.load(a)
    assert snapshot(postgres_config) == original


@pytest.mark.parametrize("damage", ["active", "history"])
def test_missing_active_or_history_is_not_silently_repaired(
    postgres_config: WarehouseConfig, damage: str
) -> None:
    # A missing active row is possible without breaking FKs. History deletion is
    # exercised with TRUNCATE CASCADE to represent externally damaged metadata.
    a, _, _ = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    seed_registered(postgres_config, a)
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        statement = "TRUNCATE {events} CASCADE" if damage == "history" else "DELETE FROM {active}"
        database.execute(
            sql.SQL(statement).format(
                active=sql.Identifier(postgres_config.schema, "load_audit_active"),
                events=sql.Identifier(postgres_config.schema, "load_audit_activation"),
            )
        )
    original = snapshot(postgres_config)
    with pytest.raises(WarehouseLoadError):
        loader.load(a)
    assert snapshot(postgres_config) == original
