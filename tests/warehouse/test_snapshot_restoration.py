from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import silver_row, write_bundle
from .snapshot_helpers import bundles, digest, expected_rows, snapshot


def test_restore_exact_a_after_b_and_repeat(postgres_config: WarehouseConfig) -> None:
    a, b, pointer_a = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    assert loader.load(a).status == "applied"
    first = snapshot(postgres_config)
    assert first["contract_current"] == expected_rows(a)
    assert loader.load(b).status == "applied"
    (postgres_config.silver_root / "current.json").write_bytes(pointer_a)
    restored = loader.load(verify_current_bundle(postgres_config.silver_root))

    assert restored.status == "applied"
    assert (restored.inserted_rows, restored.updated_rows, restored.deleted_rows) == (1, 1, 1)
    after = snapshot(postgres_config)
    assert after["contract_current"] == expected_rows(a)
    assert digest(after["contract_current"]) == digest(first["contract_current"])
    value_column = a.table.column_names.index("contract_value_cop")
    assert sum(row[value_column] for row in after["contract_current"]) == Decimal("123.46")
    assert {row[1] for row in after["contract_current"]} == {"shared", "only-A"}
    assert len(after["load_audit"]) == 2
    events = after["load_audit_activation"]
    assert [row[1] for row in events] == ["A", "B", "A"]
    assert [row[5] for row in events] == ["activation", "activation", "restoration"]
    assert events[-1][3:5] == (b.version_id, b.manifest_sha256)
    assert after["load_audit_active"][0][1:] == ("A", a.manifest_sha256, events[-1][0])
    repeated = loader.load(a)
    assert repeated.status == "noop"
    assert (repeated.inserted_rows, repeated.updated_rows, repeated.deleted_rows) == (0, 0, 0)
    assert snapshot(postgres_config) == after


@pytest.mark.parametrize("target", ["A", "B"])
def test_conflicting_active_and_inactive_identity_are_unchanged(
    postgres_config: WarehouseConfig, target: str, tmp_path
) -> None:
    a, b, _ = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    loader.load(a)
    loader.load(b)
    original = snapshot(postgres_config)
    # A separately valid bundle attempts to reuse the same identity with new
    # verified content, rather than bypassing the verifier with an altered hash.
    other_root = tmp_path / "conflicting"
    write_bundle(other_root, target, [silver_row(target, amount="999.00")])
    conflicting = verify_current_bundle(other_root)
    with pytest.raises(WarehouseConflictError):
        loader.load(conflicting)
    assert digest(snapshot(postgres_config)) == digest(original)
    assert snapshot(postgres_config) == original


@pytest.mark.parametrize("relation", ["load_audit_activation", "load_audit_active"])
def test_restoration_metadata_failure_preserves_b_and_retry_restores_a(
    postgres_config: WarehouseConfig, relation: str
) -> None:
    a, b, _ = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    loader.load(a)
    loader.load(b)
    original = snapshot(postgres_config)
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        database.execute(
            sql.SQL(
                "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ "
                "BEGIN RAISE EXCEPTION 'injected restoration failure'; END $$"
            ).format(sql.Identifier(postgres_config.schema, "reject_restoration"))
        )
        database.execute(
            sql.SQL(
                "CREATE TRIGGER reject_restoration BEFORE INSERT OR UPDATE ON {} "
                "FOR EACH ROW EXECUTE FUNCTION {}()"
            ).format(
                sql.Identifier(postgres_config.schema, relation),
                sql.Identifier(postgres_config.schema, "reject_restoration"),
            )
        )
    with pytest.raises(WarehouseLoadError):
        loader.load(a)
    assert snapshot(postgres_config) == original
    assert digest(snapshot(postgres_config)) == digest(original)
    with psycopg.connect(**postgres_config.connection_kwargs) as database:
        database.execute(
            sql.SQL("DROP TRIGGER reject_restoration ON {}").format(
                sql.Identifier(postgres_config.schema, relation)
            )
        )
    assert loader.load(a).status == "applied"
    after = snapshot(postgres_config)
    assert after["contract_current"] == expected_rows(a)
    assert len(after["load_audit_activation"]) == 3
    assert after["load_audit_activation"][-1][5] == "restoration"
    assert loader.load(a).status == "noop"


@pytest.mark.parametrize("audit_name", ["bundles", "x" * 52])
def test_custom_relation_names_restore_without_new_configuration(
    postgres_config: WarehouseConfig, audit_name: str
) -> None:
    config = replace(postgres_config, contract_table="contracts", audit_table=audit_name)
    a, b, _ = bundles(config)
    loader = WarehouseLoader(config)
    loader.load(a)
    loader.load(b)
    loader.load(a)
    after = snapshot(config)
    assert after["contracts"] == expected_rows(a)
    assert len(after[audit_name]) == 2
    assert len(after[audit_name + "_activation"]) == 3


@pytest.mark.parametrize("damage", ["hash", "event_identity", "singleton", "registry"])
def test_database_constraints_protect_activation_identity(
    postgres_config: WarehouseConfig, damage: str
) -> None:
    a, b, _ = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    loader.load(a)
    loader.load(b)
    before = snapshot(postgres_config)
    statements = {
        "hash": "UPDATE {active} SET manifest_sha256 = 'wrong'",
        "event_identity": "UPDATE {active} SET silver_version_id = %s, manifest_sha256 = %s",
        "singleton": "UPDATE {active} SET singleton = false",
        "registry": "DELETE FROM {audit} WHERE silver_version_id = 'A'",
    }
    with (
        pytest.raises(psycopg.IntegrityError),
        psycopg.connect(**postgres_config.connection_kwargs) as database,
    ):
        database.execute(
            sql.SQL(statements[damage]).format(
                active=sql.Identifier(postgres_config.schema, "load_audit_active"),
                audit=sql.Identifier(postgres_config.schema, "load_audit"),
            ),
            (a.version_id, a.manifest_sha256) if damage == "event_identity" else None,
        )
    assert snapshot(postgres_config) == before
