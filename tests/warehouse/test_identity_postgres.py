from __future__ import annotations

from unittest.mock import Mock

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.identity import IDENTITY_TEXT_FIELDS
from observatorio_secop.warehouse.activation import ActivationStore
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.cli import main
from observatorio_secop.warehouse.errors import WarehouseLoadError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import (
    identity_row,
    rewrite_manifest,
    silver_row,
    write_bundle,
    write_identity_bundle,
)
from .inventory_helpers import cli_environment
from .snapshot_helpers import expected_rows, seed_registered, snapshot


def remove_identity_schema(config):
    with psycopg.connect(**config.connection_kwargs) as database:
        for name in IDENTITY_TEXT_FIELDS:
            database.execute(
                sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
                    sql.Identifier(config.schema, config.contract_table), sql.Identifier(name)
                )
            )
        database.execute(
            sql.SQL("DROP TABLE {}").format(
                sql.Identifier(config.schema, config.audit_table + "_schema")
            )
        )


def column_names(config):
    with psycopg.connect(**config.connection_kwargs) as database:
        return database.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (config.schema, config.contract_table),
        ).fetchall()


def test_identity_schema_migrates_before_noop_and_restores_old_values(postgres_config):
    config = postgres_config
    write_bundle(config.silver_root, "A", [silver_row("A")])
    old = verify_current_bundle(config.silver_root)
    write_identity_bundle(config.silver_root, "B", [identity_row("B")])
    new = verify_current_bundle(config.silver_root)
    seed_registered(config, new, old)
    loader = WarehouseLoader(config)
    loader.load(old)
    before = snapshot(config)
    remove_identity_schema(config)
    assert ("identity_rule_version",) not in column_names(config)
    assert loader.load(old).status == "noop"
    assert snapshot(config) == before
    with psycopg.connect(**config.connection_kwargs) as database:
        assert database.execute(
            sql.SQL("SELECT revision FROM {}").format(
                sql.Identifier(config.schema, config.audit_table + "_schema")
            )
        ).fetchall() == [(2,)]
    assert (
        loader.load(new, restore=True, expected_current=("A", old.manifest_sha256)).updated_rows
        == 1
    )
    assert snapshot(config)[config.contract_table] == expected_rows(new)
    assert (
        loader.load(old, restore=True, expected_current=("B", new.manifest_sha256)).status
        == "applied"
    )
    restored = snapshot(config)
    assert restored[config.contract_table] == expected_rows(old)
    assert loader.load(old).status == "noop"
    assert snapshot(config) == restored
    assert old.table.to_pylist()[0]["supplier_code"] is None


def test_identity_schema_and_metadata_rollback_together(postgres_config, monkeypatch):
    config = postgres_config
    write_bundle(config.silver_root, "A", [silver_row("A")])
    old = verify_current_bundle(config.silver_root)
    loader = WarehouseLoader(config)
    seed_registered(config, old)
    loader.load(old)
    remove_identity_schema(config)
    before, columns = snapshot(config), column_names(config)

    def fail_after_migration(self, cursor):
        raise WarehouseLoadError("Injected failure after identity schema migration")

    with monkeypatch.context() as patch:
        patch.setattr(ActivationStore, "ensure", fail_after_migration)
        with pytest.raises(WarehouseLoadError, match="Injected"):
            loader.load(old)
    assert snapshot(config) == before and column_names(config) == columns
    assert loader.load(old).status == "noop"
    assert ("identity_rule_version",) in column_names(config)


def test_unknown_relational_identity_revision_is_not_silently_reused(postgres_config):
    config = postgres_config
    write_bundle(config.silver_root, "A", [silver_row("A")])
    bundle = verify_current_bundle(config.silver_root)
    loader = WarehouseLoader(config)
    seed_registered(config, bundle)
    loader.load(bundle)
    with psycopg.connect(**config.connection_kwargs) as database:
        relation = sql.Identifier(config.schema, config.audit_table + "_schema")
        database.execute(sql.SQL("DROP TABLE {}").format(relation))
        database.execute(
            sql.SQL("CREATE TABLE {} (singleton boolean, revision integer)").format(relation)
        )
        database.execute(sql.SQL("INSERT INTO {} VALUES (true, 99)").format(relation))
    before = snapshot(config)
    with pytest.raises(WarehouseLoadError, match="Unsupported relational"):
        loader.load(bundle)
    assert snapshot(config) == before


@pytest.mark.parametrize("failure", ["schema", "identity", "refresh"])
def test_invalid_identity_cli_load_never_connects_or_changes_active_snapshot(
    postgres_config,
    monkeypatch,
    capsys,
    failure,
):
    config = postgres_config
    write_bundle(config.silver_root, "A", [silver_row("A")])
    seed_registered(config, verify_current_bundle(config.silver_root))
    WarehouseLoader(config).load(verify_current_bundle(config.silver_root))
    before = snapshot(config)
    row = identity_row("B")
    if failure == "identity":
        row["entity_code"] = "changed-code"
    manifest = write_identity_bundle(config.silver_root, "B", [row])
    if failure == "schema":
        rewrite_manifest(manifest, lambda value: value.update(schema_version=99))
    elif failure == "refresh":
        rewrite_manifest(
            manifest,
            lambda value: value.update(input_purposes={"run-1": "identity_refresh"}, refresh=None),
        )
    args = cli_environment(config.silver_root, monkeypatch, config)
    connect = Mock(side_effect=AssertionError("Invalid identity must fail before database access"))
    with monkeypatch.context() as invocation:
        invocation.setattr("psycopg.connect", connect)
        assert main([*args, "load"]) == 2
        assert "Warehouse load failed" in capsys.readouterr().err
        connect.assert_not_called()
    assert snapshot(config) == before
