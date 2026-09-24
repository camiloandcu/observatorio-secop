from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA, verify_current_bundle
from observatorio_secop.warehouse.cli import main
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import (
    SilverBundleError,
    WarehouseConfigurationError,
)

from .conftest import rewrite_manifest, silver_row, write_bundle


def test_valid_bundle_is_verified(silver_root: Path) -> None:
    bundle = verify_current_bundle(silver_root)

    assert bundle.version_id == "version-1"
    assert bundle.row_count == 1
    assert bundle.table.column_names == EXPECTED_SCHEMA.names


def test_pointer_must_target_immutable_version(silver_root: Path) -> None:
    current_path = silver_root / "current.json"
    current = json.loads(current_path.read_text(encoding="utf-8"))
    current["manifest"] = "../other/manifest.json"
    current_path.write_text(json.dumps(current), encoding="utf-8")

    with pytest.raises(SilverBundleError, match="immutable version"):
        verify_current_bundle(silver_root)


def test_pointer_manifest_hash_is_verified(silver_root: Path) -> None:
    current_path = silver_root / "current.json"
    current = json.loads(current_path.read_text(encoding="utf-8"))
    current["manifest_sha256"] = "0" * 64
    current_path.write_text(json.dumps(current), encoding="utf-8")

    with pytest.raises(SilverBundleError, match="manifest hash"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update(status="invalid"), "identity or status"),
        (
            lambda value: value["quality_results"][0].update(passed=False),
            "failed critical",
        ),
        (lambda value: value["counts"].update(output_rows=2), "row count"),
    ],
)
def test_manifest_failures_are_rejected(silver_root: Path, mutation: object, message: str) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    rewrite_manifest(manifest, mutation)

    with pytest.raises(SilverBundleError, match=message):
        verify_current_bundle(silver_root)


def test_inventory_hash_is_verified(silver_root: Path) -> None:
    parquet = next((silver_root / "versions/version-1/data").rglob("*.parquet"))
    parquet.write_bytes(parquet.read_bytes() + b"changed")

    with pytest.raises(SilverBundleError, match="size differs"):
        verify_current_bundle(silver_root)


def test_schema_difference_is_rejected(tmp_path: Path) -> None:
    changed_schema = pa.schema([field for field in EXPECTED_SCHEMA if field.name != "entity_name"])
    row = silver_row("version-1")
    row.pop("entity_name")
    root = tmp_path / "silver"
    write_bundle(root, "version-1", [row], schema=changed_schema)

    with pytest.raises(SilverBundleError, match="schema differs"):
        verify_current_bundle(root)


def test_duplicate_contract_key_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "silver"
    write_bundle(
        root,
        "version-1",
        [silver_row("version-1"), silver_row("version-1")],
    )

    with pytest.raises(SilverBundleError, match="non-null and unique"):
        verify_current_bundle(root)


def test_configuration_uses_environment_without_exposing_password(tmp_path: Path) -> None:
    config_path = tmp_path / "warehouse.yaml"
    config_path.write_text(
        "warehouse:\n"
        "  silver_root: data/silver\n"
        "  schema: silver\n"
        "  contract_table: contract_current\n"
        "  audit_table: load_audit\n"
        "  advisory_lock_id: 123\n",
        encoding="utf-8",
    )
    environment = {
        "POSTGRES_DB": "secop",
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "secret-value",
        "POSTGRES_PORT": "5432",
    }

    config = load_warehouse_config(config_path, environment=environment)

    assert config.password == "secret-value"
    assert "secret-value" not in repr(config.connection_kwargs.keys())


def test_missing_database_secret_is_named(tmp_path: Path) -> None:
    config_path = tmp_path / "warehouse.yaml"
    config_path.write_text(
        "warehouse: {silver_root: data/silver, schema: silver, "
        "contract_table: contract_current, audit_table: load_audit, advisory_lock_id: 1}\n",
        encoding="utf-8",
    )
    with pytest.raises(WarehouseConfigurationError, match="POSTGRES_PASSWORD"):
        load_warehouse_config(
            config_path,
            environment={"POSTGRES_DB": "secop", "POSTGRES_USER": "user"},
        )


def test_cli_error_does_not_print_password(
    silver_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "warehouse.yaml"
    config_path.write_text(
        "warehouse:\n"
        f"  silver_root: {silver_root}\n"
        "  schema: silver\n"
        "  contract_table: contract_current\n"
        "  audit_table: load_audit\n"
        "  advisory_lock_id: 1\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("POSTGRES_DB", "missing")
    monkeypatch.setenv("POSTGRES_USER", "missing")
    monkeypatch.setenv("POSTGRES_PASSWORD", "never-print-this")
    monkeypatch.setenv("POSTGRES_PORT", "1")

    assert main(["--config", str(config_path), "load"]) == 2
    assert "never-print-this" not in capsys.readouterr().err
