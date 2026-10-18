from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from observatorio_secop.warehouse.config import WarehouseConfig

from .snapshot_helpers import bundles, digest, expected_rows, snapshot


def test_supported_recovery_commands_restore_retained_bundle(
    postgres_config: WarehouseConfig, tmp_path: Path
) -> None:
    a, b, _ = bundles(postgres_config)
    root = postgres_config.silver_root
    immutable_hashes = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (root / "versions").rglob("*")
        if path.is_file()
    }
    document = {
        "warehouse": {
            "silver_root": str(root),
            "schema": postgres_config.schema,
            "contract_table": postgres_config.contract_table,
            "audit_table": postgres_config.audit_table,
            "advisory_lock_id": postgres_config.advisory_lock_id,
        }
    }
    config_path = tmp_path / "warehouse.yaml"
    config_path.write_text(yaml.safe_dump(document))
    environment = {
        **os.environ,
        "SILVER_ROOT": str(root),
        "POSTGRES_HOST": postgres_config.host,
        "POSTGRES_PORT": str(postgres_config.port),
        "POSTGRES_DB": postgres_config.dbname,
        "POSTGRES_USER": postgres_config.user,
        "POSTGRES_PASSWORD": postgres_config.password,
    }

    def command(module: str, *arguments: str) -> dict:
        result = subprocess.run(
            [sys.executable, "-m", module, *arguments],
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        return json.loads(result.stdout)

    def rollback(version: str) -> None:
        selected = command("observatorio_secop.processing.silver", "rollback", "--version", version)
        assert selected["silver_version_id"] == version

    def warehouse(action: str) -> dict:
        return command("observatorio_secop.warehouse", "--config", str(config_path), action)

    rollback("A")
    assert warehouse("verify")["manifest_sha256"] == a.manifest_sha256
    assert warehouse("load")["status"] == "applied"
    first = snapshot(postgres_config)
    rollback("B")
    assert warehouse("verify")["manifest_sha256"] == b.manifest_sha256
    assert warehouse("load")["status"] == "applied"
    rollback("A")
    # The Silver pointer alone has not restored the relational snapshot.
    assert snapshot(postgres_config)["contract_current"] == expected_rows(b)
    assert warehouse("verify")["manifest_sha256"] == a.manifest_sha256
    assert warehouse("load")["status"] == "applied"
    after = snapshot(postgres_config)
    assert after["contract_current"] == expected_rows(a)
    assert digest(after["contract_current"]) == digest(first["contract_current"])
    assert warehouse("load")["status"] == "noop"
    assert snapshot(postgres_config) == after
    assert {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (root / "versions").rglob("*")
        if path.is_file()
    } == immutable_hashes
