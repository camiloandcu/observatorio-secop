from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

from observatorio_secop.warehouse.config import WarehouseConfig

from .snapshot_helpers import bundles, digest, expected_rows, seed_registered, snapshot


def test_supported_recovery_commands_restore_retained_bundle(
    postgres_config: WarehouseConfig, tmp_path: Path
) -> None:
    a, b, _ = bundles(postgres_config)
    root = postgres_config.silver_root
    seed_registered(postgres_config, a, b)
    (root / "selection_registry.json").write_text(
        json.dumps(
            {
                a.version_id: a.manifest_sha256,
                b.version_id: b.manifest_sha256,
            }
        )
    )
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

    def rollback(target, current) -> None:
        selected = command(
            "observatorio_secop.processing.silver",
            "restore",
            "--version",
            target.version_id,
            "--manifest-sha256",
            target.manifest_sha256,
            "--expected-current-version",
            current.version_id,
            "--expected-current-manifest-sha256",
            current.manifest_sha256,
        )
        assert selected["silver_version_id"] == target.version_id

    def warehouse(action: str, target=None, current=None) -> dict:
        arguments = []
        if target is not None:
            arguments += [
                "--version",
                target.version_id,
                "--manifest-sha256",
                target.manifest_sha256,
                "--expected-current-version",
                current.version_id,
                "--expected-current-hash",
                current.manifest_sha256,
            ]
        return command(
            "observatorio_secop.warehouse", "--config", str(config_path), action, *arguments
        )

    rollback(a, b)
    assert warehouse("verify")["manifest_sha256"] == a.manifest_sha256
    assert warehouse("restore", a, b)["status"] == "applied"
    first = snapshot(postgres_config)
    rollback(b, a)
    assert warehouse("verify")["manifest_sha256"] == b.manifest_sha256
    assert warehouse("restore", b, a)["status"] == "applied"
    rollback(a, b)
    # The Silver pointer alone has not restored the relational snapshot.
    assert snapshot(postgres_config)["contract_current"] == expected_rows(b)
    assert warehouse("verify")["manifest_sha256"] == a.manifest_sha256
    assert warehouse("restore", a, b)["status"] == "applied"
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
