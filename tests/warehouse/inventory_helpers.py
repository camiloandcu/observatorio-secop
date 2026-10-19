"""Controlled inventory mutations for verifier and relational preservation tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA
from observatorio_secop.warehouse.config import WarehouseConfig

from .conftest import rewrite_manifest, silver_row

INVALID_CASES = ["extra", "missing", "size", "hash", "duplicate", "alias", "escape", "symlink"]


def fingerprint(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def inventory_entry(path: Path, directory: Path) -> dict:
    return {
        "path": path.relative_to(directory).as_posix(),
        "size_bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def declare(manifest: Path, path: Path) -> None:
    rewrite_manifest(
        manifest, lambda document: document["files"].append(inventory_entry(path, manifest.parent))
    )


def add_extra(manifest: Path, relative: str = "data/signing_year=2026/extra.parquet") -> Path:
    path = manifest.parent / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    row = silver_row("version-1", "extra-key", contract_id="CO1.EXTRA")
    table = pa.Table.from_pylist([row], schema=EXPECTED_SCHEMA).drop(["signing_year"])
    pq.write_table(table, path)
    rewrite_manifest(manifest, lambda document: document["counts"].update(output_rows=2))
    return path


def cli_environment(
    root: Path, monkeypatch: pytest.MonkeyPatch, config: WarehouseConfig | None = None
) -> list[str]:
    for name, value in {
        "SILVER_ROOT": str(root),
        "POSTGRES_HOST": config.host if config else "127.0.0.1",
        "POSTGRES_PORT": str(config.port) if config else "1",
        "POSTGRES_DB": config.dbname if config else "test",
        "POSTGRES_USER": config.user if config else "test",
        "POSTGRES_PASSWORD": config.password if config else "test-only",
    }.items():
        monkeypatch.setenv(name, value)
    if config is None:
        return []
    path = root.parent / "warehouse.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "warehouse": {
                    "silver_root": str(root),
                    "schema": config.schema,
                    "contract_table": config.contract_table,
                    "audit_table": config.audit_table,
                    "advisory_lock_id": config.advisory_lock_id,
                }
            }
        )
    )
    return ["--config", str(path)]


def corrupt_inventory(manifest: Path, case: str) -> str:
    entry = json.loads(manifest.read_text())["files"][0]
    path = manifest.parent / entry["path"]
    if case == "extra":
        add_extra(manifest)
        return "undeclared data"
    if case == "missing":
        path.unlink()
        return "missing"
    if case == "size":
        path.write_bytes(path.read_bytes() + b"changed")
        return "size differs"
    if case == "hash":
        content = path.read_bytes()
        path.write_bytes(bytes([content[0] ^ 1]) + content[1:])
        return "hash differs"
    if case == "duplicate":
        rewrite_manifest(manifest, lambda document: document["files"].append(entry.copy()))
        return "duplicated"
    if case == "alias":
        alias = {**entry, "path": "data/../" + entry["path"]}
        rewrite_manifest(manifest, lambda document: document["files"].append(alias))
        return "canonical"
    if case == "escape":
        outside = manifest.parent.parent / "outside.parquet"
        outside.write_bytes(path.read_bytes())
        rewrite_manifest(
            manifest, lambda document: document["files"][0].update(path="../outside.parquet")
        )
        return "canonical"
    if case == "symlink":
        target = path.with_suffix(".retained")
        path.rename(target)
        path.symlink_to(target.name)
        return "symlink"
    raise AssertionError(f"Unknown inventory mutation: {case}")
