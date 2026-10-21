from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.identity import IDENTITY_RULE_VERSION, code_identity, load_identity_rules
from observatorio_secop.ingestion.compatibility import CompatibilityRegistry
from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA, IDENTITY_SCHEMA
from observatorio_secop.warehouse.config import WarehouseConfig


def silver_row(
    version_id: str,
    contract_key: str = "contract-1",
    *,
    contract_id: str = "CO1.PCCNTR.1",
    amount: str = "100.00",
    municipality_status: str = "KNOWN",
    municipality_key: str | None = "MEDELLIN",
    municipality_name: str | None = "Medellín",
    entity_name: str | None = "Entidad de prueba",
    procurement_method: str | None = "Contratación directa",
    updated_at: datetime | None = datetime(2026, 1, 2),
) -> dict[str, Any]:
    return {
        "silver_version_id": version_id,
        "contract_key": contract_key,
        "source_dataset_id": "jbjy-vk9h",
        "source_contract_id": contract_id,
        "contract_reference": f"REF-{contract_id}",
        "procurement_process_id": f"PROC-{contract_id}",
        "entity_name": entity_name,
        "department_raw": "Antioquia",
        "department_name": "Antioquia",
        "department_key": "ANTIOQUIA",
        "municipality_raw": municipality_name,
        "municipality_name": municipality_name,
        "municipality_key": municipality_key,
        "municipality_status": municipality_status,
        "is_medellin": municipality_key == "MEDELLIN",
        "is_valle_de_aburra": municipality_key == "MEDELLIN",
        "contract_status": "Activo",
        "main_category_code": "V1.1",
        "process_description": "Proceso sintético",
        "contract_type": "Servicios",
        "procurement_method": procurement_method,
        "signing_date_raw": "2026-01-01T00:00:00.000",
        "signing_date": date(2026, 1, 1),
        "start_date_raw": "2026-01-02T00:00:00.000",
        "start_date": date(2026, 1, 2),
        "end_date_raw": "2026-12-31T00:00:00.000",
        "end_date": date(2026, 12, 31),
        "contract_value_raw": amount,
        "contract_value_cop": Decimal(amount),
        "source_updated_at_raw": updated_at.isoformat() if updated_at else None,
        "source_updated_at": updated_at,
        "source_url": "https://example.test/contracts/1",
        "quality_warning_codes": (
            ["UNKNOWN_MUNICIPALITY"] if municipality_status == "UNKNOWN" else []
        ),
        "payload_sha256": hashlib.sha256(f"{contract_key}|{amount}".encode()).hexdigest(),
        "bronze_run_id": "run-1",
        "bronze_page_path": "pages/primary-00001.json",
        "bronze_lane": "primary",
        "bronze_page_sequence": 1,
        "bronze_row_ordinal": 0,
        "bronze_fetched_at_utc": datetime(2026, 1, 3),
        "signing_year": "2026",
    }


def write_bundle(
    root: Path,
    version_id: str,
    rows: list[dict[str, Any]],
    *,
    status: str = "valid",
    critical_passed: bool = True,
    schema: pa.Schema = EXPECTED_SCHEMA,
    parquet_options: dict[str, Any] | None = None,
) -> Path:
    version = root / "versions" / version_id
    data = version / "data"
    data.mkdir(parents=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_to_dataset(
        table,
        root_path=data,
        partition_cols=["signing_year"],
        compression="snappy",
        **(parquet_options or {}),
    )
    files = []
    for path in sorted(item for item in version.rglob("*") if item.is_file()):
        relative = path.relative_to(version).as_posix()
        files.append(
            {
                "path": relative,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = {
        "silver_version_id": version_id,
        "status": status,
        "finished_at_utc": "2026-01-04T00:00:00+00:00",
        "counts": {"output_rows": len(rows)},
        "quality_results": [
            {"check_id": "SCHEMA", "severity": "critical", "passed": critical_passed}
        ],
        "files": files,
    }
    manifest_path = version / "manifest.json"
    manifest_path.write_text(_json(manifest), encoding="utf-8")
    current = {
        "silver_version_id": version_id,
        "manifest": f"versions/{version_id}/manifest.json",
        "manifest_sha256": _sha256(manifest_path),
    }
    (root / "current.json").write_text(_json(current), encoding="utf-8")
    return manifest_path


def identity_row(
    version_id: str,
    contract_key: str = "contract-1",
    *,
    entity_code: str | None = "00100",
    supplier_code: str | None = "00200",
    supplier_name: str | None = "Proveedor sintético",
    **kwargs: Any,
) -> dict[str, Any]:
    row = silver_row(version_id, contract_key, **kwargs)
    profile = CompatibilityRegistry().profiles[1]
    row.update(
        identity_rule_version=IDENTITY_RULE_VERSION,
        source_contract_sha256=profile.contract_sha256,
        source_projection_version=profile.projection_version,
        supplier_name=supplier_name,
    )
    for role, raw in (("entity", entity_code), ("supplier", supplier_code)):
        code, status, reason = code_identity(raw, role, load_identity_rules())
        row.update(
            {
                f"{role}_code_raw": raw,
                f"{role}_code": code,
                f"{role}_identity_status": status,
                f"{role}_identity_reason": reason,
            }
        )
    return row


def write_identity_bundle(
    root: Path, version_id: str, rows: list[dict[str, Any]], **kwargs: Any
) -> Path:
    manifest = write_bundle(root, version_id, rows, schema=IDENTITY_SCHEMA, **kwargs)
    registry = CompatibilityRegistry()
    rewrite_manifest(
        manifest,
        lambda value: value.update(
            schema_version=2,
            dataset_id="jbjy-vk9h",
            identity_rule_version=IDENTITY_RULE_VERSION,
            compatibility_sha256=registry.digest,
            bronze_runs=[{"run_id": "run-1", "manifest_sha256": "a" * 64}],
            input_purposes={"run-1": "incremental"},
        ),
    )
    return manifest


def rewrite_manifest(manifest_path: Path, mutate: Any) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(manifest)
    manifest_path.write_text(_json(manifest), encoding="utf-8")
    root = manifest_path.parents[2]
    current_path = root / "current.json"
    current = json.loads(current_path.read_text(encoding="utf-8"))
    current["manifest_sha256"] = _sha256(manifest_path)
    current_path.write_text(_json(current), encoding="utf-8")


def population_key(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def population_row(
    version_id: str, contract_key: str = "contract-1", **kwargs: Any
) -> dict[str, Any]:
    return identity_row(version_id, population_key(contract_key), **kwargs)


def write_population_bundle(
    root: Path,
    version_id: str,
    rows: list[dict[str, Any]],
) -> Path:
    """Real envelope-3 fixture; ordinary successors preserve their pinned parent.

    Share the independently reviewed Arrow fixture serializer rather than inventing
    a loader-only object or bypassing the production bundle verifier.
    """
    from copy import deepcopy

    from observatorio_secop.identity import key_set_digest
    from observatorio_secop.warehouse.bundle import verify_current_bundle

    from .inventory_helpers import inventory_entry
    from .test_population_bundle import fixture, persist

    parent = verify_current_bundle(root) if (root / "current.json").exists() else None
    run_id = f"population-{version_id}"
    value = fixture(root, version_id, run_id)
    for row in rows:
        row["bronze_run_id"] = run_id
    data = value["path"].parent / "data"
    shutil.rmtree(data)
    pq.write_to_dataset(
        pa.Table.from_pylist(rows, schema=IDENTITY_SCHEMA),
        root_path=data,
        partition_cols=["signing_year"],
        compression="snappy",
    )
    manifest, coverage, reconciliation = (
        value["manifest"],
        value["coverage"],
        value["reconciliation"],
    )
    manifest["files"] = [
        inventory_entry(path, value["path"].parent) for path in sorted(data.rglob("*.parquet"))
    ]
    keys = [row["contract_key"] for row in rows]
    counts = {"input_rows": len(rows), "output_rows": len(rows), "rejected_rows": 0}
    manifest["counts"] = counts
    coverage["candidate_keys_sha256"] = key_set_digest(keys)
    acquisition = coverage["acquisitions"][0]
    acquisition["limits"] = {"rows": max(len(rows) * 2, 10), "attempts": 10}
    acquisition["consumed"] = {"rows": len(rows), "attempts": 2}
    reconciliation.update(
        candidate_keys=sorted(keys),
        admitted_keys=sorted(keys),
        counts={**counts, "unattributable_rejected_rows": 0},
    )
    prototype = value["condition_summary"][0]
    summaries = []
    for row in rows:
        summaries.append(
            {
                **prototype,
                "contract_key": row["contract_key"],
                "winner_payload_sha256": row["payload_sha256"],
                "winner_run_id": row["bronze_run_id"],
                "winner_page_path": row["bronze_page_path"],
                "winner_row_ordinal": row["bronze_row_ordinal"],
                "winner_observed_at_utc": row["bronze_fetched_at_utc"].replace(tzinfo=UTC),
                "winner_profile_sha256": row["source_contract_sha256"],
                "winner_projection_version": row["source_projection_version"],
            }
        )
    value["condition_summary"] = summaries
    manifest["population"]["summary_rows"] = len(rows)
    if parent is not None:
        document = json.loads(parent.manifest_path.read_bytes())
        baseline_keys = parent.table["contract_key"].to_pylist()
        binding = {
            "silver_version_id": parent.version_id,
            "manifest_sha256": parent.manifest_sha256,
            "keys_sha256": key_set_digest(baseline_keys),
        }
        coverage.update(
            baseline=binding,
            baseline_keys_sha256=binding["keys_sha256"],
            expected_parent={key: binding[key] for key in ("silver_version_id", "manifest_sha256")},
        )
        acquisition["baseline"] = deepcopy(binding)
        manifest["bronze_runs"] = document["bronze_runs"] + manifest["bronze_runs"]
        manifest["input_purposes"].update(document["input_purposes"])
        coverage["bronze_runs"] = deepcopy(manifest["bronze_runs"])
        reconciliation.update(
            baseline_bronze_runs=document["bronze_runs"],
            baseline_keys=sorted(baseline_keys),
            admitted_keys=sorted(set(keys) - set(baseline_keys)),
        )
    persist(value)
    rewrite_manifest(value["path"], lambda _: None)
    return value["path"]


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def silver_root(tmp_path: Path) -> Path:
    root = tmp_path / "silver"
    write_bundle(root, "version-1", [silver_row("version-1")])
    return root


TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")


@pytest.fixture
def postgres_config(tmp_path: Path) -> WarehouseConfig:
    if not TEST_DSN:
        pytest.skip("WAREHOUSE_TEST_DSN must select a disposable database")
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    schema = f"silver_test_{uuid.uuid4().hex[:12]}"
    config = WarehouseConfig(
        silver_root=tmp_path / "silver",
        schema=schema,
        contract_table="contract_current",
        audit_table="load_audit",
        advisory_lock_id=7404999,
        host=connection.get("host", "127.0.0.1"),
        port=int(connection.get("port", "5432")),
        dbname=connection.get("dbname", "postgres"),
        user=connection.get("user", "postgres"),
        password=connection.get("password", ""),
    )
    yield config
    with psycopg.connect(TEST_DSN, autocommit=True) as database:
        database.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
