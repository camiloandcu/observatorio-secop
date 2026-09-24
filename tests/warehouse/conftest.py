from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA


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
) -> Path:
    version = root / "versions" / version_id
    data = version / "data"
    data.mkdir(parents=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_to_dataset(
        table, root_path=data, partition_cols=["signing_year"], compression="snappy"
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


def rewrite_manifest(manifest_path: Path, mutate: Any) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutate(manifest)
    manifest_path.write_text(_json(manifest), encoding="utf-8")
    root = manifest_path.parents[2]
    current_path = root / "current.json"
    current = json.loads(current_path.read_text(encoding="utf-8"))
    current["manifest_sha256"] = _sha256(manifest_path)
    current_path.write_text(_json(current), encoding="utf-8")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def silver_root(tmp_path: Path) -> Path:
    root = tmp_path / "silver"
    write_bundle(root, "version-1", [silver_row("version-1")])
    return root
