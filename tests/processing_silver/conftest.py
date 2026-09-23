from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from observatorio_secop.processing.silver.config import SilverConfig, load_silver_config

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("observatorio-secop-silver-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def silver_config(tmp_path: Path) -> SilverConfig:
    loaded = load_silver_config(ROOT / "config/silver_quality.yaml")
    return replace(
        loaded,
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        contract_path=ROOT / "contracts/secop_source.yaml",
        territory_catalog_path=ROOT / "config/antioquia_municipalities.yaml",
        local_threads=1,
        output_partitions=1,
    )


def source_contract_hash() -> str:
    return hashlib.sha256((ROOT / "contracts/secop_source.yaml").read_bytes()).hexdigest()


def row(
    identity: str | None,
    *,
    updated: str | None = "2026-01-02T00:00:00.000",
    city: str = "Medellín",
    amount: str | None = "100.000000",
    signing: str | None = "2026-01-01T00:00:00.000",
    start: str | None = "2026-01-01T00:00:00.000",
    end: str | None = "2026-12-31T00:00:00.000",
    status: str = "Activo",
    department: str = "Antioquia",
) -> dict[str, Any]:
    return {
        "id_contrato": identity,
        "referencia_del_contrato": f"REF-{identity}",
        "proceso_de_compra": f"PROC-{identity}",
        "nombre_entidad": "Entidad de prueba",
        "departamento": department,
        "ciudad": city,
        "estado_contrato": status,
        "codigo_de_categoria_principal": "V1.1",
        "descripcion_del_proceso": "Proceso de prueba",
        "tipo_de_contrato": "Servicios",
        "modalidad_de_contratacion": "Directa",
        "fecha_de_firma": signing,
        "fecha_de_inicio_del_contrato": start,
        "fecha_de_fin_del_contrato": end,
        "valor_del_contrato": amount,
        "ultima_actualizacion": updated,
        "urlproceso": {"url": "https://example.test/contract"},
    }


def write_bronze_run(
    config: SilverConfig,
    run_id: str,
    rows: list[dict[str, Any]],
    *,
    fetched_at: str = "2026-01-03T00:00:00.000Z",
    status: str = "committed",
) -> Path:
    run = config.bronze_root / "runs" / run_id
    pages = run / "pages"
    pages.mkdir(parents=True)
    raw = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    page = pages / "primary-00001.json"
    page.write_bytes(raw)
    manifest = {
        "run_id": run_id,
        "dataset_id": config.dataset_id,
        "contract_sha256": source_contract_hash(),
        "status": status,
        "counts": {"written": len(rows)},
        "pages": [
            {
                "path": "pages/primary-00001.json",
                "lane": "primary",
                "sequence": 1,
                "fetched_at_utc": fetched_at,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "row_count": len(rows),
            }
        ],
    }
    (run / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    return run
