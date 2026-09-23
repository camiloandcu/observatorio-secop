from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any

import pytest

from observatorio_secop.ingestion.config import IngestionConfig, load_ingestion_config
from observatorio_secop.ingestion.contract import BronzeSourceContract, load_bronze_contract
from observatorio_secop.ingestion.transport import TransportResponse

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def config(tmp_path: Path) -> IngestionConfig:
    loaded = load_ingestion_config(ROOT / "config/bronze_ingestion.yaml")
    return IngestionConfig(
        **{
            **loaded.__dict__,
            "bronze_root": tmp_path / "bronze",
            "contract_path": ROOT / "contracts/secop_source.yaml",
            "page_size": 2,
            "max_rows": 20,
        }
    )


@pytest.fixture
def contract(config: IngestionConfig) -> BronzeSourceContract:
    return load_bronze_contract(config.contract_path, config.projection)


def row(identity: str, timestamp: str | None = "2026-01-01T00:00:00.000") -> dict[str, Any]:
    result = {
        "id_contrato": identity,
        "departamento": "Antioquia",
        "ciudad": "Medellín",
        "ultima_actualizacion": timestamp,
    }
    if timestamp is None:
        result["fecha_de_firma"] = "2026-01-01T00:00:00.000"
    return result


def metadata(contract: BronzeSourceContract, *, extra: str | None = None) -> dict[str, Any]:
    fields = [
        {"fieldName": field, "dataTypeName": column["declared_type"]}
        for field, column in contract.columns.items()
    ]
    if extra:
        fields.append({"fieldName": extra, "dataTypeName": "text"})
    return {"id": contract.dataset_id, "columns": fields}


class FakeTransport:
    def __init__(self, metadata_payload: dict[str, Any], pages: list[list[dict[str, Any]]]):
        self.metadata_payload = metadata_payload
        self.pages = deque(pages)
        self.queries: list[dict[str, str | int]] = []
        self.token = None

    def fetch_metadata(self) -> TransportResponse:
        raw = json.dumps(self.metadata_payload, separators=(",", ":")).encode()
        return TransportResponse(raw, self.metadata_payload, 200, 1, 0, {})

    def query(self, params: dict[str, str | int], **_kwargs: Any) -> TransportResponse:
        self.queries.append(params)
        payload = self.pages.popleft()
        raw = json.dumps(payload, separators=(",", ":")).encode()
        return TransportResponse(raw, payload, 200, 1, 0, {})
