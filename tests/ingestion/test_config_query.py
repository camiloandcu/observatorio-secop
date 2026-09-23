from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from observatorio_secop.ingestion.config import (
    build_run_window,
    load_ingestion_config,
    validate_config,
)
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.errors import IngestionConfigurationError, SourceContractError
from observatorio_secop.ingestion.query import (
    SourceCursor,
    null_watermark_query,
    primary_query,
)

ROOT = Path(__file__).resolve().parents[2]


def test_default_configuration_and_contract_are_valid() -> None:
    config = load_ingestion_config(ROOT / "config/bronze_ingestion.yaml")
    contract = load_bronze_contract(config.contract_path, config.projection)

    assert config.page_size == 100
    assert config.max_rows == 1000
    assert contract.dataset_id == "jbjy-vk9h"
    assert contract.identity_field == "id_contrato"
    assert contract.watermark_field == "ultima_actualizacion"
    assert contract.department_value == "Antioquia"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("page_size", 0),
        ("max_page_size", 0),
        ("max_rows", 0),
        ("overlap_days", -1),
        ("timeout_seconds", 0),
        ("max_retries", -1),
        ("backoff_base_seconds", 0),
        ("jitter_seconds", -1),
        ("max_retry_after_seconds", 0),
        ("max_response_bytes", 0),
    ],
)
def test_invalid_numeric_configuration_is_rejected(tmp_path: Path, field: str, value: int) -> None:
    document = yaml.safe_load((ROOT / "config/bronze_ingestion.yaml").read_text())
    document["ingestion"][field] = value
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(document))
    with pytest.raises(IngestionConfigurationError):
        load_ingestion_config(path)


def test_page_size_cannot_exceed_bounds(config) -> None:
    with pytest.raises(IngestionConfigurationError, match="max_page_size"):
        validate_config(replace(config, page_size=config.max_page_size + 1))
    with pytest.raises(IngestionConfigurationError, match="max_rows"):
        validate_config(replace(config, page_size=3, max_rows=2))


def test_window_uses_checkpoint_overlap_without_escaping_requested_range() -> None:
    window = build_run_window(
        "2026-01-01T00:00:00Z",
        "2026-02-01T00:00:00Z",
        checkpoint_completed_through="2026-01-20T00:00:00Z",
        overlap_days=7,
    )
    assert window.effective_from_text == "2026-01-13T00:00:00.000Z"

    clamped = build_run_window(
        "2026-01-18T00:00:00Z",
        "2026-02-01T00:00:00Z",
        checkpoint_completed_through="2026-01-20T00:00:00Z",
        overlap_days=7,
    )
    assert clamped.effective_from_text == "2026-01-18T00:00:00.000Z"


def test_window_rejects_range_older_than_checkpoint_overlap() -> None:
    with pytest.raises(IngestionConfigurationError, match="checkpoint overlap"):
        build_run_window(
            "2025-01-01T00:00:00Z",
            "2025-01-02T00:00:00Z",
            checkpoint_completed_through="2026-01-20T00:00:00Z",
            overlap_days=7,
        )


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("bad", "2026-01-01T00:00:00Z"),
        ("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"),
        ("2026-01-01T00:00:00", "2026-01-02T00:00:00Z"),
    ],
)
def test_invalid_ranges_are_rejected(start: str, end: str) -> None:
    with pytest.raises(IngestionConfigurationError):
        build_run_window(start, end, checkpoint_completed_through=None, overlap_days=0)


def test_primary_queries_are_exact_and_escape_values(config, contract) -> None:
    window = build_run_window(
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
        checkpoint_completed_through=None,
        overlap_days=0,
    )
    first = primary_query(contract, config.projection, window, limit=2)
    assert first["$limit"] == 2
    assert "$offset" not in first
    assert first["$order"] == "ultima_actualizacion ASC,id_contrato ASC"
    assert "departamento='Antioquia'" in first["$where"]
    cursor = SourceCursor("2026-01-01T12:00:00.000Z", "CO'2")
    continued = primary_query(contract, config.projection, window, limit=1, cursor=cursor)
    assert "ultima_actualizacion = '2026-01-01T12:00:00.000Z'" in continued["$where"]
    assert "id_contrato > 'CO''2'" in continued["$where"]


def test_null_lane_has_explicit_event_range_and_identity_cursor(config, contract) -> None:
    window = build_run_window(
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
        checkpoint_completed_through=None,
        overlap_days=0,
    )
    query = null_watermark_query(
        contract,
        config.projection,
        window,
        fallback_event_field="fecha_de_firma",
        limit=1,
        cursor=SourceCursor(None, "CO.1"),
    )
    assert "ultima_actualizacion is null" in query["$where"]
    assert "fecha_de_firma >= '2026-01-01T00:00:00.000'" in query["$where"]
    assert "id_contrato > 'CO.1'" in query["$where"]
    assert query["$order"] == "id_contrato ASC"


def test_projection_rejects_unknown_missing_and_sensitive_fields(config) -> None:
    with pytest.raises(SourceContractError, match="unknown"):
        load_bronze_contract(config.contract_path, (*config.projection, "made_up"))
    with pytest.raises(SourceContractError, match="missing required"):
        load_bronze_contract(
            config.contract_path,
            tuple(field for field in config.projection if field != "id_contrato"),
        )
    with pytest.raises(SourceContractError, match="sensitive"):
        load_bronze_contract(config.contract_path, (*config.projection, "documento_proveedor"))
