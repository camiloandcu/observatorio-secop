"""Metadata and page compatibility checks against the frozen source contract."""

from __future__ import annotations

from typing import Any

from observatorio_secop.ingestion.contract import BronzeSourceContract
from observatorio_secop.ingestion.errors import PageValidationError, SourceContractError
from observatorio_secop.source_profile.profiling import classify_value


def validate_metadata(metadata: dict[str, Any], contract: BronzeSourceContract) -> list[str]:
    if metadata.get("id") != contract.dataset_id:
        raise SourceContractError("Socrata metadata identifies a different dataset")
    raw_columns = metadata.get("columns")
    if not isinstance(raw_columns, list):
        raise SourceContractError("Socrata metadata is missing its columns list")
    observed = {
        item.get("fieldName"): item.get("dataTypeName")
        for item in raw_columns
        if isinstance(item, dict) and isinstance(item.get("fieldName"), str)
    }
    missing = sorted(set(contract.critical_fields) - set(observed))
    if missing:
        raise SourceContractError(f"Metadata is missing critical columns: {missing}")
    for field in contract.critical_fields:
        expected = contract.columns[field].get("declared_type")
        if observed[field] != expected:
            raise SourceContractError(
                f"Metadata column {field} expected {expected} but observed {observed[field]}"
            )
    return sorted(set(observed) - set(contract.columns))


def validate_page(
    rows: list[dict[str, Any]],
    contract: BronzeSourceContract,
    *,
    lane: str,
    fallback_event_field: str = "fecha_de_firma",
) -> list[str]:
    required = {
        contract.department_field,
        contract.identity_field,
        *(
            field
            for field in contract.critical_fields
            if contract.columns[field].get("null_rate") == 0
        ),
    }
    if lane == "primary":
        required.add(contract.watermark_field)
    else:
        required.add(fallback_event_field)
    for index, row in enumerate(rows):
        missing = sorted(field for field in required if field not in row or row[field] is None)
        if missing:
            raise PageValidationError(f"Row {index} is missing critical columns: {missing}")
        for field, value in row.items():
            if field not in contract.columns or value is None:
                continue
            expected = set(contract.columns[field].get("compatible_observed_types", []))
            observed = classify_value(value, contract.columns[field].get("declared_type"))
            if expected and observed not in expected:
                raise PageValidationError(
                    f"Column {field} expected {sorted(expected)} but observed {observed}"
                )
    return sorted({field for row in rows for field in row if field not in contract.columns})
