"""Offline source-contract and fixture validation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.source_profile.errors import ContractValidationError
from observatorio_secop.source_profile.profiling import classify_value

SENSITIVE_FIELD_PATTERN = re.compile(
    r"(?:correo|email|tel[eé]fono|documento|identificaci[oó]n|domicilio|direcci[oó]n|cuenta|banco|representante|supervisor)",
    re.IGNORECASE,
)


def load_contract(path: Path) -> dict[str, Any]:
    try:
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ContractValidationError(f"Could not load contract {path}: {error}") from error
    if not isinstance(contract, dict):
        raise ContractValidationError("Contract must be a YAML mapping")
    return contract


def validate_contract(contract: dict[str, Any], rows: list[dict[str, Any]]) -> list[str]:
    required_sections = {
        "source",
        "observation",
        "columns",
        "territory",
        "identity",
        "incremental_cursor",
        "fixture",
        "change_policy",
    }
    missing_sections = sorted(required_sections - set(contract))
    if missing_sections:
        raise ContractValidationError(
            f"Contract is missing required sections: {', '.join(missing_sections)}"
        )
    if contract["identity"].get("status") != "resolved":
        raise ContractValidationError("Identity decision is not resolved")
    if contract["incremental_cursor"].get("status") != "resolved":
        raise ContractValidationError("Incremental cursor decision is not resolved")
    if not rows:
        raise ContractValidationError("Cannot validate an empty response")

    columns = contract["columns"]
    if not isinstance(columns, list):
        raise ContractValidationError("Contract columns must be a list")
    by_field = {column.get("field"): column for column in columns}
    if None in by_field or len(by_field) != len(columns):
        raise ContractValidationError("Contract column fields must be present and unique")
    referenced_fields = {
        *contract["identity"].get("fields", []),
        contract["incremental_cursor"].get("field"),
        *contract["incremental_cursor"].get("tie_breaker", []),
        contract["territory"].get("department_field"),
        contract["territory"].get("municipality_field"),
    }
    invalid_references = sorted(field for field in referenced_fields if field not in by_field)
    if invalid_references:
        raise ContractValidationError(
            f"Contract decisions reference unknown columns: {invalid_references}"
        )
    critical = {field: column for field, column in by_field.items() if column.get("critical")}
    known_fields = set(by_field)
    unknown_fields: set[str] = set()

    missing_from_response = sorted(
        field for field in critical if not any(field in row for row in rows)
    )
    if missing_from_response:
        capabilities = sorted(
            {
                capability
                for field in missing_from_response
                for capability in critical[field].get("required_for", [])
            }
        )
        raise ContractValidationError(
            f"Response is missing critical columns {missing_from_response}; "
            f"capabilities affected: {capabilities}"
        )

    for index, row in enumerate(rows):
        missing = sorted(
            field
            for field, column in critical.items()
            if field not in row and column.get("null_rate", 0) == 0
        )
        if missing:
            capabilities = sorted(
                {
                    capability
                    for field in missing
                    for capability in critical[field].get("required_for", [])
                }
            )
            raise ContractValidationError(
                f"Row {index} is missing critical columns {missing}; "
                f"capabilities affected: {capabilities}"
            )
        for field, value in row.items():
            if field not in known_fields:
                unknown_fields.add(field)
                continue
            declared = by_field[field].get("declared_type")
            observed_type = classify_value(value, declared)
            compatible = set(by_field[field].get("compatible_observed_types", []))
            if value is not None and compatible and observed_type not in compatible:
                raise ContractValidationError(
                    f"Column {field} expected {sorted(compatible)} but observed {observed_type}"
                )
    return sorted(unknown_fields)


def validate_fixture(contract: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    fixture_policy = contract.get("fixture", {})
    max_rows = fixture_policy.get("max_rows")
    allowed_fields = set(fixture_policy.get("allowed_fields", []))
    if not isinstance(max_rows, int) or max_rows <= 0:
        raise ContractValidationError("Fixture max_rows must be a positive integer")
    if len(rows) > max_rows:
        raise ContractValidationError(f"Fixture has {len(rows)} rows; maximum is {max_rows}")
    for row in rows:
        unexpected = sorted(set(row) - allowed_fields)
        if unexpected:
            raise ContractValidationError(
                f"Fixture contains fields outside allowlist: {unexpected}"
            )
        sensitive = sorted(field for field in row if SENSITIVE_FIELD_PATTERN.search(field))
        if sensitive:
            raise ContractValidationError(
                f"Fixture contains sensitive field categories: {sensitive}"
            )
    validate_contract(contract, rows)
