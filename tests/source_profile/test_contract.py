from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from observatorio_secop.source_profile.contract import (
    load_contract,
    validate_contract,
    validate_fixture,
)
from observatorio_secop.source_profile.errors import ContractValidationError
from observatorio_secop.source_profile.workflow import render_report

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "contracts" / "secop_source.yaml"
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "secop_contracts.json"


@pytest.fixture
def contract() -> dict[str, object]:
    return load_contract(CONTRACT_PATH)


@pytest.fixture
def rows() -> list[dict[str, object]]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_frozen_contract_and_fixture_are_valid(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    validate_fixture(contract, rows)


def test_missing_critical_column_names_capability(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    changed = [{key: value for key, value in row.items() if key != "departamento"} for row in rows]
    with pytest.raises(
        ContractValidationError, match="department filter.*departamento|departamento"
    ):
        validate_contract(contract, changed)


def test_nullable_critical_value_may_be_omitted_from_one_row(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    changed = copy.deepcopy(rows)
    changed[0].pop("ultima_actualizacion")
    validate_contract(contract, changed)


def test_incompatible_type_reports_expected_and_observed(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    changed = copy.deepcopy(rows)
    changed[0]["id_contrato"] = 123
    with pytest.raises(ContractValidationError, match="id_contrato expected.*text.*number"):
        validate_contract(contract, changed)


def test_additive_optional_column_is_reported_without_failure(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    changed = copy.deepcopy(rows)
    changed[0]["new_optional_column"] = "visible"
    assert validate_contract(contract, changed) == ["new_optional_column"]


def test_empty_response_cannot_validate_or_replace_contract(
    contract: dict[str, object],
) -> None:
    with pytest.raises(ContractValidationError, match="empty"):
        validate_contract(contract, [])


def test_oversized_fixture_is_rejected(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    with pytest.raises(ContractValidationError, match="maximum"):
        validate_fixture(contract, rows * 3)


def test_fixture_field_outside_allowlist_is_rejected(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    changed = copy.deepcopy(rows)
    changed[0]["correo"] = "not-versioned@example.invalid"
    with pytest.raises(ContractValidationError, match="outside allowlist"):
        validate_fixture(contract, changed)


def test_decision_referencing_unknown_column_is_rejected(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    changed = copy.deepcopy(contract)
    changed["identity"]["fields"] = ["missing_identity"]  # type: ignore[index]
    with pytest.raises(ContractValidationError, match="unknown columns.*missing_identity"):
        validate_contract(changed, rows)


def test_report_is_generated_from_the_frozen_contract(contract: dict[str, object]) -> None:
    report = (ROOT / "docs" / "data-source-profile.md").read_text(encoding="utf-8")
    assert render_report(contract) == report


def test_fixture_covers_each_mapped_municipality_once(
    contract: dict[str, object], rows: list[dict[str, object]]
) -> None:
    territory = contract["territory"]  # type: ignore[index]
    expected = [entry["observed"] for entry in territory["valle_de_aburra"]]
    assert [row["ciudad"] for row in rows] == expected
    assert len({row["id_contrato"] for row in rows}) == len(rows)
