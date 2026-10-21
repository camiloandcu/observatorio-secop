"""Boundary tests for evidence semantics rather than production execution."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from observatorio_secop.population import (
    PopulationError,
    PopulationScope,
    canonical_json,
    condition_link_id,
    digest_json,
    legacy_coverage,
    links_arrow_schema,
    summary_arrow_schema,
    validate_acquisition,
    validate_condition_link,
    validate_condition_set,
    validate_condition_summary,
    validate_coverage,
    validate_reconciliation,
)

HASH = "a" * 64
KEY = "b" * 64
KEY2 = "c" * 64
SCOPE = PopulationScope("2020-01-01", "2021-01-01")
START = "2026-10-21T10:00:00.000000Z"
END = "2026-10-21T10:00:01.000000Z"


def acquisition() -> dict:
    return {
        "schema_version": "population-acquisition-v1",
        "campaign_id": "campaign",
        "plan_id": HASH,
        "scope_id": SCOPE.scope_id,
        "baseline": None,
        "state": "acquired",
        "limits": {"rows": 20, "attempts": 5},
        "consumed": {"rows": 2, "attempts": 3},
        "scope_scan_complete": True,
        "baseline_check_complete": True,
        "unconfirmed_ids": [],
        "observed_from_utc": START,
        "observed_to_utc": END,
        "truncated": False,
        "limitations": ["Mutable source; bounded observation, not a frozen snapshot."],
    }


def coverage() -> dict:
    return {
        "schema_version": "population-coverage-v1",
        "silver_version_id": "silver-A",
        "scope": SCOPE.as_dict(),
        "scope_id": SCOPE.scope_id,
        "mode": "authoritative_candidate",
        "state": "complete",
        "basis": "bounded_scan_observed",
        "baseline": None,
        "expected_parent": None,
        "bronze_runs": [{"run_id": "campaign", "manifest_sha256": HASH}],
        "baseline_keys_sha256": None,
        "candidate_keys_sha256": HASH,
        "acquisitions": [acquisition()],
        "limitations": ["Missing signing dates excluded."],
        "condition_schema_version": "observation-condition-v1",
    }


def summary() -> dict:
    return {
        "schema_version": "observation-condition-v1",
        "scope_id": SCOPE.scope_id,
        "silver_version_id": "silver-A",
        "contract_key": KEY,
        "state": "LATER_INVALID_OBSERVED",
        "winner_payload_sha256": HASH,
        "winner_run_id": "run-A",
        "winner_page_path": "pages/primary-00001.json",
        "winner_page_sha256": HASH,
        "winner_row_ordinal": 0,
        "winner_observed_at_utc": START,
        "winner_profile_sha256": HASH,
        "winner_projection_version": "secop-contracts-v2",
        "validation_version": "silver-v3",
        "later_invalid_count": 1,
        "equal_invalid_count": 0,
    }


def link() -> dict:
    row = summary()
    for name in ("state", "later_invalid_count", "equal_invalid_count"):
        del row[name]
    for name, value in list(row.items()):
        if name.startswith("winner_"):
            row[name.replace("winner_", "rejected_", 1)] = value
    row["rejected_observed_at_utc"] = END
    row["rejected_payload_sha256"] = KEY2
    row["temporal_relation"] = "LATER"
    row["rejection_codes"] = ["AMOUNT_NULL"]
    row["link_id"] = condition_link_id(row)
    return row


def test_scope_has_governed_semantics_and_half_open_floating_dates() -> None:
    assert PopulationScope.from_dict(SCOPE.as_dict()) == SCOPE
    assert SCOPE.scope_id == digest_json(SCOPE.as_dict())
    assert SCOPE.contains("2020-01-01T00:00:00.000", "Antioquia")
    assert SCOPE.contains("2020-12-31T23:59:59.999", "Antioquia")
    assert not SCOPE.contains("2021-01-01T00:00:00.000", "Antioquia")
    assert not SCOPE.contains("2020-12-31T23:59:59Z", "Antioquia")
    assert not SCOPE.contains(None)
    assert not SCOPE.contains("2020-01-01", "Bogotá")
    for lower, upper in [
        ("2020-01-01Z", "2021-01-01"),
        ("2020-02-30", "2021-01-01"),
        ("2021-01-01", "2021-01-01"),
        (True, "2021-01-01"),
    ]:
        with pytest.raises(PopulationError):
            PopulationScope(lower, upper)
    descriptor = SCOPE.as_dict()
    descriptor["where"] = "1=1"
    with pytest.raises(PopulationError):
        PopulationScope.from_dict(descriptor)


def test_coverage_is_acyclic_and_independent_of_quality_success() -> None:
    receipt = coverage()
    assert validate_coverage(receipt, silver_version_id="silver-A") == receipt
    # No quality-success field is fabricated: this validator establishes acquisition meaning.
    assert "quality_passed" not in receipt
    assert digest_json(receipt) == digest_json(deepcopy(receipt))
    assert b"silver-A" in canonical_json(receipt)
    assert legacy_coverage() == {
        "coverage_known": False,
        "state": None,
        "reason": "LEGACY_UNASSESSED",
    }


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.update(scope_id=HASH),
        lambda row: row.update(state="complete", basis="observed_inputs"),
        lambda row: row.update(acquisitions=[]),
        lambda row: row["acquisitions"][0].update(truncated=True),
        lambda row: row["acquisitions"][0].update(scope_scan_complete=False),
        lambda row: row["acquisitions"][0]["limits"].update(rows=True),
        lambda row: row["acquisitions"][0]["consumed"].update(attempts=6),
        lambda row: row["acquisitions"][0].update(state=[]),
        lambda row: row["acquisitions"][0].update(observed_from_utc="2026-10-21T10:00:00"),
        lambda row: row["acquisitions"][0].update(observed_to_utc="2026-10-20T10:00:00Z"),
        lambda row: row["acquisitions"][0].update(unconfirmed_ids=["unknown"]),
        lambda row: row.update(extra="unsupported"),
        lambda row: row.update(mode="authoritative_candidate", state="partial"),
        lambda row: row["bronze_runs"].append(deepcopy(row["bronze_runs"][0])),
    ],
)
def test_malformed_or_unproved_coverage_is_rejected(mutate) -> None:
    receipt = coverage()
    mutate(receipt)
    with pytest.raises(PopulationError):
        validate_coverage(receipt)


def test_missing_baseline_response_is_not_a_deletion_or_incomplete_scan() -> None:
    receipt = coverage()
    binding = {"silver_version_id": "old", "manifest_sha256": HASH, "keys_sha256": KEY}
    receipt["baseline"] = binding
    receipt["baseline_keys_sha256"] = KEY
    receipt["expected_parent"] = {
        key: binding[key] for key in ("silver_version_id", "manifest_sha256")
    }
    receipt["acquisitions"][0]["baseline"] = binding
    receipt["acquisitions"][0]["unconfirmed_ids"] = ["contract-1"]
    assert validate_coverage(receipt)["state"] == "complete"
    receipt["expected_parent"]["manifest_sha256"] = KEY2
    with pytest.raises(PopulationError):
        validate_coverage(receipt)


def test_budget_pause_has_no_fabricated_completion() -> None:
    snapshot = acquisition()
    snapshot.update(state="paused_budget", scope_scan_complete=False)
    assert validate_acquisition(snapshot)["state"] == "paused_budget"
    receipt = coverage()
    receipt.update(
        mode="preview", state="partial", basis="observed_inputs", acquisitions=[snapshot]
    )
    assert validate_coverage(receipt)["state"] == "partial"


def test_conditions_use_acquisition_chronology_and_preserve_cardinality() -> None:
    rows, links = validate_condition_set(
        [summary()],
        [link()],
        scope_id=SCOPE.scope_id,
        silver_version_id="silver-A",
        contract_keys={KEY},
    )
    assert rows[0]["later_invalid_count"] == 1
    assert links[0]["temporal_relation"] == "LATER"
    equal = link()
    equal["rejected_observed_at_utc"] = START
    equal["temporal_relation"] = "EQUAL"
    equal["link_id"] = condition_link_id(equal)
    row = summary()
    row.update(state="TEMPORAL_ORDER_INDETERMINATE", later_invalid_count=0, equal_invalid_count=1)
    assert validate_condition_set(
        [row],
        [equal],
        scope_id=SCOPE.scope_id,
        silver_version_id="silver-A",
        contract_keys={KEY},
    )
    equal["temporal_relation"] = "LATER"
    equal["link_id"] = condition_link_id(equal)
    with pytest.raises(PopulationError):
        validate_condition_link(equal)


def test_condition_time_hash_is_stable_for_arrow_datetime_and_serialized_text() -> None:
    row = link()
    row["winner_observed_at_utc"] = datetime(2026, 10, 21, 10, tzinfo=UTC)
    row["rejected_observed_at_utc"] = datetime(2026, 10, 21, 10, 0, 1, tzinfo=UTC)
    assert condition_link_id(row) == link()["link_id"]
    assert validate_condition_link(row) == link()


@pytest.mark.parametrize(
    "case", ["duplicate_summary", "duplicate_link", "unbound", "counts", "winner"]
)
def test_condition_set_rejects_incoherent_joins(case: str) -> None:
    summaries, links = [summary()], [link()]
    if case == "duplicate_summary":
        summaries *= 2
    elif case == "duplicate_link":
        links *= 2
    elif case == "unbound":
        links[0]["scope_id"] = KEY2
        links[0]["link_id"] = condition_link_id(links[0])
    elif case == "counts":
        summaries[0]["later_invalid_count"] = 2
    else:
        links[0]["winner_run_id"] = "another"
        links[0]["link_id"] = condition_link_id(links[0])
    with pytest.raises(PopulationError):
        validate_condition_set(
            summaries,
            links,
            scope_id=SCOPE.scope_id,
            silver_version_id="silver-A",
            contract_keys={KEY},
        )


def test_legacy_condition_is_unknown_and_unsupported_payload_fields_fail() -> None:
    row = summary()
    row.update(state="LEGACY_UNASSESSED", later_invalid_count=0)
    assert validate_condition_summary(row)["state"] == "LEGACY_UNASSESSED"
    row["raw_payload"] = "private"
    with pytest.raises(PopulationError):
        validate_condition_summary(row)


def test_arrow_condition_schemas_have_explicit_utc_and_no_nullable_identity() -> None:
    pa = pytest.importorskip("pyarrow")
    for schema in (summary_arrow_schema(), links_arrow_schema()):
        assert schema.field("winner_observed_at_utc").type == pa.timestamp("us", tz="UTC")
        assert all(not field.nullable for field in schema)


def test_changed_link_hash_does_not_duplicate_one_source_observation() -> None:
    first = link()
    second = deepcopy(first)
    second["rejection_codes"] = ["DATE_INVALID"]
    second["link_id"] = condition_link_id(second)
    row = summary()
    row["later_invalid_count"] = 2
    with pytest.raises(PopulationError, match="duplicate links"):
        validate_condition_set(
            [row],
            [first, second],
            scope_id=SCOPE.scope_id,
            silver_version_id="silver-A",
            contract_keys={KEY},
        )


def test_reconciliation_admissions_and_diagnostic_drift() -> None:
    receipt = {
        "schema_version": "population-reconciliation-v1",
        "scope_id": SCOPE.scope_id,
        "silver_version_id": "silver-A",
        "baseline_bronze_runs": [],
        "baseline_keys": [KEY],
        "candidate_keys": [KEY, KEY2],
        "admitted_keys": [KEY2],
        "scope_drift": [],
        "unconfirmed_ids": [],
        "historical_refresh_nodes": [],
        "counts": {
            "input_rows": 3,
            "output_rows": 2,
            "rejected_rows": 1,
            "unattributable_rejected_rows": 0,
        },
    }
    assert validate_reconciliation(receipt) == receipt
    receipt["admitted_keys"] = []
    with pytest.raises(PopulationError):
        validate_reconciliation(receipt)
