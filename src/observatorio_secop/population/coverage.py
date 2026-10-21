"""Strict receipt adapters; extraction completion does not imply data currency."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from observatorio_secop.population.core import (
    PopulationError,
    boolean,
    digest,
    integer,
    object_fields,
    strings,
    text,
    utc_text,
    version_binding,
)
from observatorio_secop.population.scope import PopulationScope

COVERAGE_SCHEMA_VERSION = "population-coverage-v1"
ACQUISITION_SCHEMA_VERSION = "population-acquisition-v1"
CONDITION_SCHEMA_VERSION = "observation-condition-v1"
MANIFEST_SCHEMA_VERSION = 3
ROW_SCHEMA_VERSION = 2
_ACQUISITION_FIELDS = {
    "schema_version",
    "campaign_id",
    "plan_id",
    "scope_id",
    "baseline",
    "state",
    "limits",
    "consumed",
    "scope_scan_complete",
    "baseline_check_complete",
    "unconfirmed_ids",
    "observed_from_utc",
    "observed_to_utc",
    "truncated",
    "limitations",
}
_COVERAGE_FIELDS = {
    "schema_version",
    "silver_version_id",
    "scope",
    "scope_id",
    "mode",
    "state",
    "basis",
    "baseline",
    "expected_parent",
    "bronze_runs",
    "baseline_keys_sha256",
    "candidate_keys_sha256",
    "acquisitions",
    "limitations",
    "condition_schema_version",
}


def _quantities(value: object, name: str, minimum: int) -> dict[str, int]:
    item = object_fields(value, {"rows", "attempts"}, name)
    return {key: integer(item[key], f"{name}.{key}", minimum=minimum) for key in item}


def validate_acquisition(value: object, *, scope_id: str | None = None) -> dict[str, Any]:
    item = deepcopy(object_fields(value, _ACQUISITION_FIELDS, "acquisition"))
    if item["schema_version"] != ACQUISITION_SCHEMA_VERSION:
        raise PopulationError("Unsupported acquisition schema_version")
    text(item["campaign_id"], "campaign_id")
    digest(item["plan_id"], "plan_id")
    digest(item["scope_id"], "scope_id")
    if scope_id is not None and item["scope_id"] != scope_id:
        raise PopulationError("Acquisition belongs to another scope")
    item["baseline"] = version_binding(item["baseline"], keys=True)
    states = {
        "planned",
        "extracting",
        "paused_budget",
        "failed_recoverable",
        "acquired",
        "truncated",
    }
    if not isinstance(item["state"], str) or item["state"] not in states:
        raise PopulationError("Unsupported acquisition state")
    limits = _quantities(item["limits"], "limits", 1)
    consumed = _quantities(item["consumed"], "consumed", 0)
    if any(consumed[key] > limits[key] for key in limits):
        raise PopulationError("Acquisition exceeds campaign ceiling")
    scan = boolean(item["scope_scan_complete"], "scope_scan_complete")
    check = boolean(item["baseline_check_complete"], "baseline_check_complete")
    truncated = boolean(item["truncated"], "truncated")
    strings(item["unconfirmed_ids"], "unconfirmed_ids")
    strings(item["limitations"], "limitations")
    if item["baseline"] is None and item["unconfirmed_ids"]:
        raise PopulationError("Unconfirmed baseline IDs require a baseline")
    start, end = item["observed_from_utc"], item["observed_to_utc"]
    if (start is None) != (end is None):
        raise PopulationError("Observation interval requires both endpoints")
    if start is not None:
        item["observed_from_utc"] = utc_text(start)
        item["observed_to_utc"] = utc_text(end)
        if item["observed_from_utc"] > item["observed_to_utc"]:
            raise PopulationError("Observation interval is reversed")
    if item["state"] == "acquired" and (not scan or not check or truncated or start is None):
        raise PopulationError(
            "Acquired campaign needs observed terminal outcomes without truncation"
        )
    if item["state"] == "acquired" and consumed["attempts"] == 0:
        raise PopulationError("Acquired campaign requires a source request outcome")
    if item["state"] == "truncated" and not truncated:
        raise PopulationError("Terminal truncated state requires truncation evidence")
    if truncated and item["state"] == "acquired":
        raise PopulationError("A truncated acquisition cannot be acquired")
    return item


def validate_coverage(
    value: object,
    *,
    silver_version_id: str | None = None,
) -> dict[str, Any]:
    item = deepcopy(object_fields(value, _COVERAGE_FIELDS, "coverage"))
    if item["schema_version"] != COVERAGE_SCHEMA_VERSION:
        raise PopulationError("Unsupported coverage schema_version")
    version = text(item["silver_version_id"], "silver_version_id")
    if silver_version_id is not None and version != silver_version_id:
        raise PopulationError("Coverage belongs to another Silver version")
    scope = PopulationScope.from_dict(item["scope"])
    if digest(item["scope_id"], "scope_id") != scope.scope_id:
        raise PopulationError("Scope hash differs from canonical descriptor")
    if item["mode"] not in ("preview", "authoritative_candidate"):
        raise PopulationError("Unsupported selection mode")
    if item["state"] not in ("sample", "partial", "complete"):
        raise PopulationError("Unsupported coverage state")
    if item["basis"] not in ("sample_selection", "observed_inputs", "bounded_scan_observed"):
        raise PopulationError("Unsupported coverage basis")
    item["baseline"] = version_binding(item["baseline"], keys=True)
    item["expected_parent"] = version_binding(item["expected_parent"])
    baseline = item["baseline"]
    if baseline is None:
        if item["baseline_keys_sha256"] is not None or item["expected_parent"] is not None:
            raise PopulationError("Bootstrap cannot claim a baseline or parent")
    elif item["baseline_keys_sha256"] != baseline["keys_sha256"] or item["expected_parent"] != {
        key: baseline[key] for key in ("silver_version_id", "manifest_sha256")
    }:
        raise PopulationError("Baseline, key digest and expected parent must agree")
    digest(item["candidate_keys_sha256"], "candidate_keys_sha256")
    if not isinstance(item["bronze_runs"], list):
        raise PopulationError("bronze_runs must be a list")
    seen = set()
    for run in item["bronze_runs"]:
        record = object_fields(run, {"run_id", "manifest_sha256"}, "Bronze binding")
        identifier = text(record["run_id"], "run_id")
        digest(record["manifest_sha256"], "manifest_sha256")
        if identifier in seen:
            raise PopulationError("Duplicate Bronze run binding")
        seen.add(identifier)
    if not isinstance(item["acquisitions"], list):
        raise PopulationError("acquisitions must be a list")
    item["acquisitions"] = [
        validate_acquisition(acquisition, scope_id=scope.scope_id)
        for acquisition in item["acquisitions"]
    ]
    campaign_ids = [entry["campaign_id"] for entry in item["acquisitions"]]
    if len(campaign_ids) != len(set(campaign_ids)):
        raise PopulationError("Duplicate acquisition campaign")
    if not set(campaign_ids) <= seen:
        raise PopulationError("Acquisition lacks an exact Bronze input binding")
    strings(item["limitations"], "limitations")
    if item["condition_schema_version"] != CONDITION_SCHEMA_VERSION:
        raise PopulationError("Unsupported observation-condition schema")
    if item["state"] == "complete":
        if item["basis"] != "bounded_scan_observed" or not item["acquisitions"]:
            raise PopulationError("Complete coverage requires bounded-plan evidence")
        if any(
            entry["state"] != "acquired" or entry["baseline"] != baseline
            for entry in item["acquisitions"]
        ):
            raise PopulationError("Complete coverage requires completed compatible plans")
    if item["state"] == "sample" and item["basis"] != "sample_selection":
        raise PopulationError("Sample coverage requires sample_selection basis")
    if item["mode"] == "authoritative_candidate" and item["state"] != "complete":
        raise PopulationError("Authoritative selection requires complete coverage")
    return item


def legacy_coverage() -> dict[str, Any]:
    return {"coverage_known": False, "state": None, "reason": "LEGACY_UNASSESSED"}
