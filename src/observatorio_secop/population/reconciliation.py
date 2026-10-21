"""Key-set admission and drift evidence with explicit auxiliary boundaries."""

from __future__ import annotations

from copy import deepcopy
from datetime import date
from typing import Any

from observatorio_secop.population.core import (
    PopulationError,
    digest,
    integer,
    object_fields,
    strings,
    text,
)

RECONCILIATION_SCHEMA_VERSION = "population-reconciliation-v1"
_FIELDS = {
    "schema_version",
    "scope_id",
    "silver_version_id",
    "baseline_bronze_runs",
    "baseline_keys",
    "candidate_keys",
    "admitted_keys",
    "scope_drift",
    "unconfirmed_ids",
    "historical_refresh_nodes",
    "counts",
}


def validate_reconciliation(
    value: object,
    *,
    scope_id: str | None = None,
    silver_version_id: str | None = None,
) -> dict[str, Any]:
    item = deepcopy(object_fields(value, _FIELDS, "population reconciliation"))
    if item["schema_version"] != RECONCILIATION_SCHEMA_VERSION:
        raise PopulationError("Unsupported reconciliation schema_version")
    digest(item["scope_id"], "scope_id")
    text(item["silver_version_id"], "silver_version_id")
    if scope_id is not None and scope_id != item["scope_id"]:
        raise PopulationError("Reconciliation belongs to another scope")
    if silver_version_id is not None and silver_version_id != item["silver_version_id"]:
        raise PopulationError("Reconciliation belongs to another Silver version")
    for field in ("baseline_keys", "candidate_keys", "admitted_keys"):
        for key in strings(item[field], field):
            digest(key, field)
    baseline, candidate, admitted = (
        set(item[field]) for field in ("baseline_keys", "candidate_keys", "admitted_keys")
    )
    if admitted != candidate - baseline:
        raise PopulationError("Admitted keys do not reconcile with baseline/candidate")
    strings(item["unconfirmed_ids"], "unconfirmed_ids")
    _runs(item["baseline_bronze_runs"])
    if not isinstance(item["scope_drift"], list):
        raise PopulationError("scope_drift must be a list")
    for drift in item["scope_drift"]:
        record = object_fields(
            drift,
            {
                "source_contract_id",
                "contract_key",
                "bronze_run_id",
                "bronze_page_path",
                "bronze_row_ordinal",
                "signing_date",
                "department",
            },
            "scope drift",
        )
        for field in ("source_contract_id", "bronze_run_id", "bronze_page_path"):
            text(record[field], field)
        digest(record["contract_key"], "contract_key")
        integer(record["bronze_row_ordinal"], "bronze_row_ordinal")
        if record["signing_date"] is not None:
            value = record["signing_date"]
            try:
                if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                    raise ValueError("not an ISO date")
            except ValueError as error:
                raise PopulationError(
                    "Drift signing_date must be a calendar date or null"
                ) from error
        if record["department"] is not None:
            text(record["department"], "department")
    if not isinstance(item["historical_refresh_nodes"], list):
        raise PopulationError("historical_refresh_nodes must be a list")
    nodes = set()
    for node in item["historical_refresh_nodes"]:
        record = object_fields(
            node,
            {
                "silver_version_id",
                "manifest_sha256",
                "refresh_run_ids",
            },
            "historical refresh node",
        )
        identifier = text(record["silver_version_id"], "silver_version_id")
        digest(record["manifest_sha256"], "manifest_sha256")
        strings(record["refresh_run_ids"], "refresh_run_ids")
        if identifier in nodes:
            raise PopulationError("Duplicate historical refresh node")
        nodes.add(identifier)
    counts = object_fields(
        item["counts"],
        {
            "input_rows",
            "output_rows",
            "rejected_rows",
            "unattributable_rejected_rows",
        },
        "reconciliation counts",
    )
    for field, number in counts.items():
        integer(number, field)
    if (
        counts["output_rows"] != len(candidate)
        or counts["input_rows"] < counts["output_rows"] + counts["rejected_rows"]
        or counts["unattributable_rejected_rows"] > counts["rejected_rows"]
    ):
        raise PopulationError("Reconciliation counts contradict contractual/rejection counts")
    return item


def _runs(value: object) -> None:
    if not isinstance(value, list):
        raise PopulationError("baseline_bronze_runs must be a list")
    seen = set()
    for row in value:
        record = object_fields(row, {"run_id", "manifest_sha256"}, "baseline Bronze binding")
        identifier = text(record["run_id"], "run_id")
        digest(record["manifest_sha256"], "manifest_sha256")
        if identifier in seen:
            raise PopulationError("Duplicate baseline Bronze binding")
        seen.add(identifier)
