"""Versioned acquisition-chronology evidence, independent of valid winner order."""

from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

from observatorio_secop.population.core import (
    PopulationError,
    digest,
    digest_json,
    integer,
    object_fields,
    strings,
    text,
    utc_text,
)
from observatorio_secop.population.coverage import CONDITION_SCHEMA_VERSION

_COMMON = ["schema_version", "scope_id", "silver_version_id", "contract_key"]
_WINNER = [
    "winner_payload_sha256",
    "winner_run_id",
    "winner_page_path",
    "winner_page_sha256",
    "winner_row_ordinal",
    "winner_observed_at_utc",
    "winner_profile_sha256",
    "winner_projection_version",
]
_REJECTED = [name.replace("winner_", "rejected_", 1) for name in _WINNER]
SUMMARY_FIELDS = [
    *_COMMON,
    "state",
    *_WINNER,
    "validation_version",
    "later_invalid_count",
    "equal_invalid_count",
]
LINK_FIELDS = [
    *_COMMON,
    "link_id",
    "temporal_relation",
    *_WINNER,
    *_REJECTED,
    "validation_version",
    "rejection_codes",
]
CONDITION_STATES = (
    "LATER_INVALID_OBSERVED",
    "TEMPORAL_ORDER_INDETERMINATE",
    "NO_LATER_INVALID_OBSERVED",
    "LEGACY_UNASSESSED",
)


def _schema(names: list[str]) -> Any:
    # Acquisition/core commands must not require the optional warehouse runtime.
    import pyarrow as pa

    fields = []
    for name in names:
        if name.endswith("_observed_at_utc"):
            kind = pa.timestamp("us", tz="UTC")
        elif name.endswith("_row_ordinal") or name.endswith("_invalid_count"):
            kind = pa.int64()
        elif name == "rejection_codes":
            kind = pa.list_(pa.field("element", pa.string(), nullable=False))
        else:
            kind = pa.string()
        fields.append(pa.field(name, kind, nullable=False))
    return pa.schema(fields)


def summary_arrow_schema() -> Any:
    return _schema(SUMMARY_FIELDS)


def links_arrow_schema() -> Any:
    return _schema(LINK_FIELDS)


def _path(value: object, name: str) -> str:
    result = text(value, name)
    path = PurePosixPath(result)
    if path.is_absolute() or ".." in path.parts or str(path) != result or "\\" in result:
        raise PopulationError(f"{name} must be a canonical contained relative path")
    return result


def _base(item: dict[str, Any]) -> None:
    if item["schema_version"] != CONDITION_SCHEMA_VERSION:
        raise PopulationError("Unsupported observation condition schema")
    digest(item["scope_id"], "scope_id")
    digest(item["contract_key"], "contract_key")
    text(item["silver_version_id"], "silver_version_id")
    text(item["validation_version"], "validation_version")
    _locator(item, "winner")


def _locator(item: dict[str, Any], prefix: str) -> None:
    for suffix in ("payload_sha256", "page_sha256", "profile_sha256"):
        digest(item[f"{prefix}_{suffix}"], f"{prefix}_{suffix}")
    text(item[f"{prefix}_run_id"], f"{prefix}_run_id")
    text(item[f"{prefix}_projection_version"], f"{prefix}_projection_version")
    _path(item[f"{prefix}_page_path"], f"{prefix}_page_path")
    integer(item[f"{prefix}_row_ordinal"], f"{prefix}_row_ordinal")
    field = f"{prefix}_observed_at_utc"
    item[field] = utc_text(item[field], field)


def validate_condition_summary(value: object) -> dict[str, Any]:
    item = deepcopy(object_fields(value, set(SUMMARY_FIELDS), "condition summary"))
    _base(item)
    later = integer(item["later_invalid_count"], "later_invalid_count")
    equal = integer(item["equal_invalid_count"], "equal_invalid_count")
    state = item["state"]
    if not isinstance(state, str) or state not in CONDITION_STATES:
        raise PopulationError("Unsupported condition state")
    expected = (
        "LATER_INVALID_OBSERVED"
        if later
        else "TEMPORAL_ORDER_INDETERMINATE"
        if equal
        else "NO_LATER_INVALID_OBSERVED"
    )
    if state == "LEGACY_UNASSESSED":
        if later or equal:
            raise PopulationError("Unassessed legacy summary cannot claim assessed links")
    elif state != expected:
        raise PopulationError("Condition state contradicts rejection counts")
    return item


def condition_link_id(value: dict[str, Any]) -> str:
    normalized = deepcopy(value)
    normalized.pop("link_id", None)
    for prefix in ("winner", "rejected"):
        name = f"{prefix}_observed_at_utc"
        normalized[name] = utc_text(normalized[name], name)
    return digest_json(normalized)


def validate_condition_link(value: object) -> dict[str, Any]:
    item = deepcopy(object_fields(value, set(LINK_FIELDS), "condition link"))
    _base(item)
    _locator(item, "rejected")
    codes = strings(item["rejection_codes"], "rejection_codes")
    if not codes:
        raise PopulationError("Rejected observation requires reasons")
    winner, rejected = item["winner_observed_at_utc"], item["rejected_observed_at_utc"]
    relation = "LATER" if rejected > winner else "EARLIER" if rejected < winner else "EQUAL"
    if item["temporal_relation"] != relation:
        raise PopulationError("Temporal relation contradicts acquisition instants")
    if digest(item["link_id"], "link_id") != condition_link_id(item):
        raise PopulationError("Observation link identity conflicts with content")
    return item


def validate_condition_set(
    summaries: list[dict[str, Any]],
    links: list[dict[str, Any]],
    *,
    scope_id: str,
    silver_version_id: str,
    contract_keys: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Reconcile unique summaries and all rejection links without multiplying facts."""
    checked_summaries = [validate_condition_summary(row) for row in summaries]
    checked_links = [validate_condition_link(row) for row in links]
    by_key = {row["contract_key"]: row for row in checked_summaries}
    if len(by_key) != len(checked_summaries) or set(by_key) != contract_keys:
        raise PopulationError("Condition summaries must cover contractual keys exactly once")
    link_ids: set[str] = set()
    rejected_locations: set[tuple[str, str, int]] = set()
    observed: dict[str, dict[str, int]] = {}
    for row in [*checked_summaries, *checked_links]:
        if row["scope_id"] != scope_id or row["silver_version_id"] != silver_version_id:
            raise PopulationError("Condition evidence has incompatible scope/version")
    for row in checked_links:
        if row["link_id"] in link_ids:
            raise PopulationError("Duplicate rejection link identity")
        link_ids.add(row["link_id"])
        location = (row["rejected_run_id"], row["rejected_page_path"], row["rejected_row_ordinal"])
        if location in rejected_locations:
            raise PopulationError("A rejected source observation cannot have duplicate links")
        rejected_locations.add(location)
        key = row["contract_key"]
        if key not in by_key or any(row[field] != by_key[key][field] for field in _WINNER):
            raise PopulationError("Rejection link does not reference the summarized winner")
        counts = observed.setdefault(key, {"LATER": 0, "EQUAL": 0, "EARLIER": 0})
        counts[row["temporal_relation"]] += 1
    for key, row in by_key.items():
        counts = observed.get(key, {"LATER": 0, "EQUAL": 0})
        if (row["later_invalid_count"], row["equal_invalid_count"]) != (
            counts["LATER"],
            counts["EQUAL"],
        ):
            raise PopulationError("Summary counts do not reconcile with rejection links")
        if row["state"] == "LEGACY_UNASSESSED" and key in observed:
            raise PopulationError("Legacy unassessed cannot have assessed links")
    return checked_summaries, checked_links
