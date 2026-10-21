"""Attributable rejection evidence, independent of valid-observation precedence."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from observatorio_secop.population import condition_link_id
from observatorio_secop.processing.silver.config import normalize_text
from observatorio_secop.processing.silver.models import VerifiedBronzeRun


def observation_conditions(
    observations: list[dict[str, Any]],
    winners: list[dict[str, Any]],
    runs: tuple[VerifiedBronzeRun, ...],
    *,
    scope_id: str,
    version_id: str,
    validation_version: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Link all reliable rejects; only strict acquisition-time order flags later."""
    pages = {(run.run_id, page.relative_path): page.sha256 for run in runs for page in run.pages}
    identities: dict[str, set[str]] = defaultdict(set)
    for row in observations:
        raw = row.get("source_contract_id")
        normalized = normalize_text(raw)
        if normalized and isinstance(raw, str):
            identities[normalized].add(raw)
    rejected: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        raw = row.get("source_contract_id")
        normalized = normalize_text(raw)
        if (
            isinstance(raw, str)
            and normalized
            and len(identities[normalized]) == 1
            and row.get("_rejection_details")
        ):
            rejected[raw].append(row)
    summaries, links = [], []
    for winner in sorted(winners, key=lambda item: item["contract_key"]):
        binding = {
            "schema_version": "observation-condition-v1",
            "scope_id": scope_id,
            "silver_version_id": version_id,
            "contract_key": winner["contract_key"],
        }
        winner_locator = _locator(winner, "winner", pages)
        own_links = []
        for reject in rejected.get(winner["source_contract_id"], []):
            observed = _utc(reject["bronze_fetched_at_utc"])
            retained = _utc(winner["bronze_fetched_at_utc"])
            relation = (
                "LATER" if observed > retained else "EQUAL" if observed == retained else "EARLIER"
            )
            item = {
                **binding,
                **winner_locator,
                **_locator(reject, "rejected", pages),
                "temporal_relation": relation,
                "validation_version": validation_version,
                "rejection_codes": sorted(
                    {detail["code"] for detail in reject["_rejection_details"]}
                ),
            }
            item["link_id"] = condition_link_id(item)
            own_links.append(item)
        # A logical observation may occur only once per run/page/ordinal. Do not
        # coalesce different observations that happen to share a payload hash.
        unique = {item["link_id"]: item for item in own_links}
        own_links = [unique[key] for key in sorted(unique)]
        later = sum(item["temporal_relation"] == "LATER" for item in own_links)
        equal = sum(item["temporal_relation"] == "EQUAL" for item in own_links)
        summaries.append(
            {
                **binding,
                **winner_locator,
                "validation_version": validation_version,
                "state": "LATER_INVALID_OBSERVED"
                if later
                else "TEMPORAL_ORDER_INDETERMINATE"
                if equal
                else "NO_LATER_INVALID_OBSERVED",
                "later_invalid_count": later,
                "equal_invalid_count": equal,
            }
        )
        links.extend(own_links)
    return summaries, links


def _locator(row: dict[str, Any], prefix: str, pages: dict[tuple[str, str], str]) -> dict[str, Any]:
    run, page = row["bronze_run_id"], row["bronze_page_path"]
    return {
        f"{prefix}_payload_sha256": row["payload_sha256"],
        f"{prefix}_run_id": run,
        f"{prefix}_page_path": page,
        f"{prefix}_page_sha256": pages[(run, page)],
        f"{prefix}_row_ordinal": row["bronze_row_ordinal"],
        f"{prefix}_observed_at_utc": _utc(row["bronze_fetched_at_utc"]),
        f"{prefix}_profile_sha256": row["source_contract_sha256"],
        f"{prefix}_projection_version": row["source_projection_version"],
    }


def _utc(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("Acquisition observation must be a UTC instant")
    # Spark collect uses host local time for TimestampType; callers serialize
    # observations as UTC strings before collection. Naive synthetic values are UTC.
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
