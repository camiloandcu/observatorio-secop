"""Durable finite campaign grants and conservative request reservations."""

from __future__ import annotations

from typing import Any

from observatorio_secop.ingestion.errors import IngestionConfigurationError, RequestBudgetExceeded
from observatorio_secop.ingestion.models import RunManifest
from observatorio_secop.ingestion.storage import BronzeStorage, canonical_json, sha256_bytes

LIMITATIONS = [
    "MUTABLE_SOURCE_NOT_TRANSACTIONAL_SNAPSHOT",
    "MISSING_SIGNING_DATES_EXCLUDED",
    "ABSENT_BASELINE_MEMBERS_NOT_SOURCE_DELETIONS",
    "UNCONFIRMED_REQUESTS_CONSERVATIVELY_CHARGED",
]


def positive(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise IngestionConfigurationError(f"{name} must be a finite positive integer")
    return value


def progress(manifest: RunManifest) -> dict[str, Any]:
    assert isinstance(manifest.population, dict)
    return manifest.population


def consumed(manifest: RunManifest, invocation: int | None = None) -> dict[str, int]:
    reservations = progress(manifest)["reservations"]
    if invocation is not None:
        reservations = [r for r in reservations if r["invocation_id"] == invocation]
    return {"rows": sum(r["charged_rows"] for r in reservations), "attempts": len(reservations)}


class CampaignBudgetExceeded(RequestBudgetExceeded):
    def __init__(self, reason: str, *, terminal: bool) -> None:
        super().__init__(reason)
        self.reason = reason
        self.terminal = terminal


class CampaignLedger:
    def __init__(self, manifest: RunManifest, storage: BronzeStorage) -> None:
        self.manifest = manifest
        self.storage = storage
        self.lane = "metadata"
        self.query: dict[str, str | int] | None = None
        self.row_capacity = 0

    def grant(self, rows: int, attempts: int, now: str) -> None:
        positive(rows, "grant rows")
        positive(attempts, "grant attempts")
        pop = progress(self.manifest)
        totals = consumed(self.manifest)
        if any(pop["ceilings"][key] <= totals[key] for key in ("rows", "attempts")):
            raise CampaignBudgetExceeded("campaign_ceiling_exhausted", terminal=True)
        if rows > pop["ceilings"]["rows"] - totals["rows"] or (
            attempts > pop["ceilings"]["attempts"] - totals["attempts"]
        ):
            raise IngestionConfigurationError(
                "Invocation grant exceeds remaining campaign allowance"
            )
        if pop["invocations"] and pop["invocations"][-1]["state"] == "extracting":
            pop["invocations"][-1].update(
                state="interrupted", terminal_reason="unconfirmed_interruption"
            )
        pop["invocations"].append(
            {
                "invocation_id": len(pop["invocations"]) + 1,
                "grant_rows": rows,
                "grant_attempts": attempts,
                "started_at_utc": now,
                "consumed": {"rows": 0, "attempts": 0},
                "state": "extracting",
                "terminal_reason": None,
            }
        )

    def remaining(self, kind: str) -> int:
        pop = progress(self.manifest)
        invocation = pop["invocations"][-1]
        return int(
            min(
                pop["ceilings"][kind] - consumed(self.manifest)[kind],
                invocation[f"grant_{kind}"]
                - consumed(self.manifest, invocation["invocation_id"])[kind],
            )
        )

    def require(self, rows: int, attempts: int = 1) -> None:
        pop = progress(self.manifest)
        total = consumed(self.manifest)
        for kind, amount in (("rows", rows), ("attempts", attempts)):
            if amount > pop["ceilings"][kind] - total[kind]:
                raise CampaignBudgetExceeded(f"campaign_{kind}_exhausted", terminal=True)
            if amount > self.remaining(kind):
                raise CampaignBudgetExceeded(f"invocation_{kind}_exhausted", terminal=False)

    def reserve(self) -> None:
        """Called by the transport for every physical attempt, including retries."""
        self.require(self.row_capacity)
        pop = progress(self.manifest)
        pop["reservations"].append(
            {
                "reservation_id": len(pop["reservations"]) + 1,
                "invocation_id": pop["invocations"][-1]["invocation_id"],
                "lane": self.lane,
                "max_rows": self.row_capacity,
                "charged_rows": self.row_capacity,
                "state": "uncertain",
                "query_sha256": sha256_bytes(canonical_json(self.query)),
                "page_sequence": None,
            }
        )
        self.persist("extracting")

    def response(self, rows: int, page_sequence: int | None) -> None:
        reservation = progress(self.manifest)["reservations"][-1]
        reservation.update(state="response", charged_rows=rows, page_sequence=page_sequence)

    def persist(self, state: str) -> None:
        complete = all(lane["complete"] for lane in self.manifest.lanes.values())
        progress(self.manifest)["complete"] = self.manifest.range_complete = complete
        record_invocation(self.manifest, state)
        update_acquisition(self.manifest, state)
        self.storage.write_page_state(self.manifest.run_id, self.manifest)


def record_invocation(manifest: RunManifest, state: str) -> None:
    pop = progress(manifest)
    if pop["invocations"]:
        invocation = pop["invocations"][-1]
        invocation.update(
            consumed=consumed(manifest, invocation["invocation_id"]),
            state=state,
            terminal_reason=pop["terminal_reason"],
        )


def update_acquisition(manifest: RunManifest, state: str) -> dict[str, Any]:
    pop = progress(manifest)
    baseline = pop["baseline"]
    times = sorted(page.fetched_at_utc for page in manifest.pages)
    snapshot = {
        "schema_version": "population-acquisition-v1",
        "campaign_id": manifest.run_id,
        "plan_id": pop["plan_id"],
        "scope_id": pop["scope_id"],
        "baseline": (
            {
                "silver_version_id": baseline["version_id"],
                "manifest_sha256": baseline["manifest_sha256"],
                "keys_sha256": baseline["keys_sha256"],
            }
            if baseline
            else None
        ),
        "state": state,
        "limits": dict(pop["ceilings"]),
        "consumed": consumed(manifest),
        "scope_scan_complete": manifest.lanes["scope_scan"]["complete"],
        "baseline_check_complete": manifest.lanes["baseline_check"]["complete"],
        "unconfirmed_ids": manifest.lanes["baseline_check"]["missing_ids"],
        "observed_from_utc": times[0] if times else None,
        "observed_to_utc": times[-1] if times else None,
        "truncated": state == "truncated",
        "limitations": list(LIMITATIONS),
    }
    pop["acquisition"] = snapshot
    return snapshot
