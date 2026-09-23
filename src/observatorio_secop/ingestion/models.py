"""Versioned records persisted by Bronze ingestion."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class PageRecord:
    lane: str
    sequence: int
    path: str
    sha256: str
    row_count: int
    status: int
    duration_ms: int
    retries: int
    requested_cursor: dict[str, str | None] | None
    final_cursor: dict[str, str | None] | None
    fetched_at_utc: str
    query: dict[str, str | int]


@dataclass
class RunManifest:
    run_id: str
    dataset_id: str
    contract_sha256: str
    parameters: dict[str, Any]
    checkpoint_before: str | None
    started_at_utc: str
    status: str = "staging"
    schema_version: int = 1
    pages: list[PageRecord] = field(default_factory=list)
    lanes: dict[str, dict[str, Any]] = field(default_factory=dict)
    counts: dict[str, int] = field(
        default_factory=lambda: {
            "received": 0,
            "written": 0,
            "new": 0,
            "repeated": 0,
            "new_versions": 0,
            "retries": 0,
        }
    )
    additive_drift: list[str] = field(default_factory=list)
    checkpoint_candidate: str | None = None
    range_complete: bool = False
    truncated: bool = False
    finished_at_utc: str | None = None
    token_used: bool = False
    coverage_limitations: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
