"""Serializable records used by Silver processing."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BronzePage:
    run_id: str
    path: Path
    relative_path: str
    lane: str
    sequence: int
    fetched_at_utc: str
    sha256: str
    row_count: int


@dataclass(frozen=True)
class VerifiedBronzeRun:
    run_id: str
    dataset_id: str
    contract_sha256: str
    manifest_path: Path
    manifest_sha256: str
    pages: tuple[BronzePage, ...]


@dataclass(frozen=True)
class QualityResult:
    check_id: str
    severity: str
    passed: bool
    observed: Any
    expected: Any
    affected_rows: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SilverManifest:
    silver_version_id: str
    dataset_id: str
    transformation_version: str
    key_rule_version: str
    contract_sha256: str
    config_sha256: str
    territory_catalog_sha256: str
    bronze_runs: list[dict[str, str]]
    counts: dict[str, int | float]
    quality_results: list[QualityResult]
    status: str
    started_at_utc: str
    finished_at_utc: str | None = None
    files: list[dict[str, Any]] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    schema_version: int = 1

    @property
    def critical_passed(self) -> bool:
        return all(item.passed for item in self.quality_results if item.severity == "critical")

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["quality_results"] = [item.as_dict() for item in self.quality_results]
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> SilverManifest:
        copied = dict(value)
        copied["quality_results"] = [
            QualityResult(**item) for item in copied.get("quality_results", [])
        ]
        return cls(**copied)
