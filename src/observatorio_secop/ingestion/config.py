"""Operational configuration and run-window validation for Bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.ingestion.errors import IngestionConfigurationError


@dataclass(frozen=True)
class IngestionConfig:
    page_size: int
    max_page_size: int
    max_rows: int
    overlap_days: int
    timeout_seconds: float
    max_retries: int
    backoff_base_seconds: float
    jitter_seconds: float
    max_retry_after_seconds: float
    max_response_bytes: int
    bronze_root: Path
    contract_path: Path
    fallback_event_field: str
    projection: tuple[str, ...]

    def with_overrides(
        self,
        *,
        page_size: int | None = None,
        max_rows: int | None = None,
        overlap_days: int | None = None,
    ) -> IngestionConfig:
        updated = replace(
            self,
            page_size=self.page_size if page_size is None else page_size,
            max_rows=self.max_rows if max_rows is None else max_rows,
            overlap_days=self.overlap_days if overlap_days is None else overlap_days,
        )
        validate_config(updated)
        return updated


@dataclass(frozen=True)
class RunWindow:
    requested_from: datetime
    requested_to: datetime
    effective_from: datetime

    @property
    def requested_from_text(self) -> str:
        return format_utc(self.requested_from)

    @property
    def requested_to_text(self) -> str:
        return format_utc(self.requested_to)

    @property
    def effective_from_text(self) -> str:
        return format_utc(self.effective_from)


def load_ingestion_config(path: Path) -> IngestionConfig:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        raw = document["ingestion"]
    except (OSError, KeyError, TypeError, yaml.YAMLError) as error:
        raise IngestionConfigurationError(
            f"Could not load ingestion config {path}: {error}"
        ) from error

    if not isinstance(raw, dict):
        raise IngestionConfigurationError("ingestion configuration must be a mapping")
    try:
        config = IngestionConfig(
            page_size=_integer(raw["page_size"], "page_size", minimum=1),
            max_page_size=_integer(raw["max_page_size"], "max_page_size", minimum=1),
            max_rows=_integer(raw["max_rows"], "max_rows", minimum=1),
            overlap_days=_integer(raw["overlap_days"], "overlap_days", minimum=0),
            timeout_seconds=_number(raw["timeout_seconds"], "timeout_seconds", positive=True),
            max_retries=_integer(raw["max_retries"], "max_retries", minimum=0),
            backoff_base_seconds=_number(
                raw["backoff_base_seconds"], "backoff_base_seconds", positive=True
            ),
            jitter_seconds=_number(raw["jitter_seconds"], "jitter_seconds", positive=False),
            max_retry_after_seconds=_number(
                raw["max_retry_after_seconds"], "max_retry_after_seconds", positive=True
            ),
            max_response_bytes=_integer(raw["max_response_bytes"], "max_response_bytes", minimum=1),
            bronze_root=Path(_text(raw["bronze_root"], "bronze_root")),
            contract_path=Path(_text(raw["contract_path"], "contract_path")),
            fallback_event_field=_text(raw["fallback_event_field"], "fallback_event_field"),
            projection=tuple(raw["projection"]),
        )
    except KeyError as error:
        raise IngestionConfigurationError(
            f"Missing ingestion configuration key: {error}"
        ) from error
    validate_config(config)
    return config


def validate_config(config: IngestionConfig) -> None:
    if config.page_size > config.max_page_size:
        raise IngestionConfigurationError("page_size cannot exceed max_page_size")
    if config.page_size > config.max_rows:
        raise IngestionConfigurationError("page_size cannot exceed max_rows in development mode")
    if not config.projection or not all(
        isinstance(field, str) and field for field in config.projection
    ):
        raise IngestionConfigurationError("projection must contain non-empty field names")
    if len(set(config.projection)) != len(config.projection):
        raise IngestionConfigurationError("projection contains duplicate fields")
    if config.jitter_seconds < 0:
        raise IngestionConfigurationError("jitter_seconds cannot be negative")


def build_run_window(
    requested_from: str,
    requested_to: str,
    *,
    checkpoint_completed_through: str | None,
    overlap_days: int,
) -> RunWindow:
    start = parse_utc(requested_from, "from")
    end = parse_utc(requested_to, "to")
    if start > end:
        raise IngestionConfigurationError("from must be earlier than or equal to to")
    effective = start
    if checkpoint_completed_through:
        checkpoint = parse_utc(checkpoint_completed_through, "checkpoint")
        effective = max(start, checkpoint - timedelta(days=overlap_days))
    if effective > end:
        raise IngestionConfigurationError(
            "checkpoint overlap begins after the requested range; choose a later range"
        )
    return RunWindow(start, end, effective)


def parse_utc(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise IngestionConfigurationError(f"{name} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise IngestionConfigurationError(f"{name} must include a UTC timezone")
    return parsed.astimezone(UTC)


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _integer(value: Any, name: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise IngestionConfigurationError(f"{name} must be an integer >= {minimum}")
    return value


def _number(value: Any, name: str, *, positive: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise IngestionConfigurationError(f"{name} must be numeric")
    result = float(value)
    if positive and result <= 0:
        raise IngestionConfigurationError(f"{name} must be positive")
    return result


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise IngestionConfigurationError(f"{name} must be non-empty text")
    return value
