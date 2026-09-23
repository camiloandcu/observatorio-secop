"""Deterministic SoQL query construction for incremental lanes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from observatorio_secop.ingestion.config import RunWindow
from observatorio_secop.ingestion.contract import BronzeSourceContract
from observatorio_secop.ingestion.errors import IngestionConfigurationError


@dataclass(frozen=True, order=True)
class SourceCursor:
    timestamp: str | None
    identity: str

    def as_dict(self) -> dict[str, str | None]:
        return {"timestamp": self.timestamp, "identity": self.identity}


def primary_query(
    contract: BronzeSourceContract,
    projection: tuple[str, ...],
    window: RunWindow,
    *,
    limit: int,
    cursor: SourceCursor | None = None,
) -> dict[str, str | int]:
    _validate_limit(limit)
    watermark = contract.watermark_field
    identity = contract.identity_field
    predicates = [
        _equals(contract.department_field, contract.department_value),
        f"{watermark} is not null",
        f"{watermark} >= {_literal(_source_timestamp(window.effective_from_text))}",
        f"{watermark} <= {_literal(_source_timestamp(window.requested_to_text))}",
    ]
    if cursor:
        if cursor.timestamp is None:
            raise IngestionConfigurationError("Primary cursor requires a timestamp")
        timestamp = _literal(cursor.timestamp)
        identity_value = _literal(cursor.identity)
        predicates.append(
            f"({watermark} > {timestamp} OR "
            f"({watermark} = {timestamp} AND {identity} > {identity_value}))"
        )
    return {
        "$select": ",".join(projection),
        "$where": " AND ".join(predicates),
        "$order": f"{watermark} ASC,{identity} ASC",
        "$limit": limit,
    }


def null_watermark_query(
    contract: BronzeSourceContract,
    projection: tuple[str, ...],
    window: RunWindow,
    *,
    fallback_event_field: str,
    limit: int,
    cursor: SourceCursor | None = None,
) -> dict[str, str | int]:
    _validate_limit(limit)
    if fallback_event_field not in contract.columns:
        raise IngestionConfigurationError(
            f"Fallback event field {fallback_event_field} is absent from source contract"
        )
    watermark = contract.watermark_field
    identity = contract.identity_field
    predicates = [
        _equals(contract.department_field, contract.department_value),
        f"{watermark} is null",
        f"{fallback_event_field} >= {_literal(_source_timestamp(window.requested_from_text))}",
        f"{fallback_event_field} <= {_literal(_source_timestamp(window.requested_to_text))}",
    ]
    if cursor:
        predicates.append(f"{identity} > {_literal(cursor.identity)}")
    return {
        "$select": ",".join(projection),
        "$where": " AND ".join(predicates),
        "$order": f"{identity} ASC",
        "$limit": limit,
    }


def cursor_from_row(
    row: dict[str, object], contract: BronzeSourceContract, *, lane: str
) -> SourceCursor:
    identity = row.get(contract.identity_field)
    if not isinstance(identity, str) or not identity:
        raise IngestionConfigurationError("Page row is missing a usable identity")
    if lane == "primary":
        timestamp = row.get(contract.watermark_field)
        if not isinstance(timestamp, str) or not timestamp:
            raise IngestionConfigurationError("Primary page row is missing its watermark")
        return SourceCursor(timestamp=timestamp, identity=identity)
    return SourceCursor(timestamp=None, identity=identity)


def effective_start(
    requested_from: datetime, checkpoint_completed_through: datetime | None, overlap_days: int
) -> datetime:
    if checkpoint_completed_through is None:
        return requested_from
    from datetime import timedelta

    return max(requested_from, checkpoint_completed_through - timedelta(days=overlap_days))


def _equals(field: str, value: str) -> str:
    return f"{field}={_literal(value)}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _source_timestamp(value: str) -> str:
    """Socrata calendar_date values are published without a timezone suffix."""
    return value.removesuffix("Z")


def _validate_limit(limit: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise IngestionConfigurationError("Query limit must be a positive integer")
