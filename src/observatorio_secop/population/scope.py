"""Governed signing-population meaning; source clocks have no inferred zone."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from observatorio_secop.population.core import PopulationError, digest_json, object_fields

SCOPE_SCHEMA_VERSION = "population-scope-v1"
_CONSTANTS = {
    "schema_version": SCOPE_SCHEMA_VERSION,
    "dataset_id": "jbjy-vk9h",
    "department_field": "departamento",
    "department_value": "Antioquia",
    "basis": "signing_interval",
    "signing_field": "fecha_de_firma",
    "missing_signing": "excluded",
    "source_clock": "unqualified_calendar_date",
}


def _bound(value: str) -> date:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise PopulationError("Signing bounds must be ISO dates (YYYY-MM-DD), without a zone")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise PopulationError("Signing bounds must be valid calendar dates") from error


@dataclass(frozen=True)
class PopulationScope:
    signing_from: str
    signing_to: str

    def __post_init__(self) -> None:
        if _bound(self.signing_from) >= _bound(self.signing_to):
            raise PopulationError("Signing bounds must be a nonempty half-open interval")

    def as_dict(self) -> dict[str, str]:
        return {**_CONSTANTS, "signing_from": self.signing_from, "signing_to": self.signing_to}

    @property
    def scope_id(self) -> str:
        return digest_json(self.as_dict())

    @classmethod
    def from_dict(cls, value: object) -> PopulationScope:
        item = object_fields(value, set(_CONSTANTS) | {"signing_from", "signing_to"}, "scope")
        if any(item[key] != expected for key, expected in _CONSTANTS.items()):
            raise PopulationError("Unsupported population scope semantics")
        return cls(item["signing_from"], item["signing_to"])

    def contains(self, signing_date: Any, department: str | None = "Antioquia") -> bool:
        """Evaluate membership without converting floating source dates to UTC."""
        if department != "Antioquia" or signing_date is None:
            return False
        if isinstance(signing_date, datetime):
            if signing_date.tzinfo is not None:
                return False
            signing_date = signing_date.date()
        elif isinstance(signing_date, str):
            try:
                if len(signing_date) == 10:
                    signing_date = _bound(signing_date)
                else:
                    parsed = datetime.fromisoformat(signing_date)
                    if parsed.tzinfo is not None:
                        return False
                    signing_date = parsed.date()
            except (ValueError, PopulationError):
                return False
        return isinstance(signing_date, date) and _bound(
            self.signing_from
        ) <= signing_date < _bound(self.signing_to)
