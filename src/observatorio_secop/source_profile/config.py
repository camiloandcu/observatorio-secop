"""Load bounded, secret-free configuration for SECOP source profiling."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.source_profile.errors import ConfigurationError


@dataclass(frozen=True)
class Limits:
    sample_rows: int
    max_query_rows: int
    max_requests: int
    max_response_bytes: int
    timeout_seconds: float
    retries: int


@dataclass(frozen=True)
class SourceConfig:
    dataset_id: str
    api_base_url: str
    limits: Limits
    fixture_max_rows: int
    fixture_allowed_fields: tuple[str, ...]
    department: str
    municipalities: tuple[str, ...]


def _positive_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive integer")
    return value


def load_config(path: Path) -> SourceConfig:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ConfigurationError(f"Could not load configuration {path}: {error}") from error

    if not isinstance(raw, dict):
        raise ConfigurationError("Source configuration must be a mapping")

    try:
        source = raw["source"]
        limits = raw["limits"]
        fixture = raw["fixture"]
        territory = raw["territory"]
        dataset_id = source["dataset_id"]
        api_base_url = source["api_base_url"]
        allowed_fields = fixture["allowed_fields"]
        municipalities = territory["valle_de_aburra"]
        department = territory["department"]
    except (KeyError, TypeError) as error:
        raise ConfigurationError(f"Missing required configuration key: {error}") from error

    if dataset_id != "jbjy-vk9h":
        raise ConfigurationError("dataset_id must identify the reviewed SECOP II source jbjy-vk9h")
    if not isinstance(api_base_url, str) or not api_base_url.startswith("https://"):
        raise ConfigurationError("api_base_url must use HTTPS")
    if not isinstance(allowed_fields, list) or not all(
        isinstance(field, str) and field for field in allowed_fields
    ):
        raise ConfigurationError("fixture.allowed_fields must contain field names")
    if not isinstance(municipalities, list) or len(municipalities) != 10:
        raise ConfigurationError("territory.valle_de_aburra must contain ten municipalities")
    if not isinstance(department, str) or not department:
        raise ConfigurationError("territory.department must be a non-empty string")

    parsed_limits = Limits(
        sample_rows=_positive_int(limits.get("sample_rows"), "limits.sample_rows"),
        max_query_rows=_positive_int(limits.get("max_query_rows"), "limits.max_query_rows"),
        max_requests=_positive_int(limits.get("max_requests"), "limits.max_requests"),
        max_response_bytes=_positive_int(
            limits.get("max_response_bytes"), "limits.max_response_bytes"
        ),
        timeout_seconds=float(limits.get("timeout_seconds", 0)),
        retries=_positive_int(limits.get("retries"), "limits.retries"),
    )
    if parsed_limits.sample_rows > parsed_limits.max_query_rows:
        raise ConfigurationError("sample_rows cannot exceed max_query_rows")
    if parsed_limits.timeout_seconds <= 0:
        raise ConfigurationError("timeout_seconds must be positive")

    return SourceConfig(
        dataset_id=dataset_id,
        api_base_url=api_base_url.rstrip("/"),
        limits=parsed_limits,
        fixture_max_rows=_positive_int(fixture.get("max_rows"), "fixture.max_rows"),
        fixture_allowed_fields=tuple(allowed_fields),
        department=department,
        municipalities=tuple(municipalities),
    )
