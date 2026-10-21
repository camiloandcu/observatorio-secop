"""Small strict primitives shared across acquisition and publication boundaries."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from typing import Any


class PopulationError(ValueError):
    """Population evidence is malformed, inconsistent or unsupported."""


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return utc_text(value)
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"Unsupported canonical JSON value: {type(value).__name__}")


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def digest_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def object_fields(value: object, fields: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise PopulationError(f"{name} must contain exactly {sorted(fields)}")
    if any(not isinstance(key, str) for key in value):
        raise PopulationError(f"{name} keys must be strings")
    return value


def text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise PopulationError(f"{name} must be nonempty text")
    return value


def digest(value: object, name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise PopulationError(f"{name} must be a lowercase SHA-256 digest")
    return value


def integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise PopulationError(f"{name} must be an integer >= {minimum}")
    return value


def boolean(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise PopulationError(f"{name} must be a boolean")
    return value


def strings(value: object, name: str, *, unique: bool = True) -> list[str]:
    if not isinstance(value, list):
        raise PopulationError(f"{name} must be a list")
    result = [text(item, name) for item in value]
    if unique and len(result) != len(set(result)):
        raise PopulationError(f"{name} must not contain duplicates")
    return result


def utc_text(value: object, name: str = "observation time") -> str:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise PopulationError(f"{name} must be a UTC instant") from error
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise PopulationError(f"{name} must be a UTC instant")
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise PopulationError(f"{name} must have an explicit UTC offset")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def version_binding(value: object, *, keys: bool = False) -> dict[str, str] | None:
    if value is None:
        return None
    fields = {"silver_version_id", "manifest_sha256"}
    if keys:
        fields.add("keys_sha256")
    item = object_fields(value, fields, "version binding")
    result = {
        "silver_version_id": text(item["silver_version_id"], "silver_version_id"),
        "manifest_sha256": digest(item["manifest_sha256"], "manifest_sha256"),
    }
    if keys:
        result["keys_sha256"] = digest(item["keys_sha256"], "keys_sha256")
    return result
