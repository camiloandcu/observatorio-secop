"""Deterministic UTF8 JSON for identities and private execution messages."""

from __future__ import annotations

import json
from typing import Any


def canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode()


def parse_object(data: bytes) -> dict[str, Any]:
    from .errors import ExportError

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in pairs:
            if key in result:
                raise ExportError("EXPORT_PACKAGE_CONFLICT", "Duplicate manifest JSON key")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Nonfinite manifest JSON constant")

    result = json.loads(data, object_pairs_hook=unique, parse_constant=reject_constant)
    if not isinstance(result, dict):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Manifest must be an object")
    return result
