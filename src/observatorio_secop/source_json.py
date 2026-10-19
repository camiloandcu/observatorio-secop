"""Decode JSON without a floating-point round trip for platform identifiers."""

from __future__ import annotations

import json
from typing import Any

CODE_FIELDS = ("codigo_entidad", "codigo_proveedor")


def source_json_loads(raw: bytes | str) -> Any:
    """Keep ordinary field types, but capture numeric code tokens exactly as text."""
    value = json.loads(raw)
    exact = json.loads(raw, parse_int=str, parse_float=str)
    if isinstance(value, list) and isinstance(exact, list):
        for row, tokens in zip(value, exact, strict=True):
            if isinstance(row, dict) and isinstance(tokens, dict):
                for field in CODE_FIELDS:
                    if field in tokens and not isinstance(row[field], bool):
                        row[field] = tokens[field]
    return value
