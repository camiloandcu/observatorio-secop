"""Lossless scalar codecs and recursive fail-closed nested-data validation."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

from .contract import Column, Contract
from .errors import ExportError

PRIVATE = re.compile(r"(?:/home/|/mnt/|/tmp/|(?<![a-zA-Z0-9])[a-zA-Z]:[\\/]|\\\\|\w@\w+\.)")
NUMERIC = re.compile(r"-?\d+(?:\.\d+)?\Z")


def contains_private_text(value: str) -> bool:
    # Every forbidden alternative contains one of these separators. Most identity
    # digests/codes contain none; avoid regex work on those unchanged values.
    return ("/" in value or "\\" in value or "@" in value) and bool(PRIVATE.search(value))


def fail() -> None:
    raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Value violates its reviewed export codec")


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, str | Decimal) or not NUMERIC.fullmatch(str(value)):
        fail()
    result = Decimal(value)
    if not result.is_finite():
        fail()
    return result


def check_json(value: Any, policy: dict[str, Any], contract: Contract, depth: int = 0) -> None:
    if depth > contract.max_json_depth:
        fail()
    if value is not None:
        if ("keys" in policy or policy.get("integer_map")) and not isinstance(value, dict):
            fail()
        if "array" in policy and not isinstance(value, list):
            fail()
    if isinstance(value, dict):
        if policy.get("integer_map"):
            for key, count in value.items():
                if not isinstance(key, str):
                    fail()
                check_json(key, {}, contract, depth + 1)
                if type(count) is not int or count < 0:
                    fail()
            return
        if "keys" not in policy or not set(value) <= set(policy["keys"]):
            fail()
        for key, item in value.items():
            check_json(item, policy.get("children", {}).get(key, {}), contract, depth + 1)
    elif isinstance(value, list):
        if policy and "array" not in policy:
            fail()
        for item in value:
            if not policy and isinstance(item, dict | list):
                fail()
            check_json(item, policy.get("array", {}), contract, depth + 1)
    elif isinstance(value, str):
        if (
            len(value.encode()) > contract.scalar_max_bytes
            or "/" in value
            or "\\" in value
            or "@" in value
            or any(ord(ch) < 32 for ch in value)
        ):
            fail()
    elif isinstance(value, Decimal):
        if not value.is_finite():
            fail()
    elif value is not None and type(value) not in (bool, int):
        fail()


def source_url(value: str, contract: Contract) -> str:
    if any(ord(ch) < 33 for ch in value) or contains_private_text(unquote(value)):
        fail()
    try:
        p = urlsplit(value)
        if (
            p.scheme != "https"
            or p.hostname not in contract.url_hosts
            or p.username
            or p.password
            or p.port not in (None, 443)
            or p.fragment
            or p.path != "/Public/Tendering/OpportunityDetail/Index"
        ):
            fail()
        permitted = {"noticeuid", "isfrompublicarea", "ismodal", "aspopupview"}
        for key, item in parse_qsl(p.query, keep_blank_values=True):
            if key.lower() not in permitted or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", item):
                fail()
    except ValueError as error:
        raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Disallowed process link") from error
    return value


def encode(value: Any, column: Column, contract: Contract) -> Any:
    if value is None:
        return None
    codec = column.codec
    if codec == "text":
        if not isinstance(value, str) or contains_private_text(value):
            fail()
        if column.name in ("source_url", "source_process_url"):
            return source_url(value, contract)
        return value
    if codec.startswith("int"):
        bits = int(codec[3:])
        if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
            fail()
        return value
    if codec == "bool":
        if type(value) is not bool:
            fail()
        return value
    if codec == "date":
        if type(value) is not date:
            fail()
        return value
    if codec in ("decimal", "decimal-text-v1"):
        d = _decimal(value)
        if codec == "decimal-text-v1":
            return str(value)
        try:
            with localcontext() as context:
                context.prec = max(50, len(d.as_tuple().digits) + column.scale + 1)
                quantized = d.quantize(Decimal(1).scaleb(-column.scale))
                if quantized != d or abs(quantized) >= Decimal(10) ** (
                    column.precision - column.scale
                ):
                    fail()
                return quantized
        except InvalidOperation as error:
            raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Unrepresentable decimal") from error
    if codec in ("utc-instant-us-v1", "wall-clock-us-v1"):
        if not isinstance(value, datetime):
            fail()
        if codec == "utc-instant-us-v1":
            # Naive instants are accepted ONLY for columns whose reviewed codec declares UTC.
            instant = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            return instant.isoformat(timespec="microseconds").replace("+00:00", "Z")
        if value.tzinfo is not None:
            fail()
        return value.isoformat(timespec="microseconds")
    if codec in ("json-text-v1", "array-json-v1"):
        if codec == "array-json-v1" and isinstance(value, list):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if not isinstance(value, str):
            fail()
        try:

            def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
                result = {}
                for key, item in pairs:
                    if key in result:
                        fail()
                    result[key] = item
                return result

            parsed = json.loads(
                value,
                parse_float=Decimal,
                parse_constant=lambda _: fail(),
                object_pairs_hook=unique,
            )
        except (ValueError, RecursionError) as error:
            raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Invalid nested encoding") from error
        if codec == "array-json-v1":
            if not isinstance(parsed, list) or any(
                x is not None and not isinstance(x, str) for x in parsed
            ):
                fail()
            for item in parsed:
                if item is not None:
                    check_json(item, {}, contract)
        else:
            check_json(parsed, contract.policies[column.json_policy or ""], contract)
        return value
    fail()


def canonical_cell(value: Any, column: Column) -> bytes:
    if value is None:
        return b"N;"
    if column.codec == "decimal":
        text = format(value, f".{column.scale}f")
    elif type(value) is bool:
        text = "true" if value else "false"
    elif isinstance(value, date):
        text = value.isoformat()
    else:
        text = str(value)
    raw = text.encode("utf-8")
    return b"V" + str(len(raw)).encode("ascii") + b":" + raw + b";"


def validate_stored(value: Any, column: Column, contract: Contract) -> None:
    """Reapply the allowlist to decoded Parquet, including exact text codecs."""
    logical = value
    if value is not None and column.codec in ("utc-instant-us-v1", "wall-clock-us-v1"):
        if not isinstance(value, str):
            fail()
        try:
            logical = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            fail()
    if encode(logical, column, contract) != value:
        fail()
