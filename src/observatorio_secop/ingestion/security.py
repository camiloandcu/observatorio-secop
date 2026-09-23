"""Central redaction for operational output."""

from __future__ import annotations

from typing import Any

SENSITIVE_NAMES = {"authorization", "x-app-token", "token", "payload", "body", "headers"}


def redact(value: Any, *, secrets: tuple[str, ...] = ()) -> Any:
    """Return a JSON-safe value with secret-bearing keys and values removed."""
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]"
            if str(key).casefold() in SENSITIVE_NAMES
            else redact(item, secrets=secrets)
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [redact(item, secrets=secrets) for item in value]
    if isinstance(value, str):
        result = value
        for secret in secrets:
            if secret:
                result = result.replace(secret, "[REDACTED]")
        return result
    if value is None or isinstance(value, bool | int | float):
        return value
    return str(value)
