"""Conservative platform-code identity and reproducible coverage arithmetic."""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

CONTRACTS_ROOT = Path(__file__).resolve().parents[2] / "contracts"
IDENTITY_RULE_VERSION = "platform-code-v1"
LEGACY_PROJECTION = "secop-contracts-v1"
IDENTITY_PROJECTION = "secop-contracts-v2"
IDENTITY_TEXT_FIELDS = (
    "entity_code_raw",
    "supplier_code_raw",
    "entity_code",
    "supplier_code",
    "supplier_name",
    "entity_identity_status",
    "supplier_identity_status",
    "entity_identity_reason",
    "supplier_identity_reason",
    "identity_rule_version",
    "source_contract_sha256",
    "source_projection_version",
)
REQUIRED_IDENTITY_FIELDS = {
    "entity_identity_status",
    "supplier_identity_status",
    "identity_rule_version",
    "source_projection_version",
}


def code_identity(
    raw: str | None, role: str, rules: dict[str, Any], *, legacy: bool = False
) -> tuple[str | None, str, str | None]:
    if legacy:
        return None, "UNKNOWN", "LEGACY_PROJECTION"
    if raw is None or not raw.strip():
        return None, "UNKNOWN", "MISSING_CODE"
    code = raw.strip()
    rule = rules["roles"][role]
    if code.upper() in rule["placeholders"]:
        return None, "UNKNOWN", "PLACEHOLDER_CODE"
    if not re.fullmatch(rule["pattern"], code):
        return None, "UNKNOWN", "INVALID_CODE"
    return code, "KNOWN", None


def load_identity_rules(path: Path = CONTRACTS_ROOT / "identity_rules.yaml") -> dict[str, Any]:
    value = yaml.safe_load(path.read_bytes())
    if value.get("version") != IDENTITY_RULE_VERSION or value.get("status") != "reviewed":
        raise ValueError("Identity rules are not reviewed/supported")
    evidence = path.parent / value["evidence_path"]
    if hashlib.sha256(evidence.read_bytes()).hexdigest() != value["evidence_sha256"]:
        raise ValueError("Identity rule evidence hash mismatch")
    for role in ("entity", "supplier"):
        rule = value["roles"][role]
        re.compile(rule["pattern"])
        if not isinstance(rule["placeholders"], list):
            raise ValueError("Invalid identity placeholder rule")
    return value


def key_set_digest(keys: list[str]) -> str:
    return hashlib.sha256(json.dumps(sorted(keys), separators=(",", ":")).encode()).hexdigest()


def identity_record_valid(row: dict[str, Any], rules: dict[str, Any]) -> bool:
    if row.get("identity_rule_version") != IDENTITY_RULE_VERSION:
        return False
    projection = row.get("source_projection_version")
    if projection not in (LEGACY_PROJECTION, IDENTITY_PROJECTION):
        return False
    for role in ("entity", "supplier"):
        expected = code_identity(
            row.get(f"{role}_code_raw"), role, rules, legacy=projection == LEGACY_PROJECTION
        )
        observed = (
            row.get(f"{role}_code"),
            row.get(f"{role}_identity_status"),
            row.get(f"{role}_identity_reason"),
        )
        if expected != observed:
            return False
    return True


def identity_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    total_value = sum((Decimal(str(row["contract_value_cop"])) for row in rows), Decimal("0.00"))
    for role in ("entity", "supplier"):
        known = [row for row in rows if row[f"{role}_identity_status"] == "KNOWN"]
        known_value = sum((Decimal(str(r["contract_value_cop"])) for r in known), Decimal("0.00"))
        reasons: dict[str, dict[str, Any]] = {}
        for row in rows:
            if row[f"{role}_identity_status"] == "UNKNOWN":
                reason = row[f"{role}_identity_reason"]
                entry = reasons.setdefault(reason, {"count": 0, "value_cop": Decimal("0.00")})
                entry["count"] += 1
                entry["value_cop"] += Decimal(str(row["contract_value_cop"]))
        result[role] = {
            "label_variant_groups": sum(
                len(
                    {
                        row.get(f"{role}_name")
                        for row in known
                        if row.get(f"{role}_code") == code and row.get(f"{role}_name")
                    }
                )
                > 1
                for code in {row.get(f"{role}_code") for row in known}
            ),
            "denominator": "accepted_deduplicated_contracts",
            "total_count": len(rows),
            "total_value_cop": str(total_value),
            "known_count": len(known),
            "unknown_count": len(rows) - len(known),
            "known_value_cop": str(known_value),
            "unknown_value_cop": str(total_value - known_value),
            "coverage_by_count": str(Decimal(len(known)) / len(rows)) if rows else None,
            "coverage_by_value": str(known_value / total_value) if total_value else None,
            "count_coverage_reason": None if rows else "EMPTY_POPULATION",
            "value_coverage_reason": None if total_value else "ZERO_TOTAL_VALUE",
            "unknown_reasons": {
                key: {"count": item["count"], "value_cop": str(item["value_cop"])}
                for key, item in sorted(reasons.items())
            },
        }
    return result
