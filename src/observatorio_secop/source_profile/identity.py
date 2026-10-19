"""Focused, bounded platform identity evidence; never publishes source rows."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from observatorio_secop.source_profile.client import SocrataClient
from observatorio_secop.source_profile.config import SourceConfig
from observatorio_secop.source_profile.errors import InvalidResponseError

IDENTITY_FIELDS = (
    "id_contrato",
    "codigo_entidad",
    "codigo_proveedor",
    "nombre_entidad",
    "proveedor_adjudicado",
    "valor_del_contrato",
    "ultima_actualizacion",
)
PLACEHOLDERS = ("0", "-1", "N/A", "NA", "NULL", "NO DEFINIDO", "NO APLICA")


def summarize_identity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        raise InvalidResponseError("Identity sample is empty; evidence is insufficient")
    if any(set(row) - set(IDENTITY_FIELDS) for row in rows):
        raise InvalidResponseError(
            "Identity response contains fields outside the approved projection"
        )
    result: dict[str, Any] = {"rows": len(rows), "roles": {}}
    identifiers = [row.get("id_contrato") for row in rows]
    result["repeated_contract_observations"] = len(identifiers) - len(set(identifiers))
    amounts: list[Decimal | None] = []
    for row in rows:
        try:
            amount = Decimal(str(row.get("valor_del_contrato")))
            amounts.append(amount if amount.is_finite() and amount >= 0 else None)
        except InvalidOperation:
            amounts.append(None)
    result["invalid_amount_observations"] = amounts.count(None)
    result["value_denominator_cop"] = (
        str(sum((value for value in amounts if value is not None), Decimal("0.00")))
        if None not in amounts
        else None
    )
    for role, code_field, label_field in (
        ("entity", "codigo_entidad", "nombre_entidad"),
        ("supplier", "codigo_proveedor", "proveedor_adjudicado"),
    ):
        states: Counter[str] = Counter()
        shapes: Counter[str] = Counter()
        types: Counter[str] = Counter()
        labels_by_code: dict[str, set[str]] = defaultdict(set)
        codes_by_label: dict[str, set[str]] = defaultdict(set)
        raw_by_trimmed: dict[str, set[str]] = defaultdict(set)
        values: dict[str, Decimal] = defaultdict(lambda: Decimal("0.00"))
        missing_labels = 0
        for row, amount in zip(rows, amounts, strict=True):
            raw = row.get(code_field)
            text = raw.strip() if isinstance(raw, str) else None
            types[type(raw).__name__] += 1
            if code_field not in row:
                state = "ABSENT"
            elif raw is None:
                state = "NULL"
            elif text == "":
                state = "EMPTY"
            elif text is not None and text.upper() in PLACEHOLDERS:
                state = "PLACEHOLDER"
            elif text is not None and re.fullmatch(r"[0-9]+", text):
                state = "DIGIT_CODE_CANDIDATE"
            else:
                state = "OTHER_SHAPE"
            states[state] += 1
            if amount is not None:
                values[state] += amount
            label = row.get(label_field)
            if not isinstance(label, str) or not label.strip():
                missing_labels += 1
            if text:
                shapes["digits" if text.isascii() and text.isdigit() else "other"] += 1
                raw_by_trimmed[text].add(str(raw))
                if isinstance(label, str) and label.strip():
                    labels_by_code[text].add(label.strip())
                    codes_by_label[label.strip()].add(text)
        result["roles"][role] = {
            "code_states": dict(sorted(states.items())),
            "value_by_code_state_cop": {key: str(value) for key, value in sorted(values.items())},
            "decoded_code_types": dict(sorted(types.items())),
            "code_shapes": dict(sorted(shapes.items())),
            "distinct_codes": len(raw_by_trimmed),
            "missing_labels": missing_labels,
            "codes_with_multiple_labels": sum(len(v) > 1 for v in labels_by_code.values()),
            "labels_with_multiple_codes": sum(len(v) > 1 for v in codes_by_label.values()),
            "trim_equivalence_groups": sum(len(v) > 1 for v in raw_by_trimmed.values()),
        }
    return result


def observe_identity(config: SourceConfig, client: SocrataClient) -> dict[str, Any]:
    metadata = client.fetch_metadata()
    fields = {item.get("fieldName"): item.get("dataTypeName") for item in metadata["columns"]}
    if set(IDENTITY_FIELDS) - set(fields):
        raise InvalidResponseError("Live metadata lacks approved identity projection fields")
    query: dict[str, str | int] = {
        "$select": ",".join(IDENTITY_FIELDS),
        "$where": "departamento='Antioquia'",
        "$order": "id_contrato",
        "$limit": config.limits.sample_rows,
    }
    rows = client.query(**query)
    summary = summarize_identity(rows)
    return {
        "dataset_id": config.dataset_id,
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "query": query,
        "limits": asdict(config.limits),
        "http_attempts": client.budget.requests,
        "declared_types": {name: fields[name] for name in IDENTITY_FIELDS},
        "sample_sha256": hashlib.sha256(
            json.dumps(rows, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest(),
        "sample_at_limit": len(rows) == config.limits.sample_rows,
        "summary": summary,
        "limitations": [
            "Ordered bounded sample only; not a population or longitudinal stability estimate.",
            "Digit-code candidates are evidence, not finalized usability rules.",
            "Multiple labels are not proof of code reuse or real-world identity ambiguity.",
            "Numeric code lexemes are decoded as text without binary-float conversion.",
        ],
    }
