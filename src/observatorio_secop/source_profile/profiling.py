"""Deterministic profiling and evidence-based source decisions."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from itertools import combinations
from typing import Any

from observatorio_secop.source_profile.errors import ContractValidationError

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}(?:T.*)?$")


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value.strip())
    return " ".join(
        "".join(character for character in normalized if not unicodedata.combining(character))
        .casefold()
        .split()
    )


def classify_value(value: Any, declared_type: str | None = None) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, str):
        if declared_type == "number":
            try:
                Decimal(value)
                return "number"
            except InvalidOperation:
                return "text"
        if declared_type in {"calendar_date", "floating_timestamp"}:
            if DATE_PATTERN.match(value):
                try:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return "calendar_date"
                except ValueError:
                    pass
            return "text"
        if declared_type == "url" and isinstance(value, dict):
            return "url"
        return "text"
    return type(value).__name__


def profile_columns(
    metadata_columns: Iterable[dict[str, Any]],
    rows: list[dict[str, Any]],
    safe_example_fields: set[str],
    *,
    max_examples: int = 3,
) -> list[dict[str, Any]]:
    if not rows:
        raise ContractValidationError("Cannot profile an empty sample")

    profiles: list[dict[str, Any]] = []
    for column in sorted(metadata_columns, key=lambda item: str(item.get("fieldName", ""))):
        field = column.get("fieldName")
        if not isinstance(field, str) or not field:
            raise ContractValidationError("Metadata contains a column without fieldName")
        declared = str(column.get("dataTypeName", "unknown"))
        values = [row.get(field) for row in rows]
        observed = Counter(classify_value(value, declared) for value in values if value is not None)
        non_null_values = [value for value in values if value is not None]
        examples: list[Any] = []
        if field in safe_example_fields:
            for value in non_null_values:
                if value not in examples:
                    examples.append(value)
                if len(examples) == max_examples:
                    break
        compatible_types = _compatible_observed_types(declared)
        incompatible = sorted(set(observed) - compatible_types)
        profiles.append(
            {
                "field": field,
                "label": column.get("name", field),
                "declared_type": declared,
                "observed_types": dict(sorted(observed.items())),
                "rows_evaluated": len(rows),
                "present_count": len(non_null_values),
                "null_count": len(rows) - len(non_null_values),
                "null_rate": round((len(rows) - len(non_null_values)) / len(rows), 6),
                "sample_cardinality": len({_stable_value(value) for value in non_null_values}),
                "examples": examples,
                "compatibility": "incompatible" if incompatible else "compatible",
                "incompatible_observed_types": incompatible,
            }
        )
    return profiles


def _stable_value(value: Any) -> str:
    if isinstance(value, dict | list):
        import json

        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _compatible_observed_types(declared: str) -> set[str]:
    mapping = {
        "number": {"number"},
        "calendar_date": {"calendar_date"},
        "floating_timestamp": {"calendar_date"},
        "checkbox": {"boolean", "text"},
        "url": {"object", "text", "url"},
        "text": {"text"},
    }
    return mapping.get(declared, {declared})


def find_semantic_fields(metadata_columns: list[dict[str, Any]]) -> dict[str, str]:
    candidates: dict[str, list[tuple[int, str]]] = {
        "department": [],
        "municipality": [],
        "identity": [],
        "watermark": [],
    }
    for column in metadata_columns:
        field = str(column.get("fieldName", ""))
        label = normalize_text(str(column.get("name", "")))
        description = normalize_text(str(column.get("description", "")))
        if label == "departamento":
            candidates["department"].append((100, field))
        if label in {"ciudad", "municipio"}:
            candidates["municipality"].append((100, field))
        if "identificador del contrato firmado" in description:
            score = 100 + (20 if "plataforma" in description else 0)
            candidates["identity"].append((score, field))
        if "ultima actualizacion" in label or "ultima actualizacion" in description:
            candidates["watermark"].append((100, field))

    if not candidates["watermark"]:
        fallback_field, _captures_updates = find_watermark_candidate(metadata_columns)
        candidates["watermark"].append((1, fallback_field))

    selected: dict[str, str] = {}
    for capability, values in candidates.items():
        if not values:
            raise ContractValidationError(
                f"No observed metadata field supports the {capability} capability"
            )
        values.sort(key=lambda item: (-item[0], item[1]))
        if len(values) > 1 and values[0][0] == values[1][0]:
            raise ContractValidationError(f"Ambiguous metadata fields for {capability}")
        selected[capability] = values[0][1]
    return selected


def find_watermark_candidate(metadata_columns: list[dict[str, Any]]) -> tuple[str, bool]:
    update_candidates: list[str] = []
    fallback_candidates: list[tuple[int, str]] = []
    for column in metadata_columns:
        field = str(column.get("fieldName", ""))
        label = normalize_text(str(column.get("name", "")))
        description = normalize_text(str(column.get("description", "")))
        if column.get("dataTypeName") not in {"calendar_date", "floating_timestamp"}:
            continue
        if "ultima actualizacion" in label or "ultima actualizacion" in description:
            update_candidates.append(field)
            continue
        score = 0
        if "firma" in label or "firma" in description:
            score = 30
        elif "inicio" in label or "inicio" in description:
            score = 20
        elif "fin" in label or "fin" in description:
            score = 10
        fallback_candidates.append((score, field))
    if len(update_candidates) == 1:
        return update_candidates[0], True
    if len(update_candidates) > 1:
        raise ContractValidationError("Ambiguous update timestamp fields in observed metadata")
    if not fallback_candidates:
        raise ContractValidationError("No observed temporal field can support a watermark")
    fallback_candidates.sort(key=lambda item: (-item[0], item[1]))
    return fallback_candidates[0][1], False


def choose_identity(
    candidates: list[str], rows: list[dict[str, Any]], *, max_width: int = 3
) -> dict[str, Any]:
    for width in range(1, min(max_width, len(candidates)) + 1):
        for fields in combinations(candidates, width):
            values = [tuple(row.get(field) for field in fields) for row in rows]
            null_rows = sum(any(value in (None, "") for value in key) for key in values)
            duplicates = len(values) - len(set(values))
            if null_rows == 0 and duplicates == 0:
                return {
                    "status": "resolved",
                    "kind": "natural" if width == 1 else "composite",
                    "fields": list(fields),
                    "rows_evaluated": len(rows),
                    "null_rows": 0,
                    "duplicate_rows": 0,
                    "limitations": (
                        "Uniqueness is observed evidence, not a guarantee for future rows."
                    ),
                }
    raise ContractValidationError("No defensible natural or composite identity was observed")


def choose_watermark(
    field: str, rows: list[dict[str, Any]], identity_fields: list[str]
) -> dict[str, Any]:
    values = [row.get(field) for row in rows]
    parsed = [
        value for value in values if classify_value(value, "calendar_date") == "calendar_date"
    ]
    if not parsed or len(set(parsed)) < 2:
        raise ContractValidationError("No varying parseable watermark evidence was observed")
    return {
        "status": "resolved",
        "field": field,
        "declared_type": "calendar_date",
        "semantics": "source record last update",
        "precision": "milliseconds as published",
        "timezone": "not declared by source",
        "rows_evaluated": len(rows),
        "null_count": sum(value in (None, "") for value in values),
        "minimum": min(parsed),
        "maximum": max(parsed),
        "cardinality": len(set(parsed)),
        "captures_updates": True,
        "tie_breaker": identity_fields,
    }


def resolve_territory(
    department_rows: list[dict[str, Any]],
    municipality_rows: list[dict[str, Any]],
    expected_department: str,
    canonical_municipalities: tuple[str, ...],
) -> dict[str, Any]:
    department_matches = [
        row
        for row in department_rows
        if normalize_text(str(row.get("departamento", ""))) == normalize_text(expected_department)
    ]
    if len(department_matches) != 1:
        raise ContractValidationError(
            f"Expected one observed department value for {expected_department}, found "
            f"{len(department_matches)}"
        )

    observed_by_normalized: dict[str, list[dict[str, Any]]] = {}
    for row in municipality_rows:
        value = str(row.get("ciudad", ""))
        observed_by_normalized.setdefault(normalize_text(value), []).append(row)

    mapping: list[dict[str, Any]] = []
    missing: list[str] = []
    ambiguous: list[str] = []
    for canonical in canonical_municipalities:
        matches = observed_by_normalized.get(normalize_text(canonical), [])
        if not matches:
            missing.append(canonical)
            continue
        if len(matches) > 1:
            ambiguous.append(canonical)
            continue
        mapping.append(
            {
                "canonical": canonical,
                "observed": matches[0]["ciudad"],
                "row_count": _metric_int(matches[0], "rows"),
            }
        )
    if missing or ambiguous:
        raise ContractValidationError(
            f"Territorial mapping unresolved; missing={missing}, ambiguous={ambiguous}"
        )
    return {
        "department_value": department_matches[0]["departamento"],
        "department_row_count": _metric_int(department_matches[0], "rows"),
        "municipalities": mapping,
        "normalization": "Unicode accents, case, surrounding and repeated spaces are normalized",
    }


def resolve_identity_evidence(
    field: str, metrics: dict[str, Any], duplicate_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    total = _metric_int(metrics, "total")
    non_null = _metric_int(metrics, "non_null")
    distinct = _metric_int(metrics, "distinct_count")
    if total <= 0 or non_null != total or distinct != total or duplicate_rows:
        raise ContractValidationError(
            f"Identity candidate {field} is not defensible: total={total}, "
            f"non_null={non_null}, distinct={distinct}, duplicate_groups={len(duplicate_rows)}"
        )
    return {
        "status": "resolved",
        "kind": "natural",
        "fields": [field],
        "scope": "records whose observed department value is Antioquia",
        "rows_evaluated": total,
        "null_rows": total - non_null,
        "distinct_keys": distinct,
        "duplicate_groups": 0,
        "limitations": "Observed uniqueness does not guarantee uniqueness in future source rows.",
    }


def resolve_watermark_evidence(
    field: str,
    metrics: dict[str, Any],
    identity_fields: list[str],
    *,
    captures_updates: bool = True,
) -> dict[str, Any]:
    total = _metric_int(metrics, "total")
    non_null = _metric_int(metrics, "non_null")
    cardinality = _metric_int(metrics, "cardinality")
    minimum = metrics.get("minimum")
    maximum = metrics.get("maximum")
    if total <= 0 or non_null <= 0 or cardinality < 2 or not minimum or not maximum:
        raise ContractValidationError(f"Watermark candidate {field} lacks usable temporal evidence")
    if (
        classify_value(minimum, "calendar_date") != "calendar_date"
        or classify_value(maximum, "calendar_date") != "calendar_date"
    ):
        raise ContractValidationError(f"Watermark candidate {field} has non-parseable bounds")
    return {
        "status": "resolved",
        "field": field,
        "declared_type": "calendar_date",
        "semantics": (
            "source record last update" if captures_updates else "fallback event-time cursor"
        ),
        "precision": "milliseconds as published",
        "timezone": "not declared by source",
        "scope": "records whose observed department value is Antioquia",
        "rows_evaluated": total,
        "non_null_count": non_null,
        "null_count": total - non_null,
        "null_rate": round((total - non_null) / total, 6),
        "minimum": minimum,
        "maximum": maximum,
        "cardinality": cardinality,
        "captures_updates": captures_updates,
        "tie_breaker": identity_fields,
        "limitations": (
            "Records with a null update timestamp require a separate fallback in future ingestion."
            if captures_updates
            else "This event-time cursor does not capture later modifications to existing records."
        ),
    }


def _metric_int(row: dict[str, Any], field: str) -> int:
    try:
        return int(row[field])
    except (KeyError, TypeError, ValueError) as error:
        raise ContractValidationError(f"Aggregate metric {field} is missing or invalid") from error
