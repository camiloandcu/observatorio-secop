from __future__ import annotations

import pytest

from observatorio_secop.source_profile.errors import ContractValidationError
from observatorio_secop.source_profile.profiling import (
    choose_identity,
    choose_watermark,
    find_semantic_fields,
    find_watermark_candidate,
    normalize_text,
    profile_columns,
    resolve_identity_evidence,
    resolve_territory,
    resolve_watermark_evidence,
)

METADATA = [
    {
        "name": "Departamento",
        "fieldName": "departamento",
        "dataTypeName": "text",
        "description": "Departamento de la entidad",
    },
    {
        "name": "Ciudad",
        "fieldName": "ciudad",
        "dataTypeName": "text",
        "description": "Ciudad de la entidad",
    },
    {
        "name": "ID Contrato",
        "fieldName": "id_contrato",
        "dataTypeName": "text",
        "description": "Identificador del contrato firmado, generado por la plataforma",
    },
    {
        "name": "Ultima Actualizacion",
        "fieldName": "ultima_actualizacion",
        "dataTypeName": "calendar_date",
        "description": "Fecha de última actualización del contrato electrónico",
    },
    {
        "name": "Valor",
        "fieldName": "valor",
        "dataTypeName": "number",
        "description": "Valor del contrato",
    },
]


def test_profile_is_deterministic_and_reports_nulls_cardinality_and_bad_type() -> None:
    rows = [
        {
            "id_contrato": "2",
            "departamento": "Antioquia",
            "ciudad": "Medellín",
            "ultima_actualizacion": "2025-01-02T00:00:00.000",
            "valor": "not-a-number",
        },
        {
            "id_contrato": "1",
            "departamento": "Antioquia",
            "ciudad": None,
            "ultima_actualizacion": "2025-01-01T00:00:00.000",
            "valor": "10",
        },
    ]
    first = profile_columns(METADATA, rows, {"ciudad"})
    second = profile_columns(METADATA, rows, {"ciudad"})

    assert first == second
    assert [column["field"] for column in first] == sorted(column["field"] for column in first)
    city = next(column for column in first if column["field"] == "ciudad")
    assert city["null_count"] == 1
    assert city["sample_cardinality"] == 1
    assert city["examples"] == ["Medellín"]
    value = next(column for column in first if column["field"] == "valor")
    assert value["compatibility"] == "incompatible"
    assert value["incompatible_observed_types"] == ["text"]


def test_sensitive_examples_are_not_exposed() -> None:
    profile = profile_columns(METADATA, [{"id_contrato": "secret"}], set())
    identity = next(column for column in profile if column["field"] == "id_contrato")
    assert identity["examples"] == []


def test_semantic_fields_come_from_observed_metadata() -> None:
    assert find_semantic_fields(METADATA) == {
        "department": "departamento",
        "municipality": "ciudad",
        "identity": "id_contrato",
        "watermark": "ultima_actualizacion",
    }


def test_missing_or_ambiguous_semantic_field_is_rejected() -> None:
    with pytest.raises(ContractValidationError, match="watermark"):
        find_semantic_fields(METADATA[:-2] + [METADATA[-1]])


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(" Medellín ", "medellin"), ("ITAGÜÍ", "itagui"), ("La   Estrella", "la estrella")],
)
def test_territorial_normalization(raw: str, expected: str) -> None:
    assert normalize_text(raw) == expected


def test_identity_prefers_natural_then_minimal_composite() -> None:
    natural = choose_identity(["id"], [{"id": "1"}, {"id": "2"}])
    assert natural["kind"] == "natural"

    composite = choose_identity(
        ["reference", "entity"],
        [
            {"reference": "A", "entity": "1"},
            {"reference": "A", "entity": "2"},
            {"reference": "B", "entity": "1"},
            {"reference": "B", "entity": "2"},
        ],
    )
    assert composite["kind"] == "composite"
    assert composite["fields"] == ["reference", "entity"]


def test_identity_rejects_nulls_and_collisions() -> None:
    with pytest.raises(ContractValidationError, match="No defensible"):
        choose_identity(["id"], [{"id": None}, {"id": None}])


def test_watermark_uses_identity_as_tie_breaker() -> None:
    decision = choose_watermark(
        "updated",
        [
            {"updated": "2025-01-01T00:00:00.000"},
            {"updated": "2025-01-02T00:00:00.000"},
        ],
        ["id"],
    )
    assert decision["captures_updates"] is True
    assert decision["tie_breaker"] == ["id"]


def test_watermark_rejects_invalid_or_constant_values() -> None:
    with pytest.raises(ContractValidationError, match="watermark"):
        choose_watermark("updated", [{"updated": "bad"}, {"updated": "bad"}], ["id"])


def test_territory_maps_normalized_variants_and_counts() -> None:
    decision = resolve_territory(
        [{"departamento": "Antioquia", "rows": "20"}],
        [
            {"ciudad": "Medellín", "rows": "12"},
            {"ciudad": "Itagui", "rows": "8"},
        ],
        "ANTIOQUIA",
        ("Medellín", "Itagüí"),
    )
    assert decision["department_row_count"] == 20
    assert decision["municipalities"][1]["observed"] == "Itagui"


def test_territory_rejects_missing_or_ambiguous_municipality() -> None:
    with pytest.raises(ContractValidationError, match="missing=.*Bello"):
        resolve_territory(
            [{"departamento": "Antioquia", "rows": "1"}],
            [{"ciudad": "Medellín", "rows": "1"}],
            "Antioquia",
            ("Medellín", "Bello"),
        )


def test_identity_aggregate_evidence_must_be_complete_and_unique() -> None:
    decision = resolve_identity_evidence(
        "id_contrato",
        {"total": "2", "non_null": "2", "distinct_count": "2"},
        [],
    )
    assert decision["kind"] == "natural"
    with pytest.raises(ContractValidationError, match="not defensible"):
        resolve_identity_evidence(
            "id_contrato",
            {"total": "2", "non_null": "2", "distinct_count": "1"},
            [{"id_contrato": "duplicate", "occurrences": "2"}],
        )


def test_watermark_candidate_prefers_update_and_can_fall_back() -> None:
    assert find_watermark_candidate(METADATA) == ("ultima_actualizacion", True)
    fallback_metadata = [
        {
            "name": "Fecha de Firma",
            "fieldName": "fecha_de_firma",
            "dataTypeName": "calendar_date",
            "description": "Fecha de firma del contrato",
        }
    ]
    assert find_watermark_candidate(fallback_metadata) == ("fecha_de_firma", False)
    metadata_with_fallback = [*METADATA[:-2], *fallback_metadata, METADATA[-1]]
    assert find_semantic_fields(metadata_with_fallback)["watermark"] == "fecha_de_firma"


def test_fallback_watermark_discloses_limited_coverage() -> None:
    decision = resolve_watermark_evidence(
        "fecha_de_firma",
        {
            "total": "2",
            "non_null": "2",
            "cardinality": "2",
            "minimum": "2024-01-01T00:00:00.000",
            "maximum": "2024-01-02T00:00:00.000",
        },
        ["id_contrato"],
        captures_updates=False,
    )
    assert decision["captures_updates"] is False
    assert "does not capture" in decision["limitations"]


def test_watermark_metrics_reject_invalid_bounds() -> None:
    with pytest.raises(ContractValidationError, match="non-parseable"):
        resolve_watermark_evidence(
            "updated",
            {
                "total": "2",
                "non_null": "2",
                "cardinality": "2",
                "minimum": "invalid",
                "maximum": "also-invalid",
            },
            ["id"],
        )
