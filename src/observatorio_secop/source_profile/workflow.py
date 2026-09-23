"""Orchestrate bounded live observation and build reviewable artifacts."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from observatorio_secop.source_profile.client import SocrataClient
from observatorio_secop.source_profile.config import SourceConfig
from observatorio_secop.source_profile.contract import validate_fixture
from observatorio_secop.source_profile.profiling import (
    find_semantic_fields,
    find_watermark_candidate,
    profile_columns,
    resolve_identity_evidence,
    resolve_territory,
    resolve_watermark_evidence,
)


def observe_source(
    config: SourceConfig,
    client: SocrataClient,
    *,
    observed_at: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], str]:
    metadata = client.fetch_metadata()
    metadata_columns = metadata["columns"]
    semantic_fields = find_semantic_fields(metadata_columns)
    all_fields = [column["fieldName"] for column in metadata_columns]
    selected_fields = ",".join(all_fields)
    department_field = semantic_fields["department"]
    municipality_field = semantic_fields["municipality"]
    identity_field = semantic_fields["identity"]
    watermark_field, captures_updates = find_watermark_candidate(metadata_columns)
    department_literal = _soql_literal(config.department)

    sample = client.query(
        **{
            "$select": selected_fields,
            "$where": f"{department_field}={department_literal}",
            "$order": identity_field,
            "$limit": config.limits.sample_rows,
        }
    )
    profiles = profile_columns(
        metadata_columns,
        sample,
        set(config.fixture_allowed_fields),
    )

    department_rows = client.query(
        **{
            "$select": f"{department_field} as departamento,count(*) as rows",
            "$group": department_field,
            "$order": "rows desc",
            "$limit": 100,
        }
    )
    municipality_rows = client.query(
        **{
            "$select": f"{municipality_field} as ciudad,count(*) as rows",
            "$where": f"{department_field}={department_literal}",
            "$group": municipality_field,
            "$order": "rows desc",
            "$limit": config.limits.max_query_rows,
        }
    )
    territory_evidence = resolve_territory(
        department_rows,
        municipality_rows,
        config.department,
        config.municipalities,
    )

    identity_metrics = client.query(
        **{
            "$select": (
                f"count(*) as total,count({identity_field}) as non_null,"
                f"count(distinct {identity_field}) as distinct_count"
            ),
            "$where": f"{department_field}={department_literal}",
            "$limit": 1,
        }
    )[0]
    duplicate_rows = client.query(
        allow_empty=True,
        **{
            "$select": f"{identity_field},count(*) as occurrences",
            "$where": f"{department_field}={department_literal}",
            "$group": identity_field,
            "$having": "count(*) > 1",
            "$order": "occurrences desc",
            "$limit": 10,
        },
    )
    identity = resolve_identity_evidence(identity_field, identity_metrics, duplicate_rows)

    watermark_metrics = client.query(
        **{
            "$select": (
                f"count(*) as total,count({watermark_field}) as non_null,"
                f"count(distinct {watermark_field}) as cardinality,"
                f"min({watermark_field}) as minimum,max({watermark_field}) as maximum"
            ),
            "$where": f"{department_field}={department_literal}",
            "$limit": 1,
        }
    )[0]
    incremental_cursor = resolve_watermark_evidence(
        watermark_field,
        watermark_metrics,
        identity["fields"],
        captures_updates=captures_updates,
    )

    fixture = []
    for mapping in territory_evidence["municipalities"]:
        fixture.extend(
            client.query(
                **{
                    "$select": ",".join(config.fixture_allowed_fields),
                    "$where": (
                        f"{department_field}={department_literal} AND "
                        f"{municipality_field}={_soql_literal(mapping['observed'])} AND "
                        f"{identity_field} is not null AND {watermark_field} is not null"
                    ),
                    "$order": identity_field,
                    "$limit": 1,
                }
            )
        )

    critical_requirements = {
        identity_field: ["identity", "traceability", "incremental tie-breaker"],
        watermark_field: ["incremental cursor"],
        department_field: ["department filter"],
        municipality_field: ["municipality mapping"],
    }
    contract_columns = []
    for profile in profiles:
        field = profile["field"]
        declared = profile["declared_type"]
        contract_columns.append(
            {
                **profile,
                "critical": field in critical_requirements,
                "required_for": critical_requirements.get(field, []),
                "compatible_observed_types": sorted(_compatible_types(declared)),
            }
        )

    observation_time = observed_at or datetime.now(UTC).isoformat(timespec="seconds")
    contract = {
        "contract_version": 1,
        "source": {
            "provider": "Datos Abiertos Colombia / SECOP II",
            "dataset_id": config.dataset_id,
            "dataset_name": metadata.get("name"),
            "api_base_url": config.api_base_url,
            "metadata_rows_updated_at": metadata.get("rowsUpdatedAt"),
        },
        "observation": {
            "observed_at_utc": observation_time,
            "scope": f"{department_field}={department_literal}",
            "sample_rows": len(sample),
            "sample_limit": config.limits.sample_rows,
            "max_query_rows": config.limits.max_query_rows,
            "request_count": client.budget.requests,
            "max_requests": config.limits.max_requests,
            "max_response_bytes": config.limits.max_response_bytes,
        },
        "columns": contract_columns,
        "territory": {
            "department_field": department_field,
            "department_value": territory_evidence["department_value"],
            "department_row_count": territory_evidence["department_row_count"],
            "department_predicate": f"{department_field}={department_literal}",
            "municipality_field": municipality_field,
            "normalization": territory_evidence["normalization"],
            "valle_de_aburra": territory_evidence["municipalities"],
        },
        "identity": identity,
        "incremental_cursor": incremental_cursor,
        "fixture": {
            "format": "JSON array",
            "max_rows": config.fixture_max_rows,
            "allowed_fields": list(config.fixture_allowed_fields),
            "selection": "one deterministic safe row per Valle de Aburrá municipality",
        },
        "change_policy": {
            "critical_column_missing": "fail",
            "incompatible_type": "fail",
            "optional_column_added": "report",
            "network_failure": "fail without replacing existing artifacts",
            "empty_response": "fail without replacing existing artifacts",
        },
        "differences": [
            {
                "topic": "territorial naming",
                "observed": "The source publishes ciudad and spells Itagüí as Itagui.",
                "decision": "Keep the observed value in the source contract and map it to Itagüí.",
            },
            {
                "topic": "analytical update timestamp",
                "observed": "The source field is ultima_actualizacion and contains nulls.",
                "decision": (
                    "Map it later to the analytical source_updated_at concept without claiming "
                    "complete incremental coverage for null rows."
                ),
            },
        ],
    }
    validate_fixture(contract, fixture)
    report = render_report(contract)
    return contract, fixture, report


def render_report(contract: dict[str, Any]) -> str:
    observation = contract["observation"]
    territory = contract["territory"]
    identity = contract["identity"]
    cursor = contract["incremental_cursor"]
    lines = [
        "# Perfil de la fuente SECOP II",
        "",
        "Este documento resume una observación controlada del conjunto público "
        "`SECOP II - Contratos Electrónicos` (`jbjy-vk9h`). No es una descarga del "
        "histórico nacional ni una garantía sobre datos futuros.",
        "",
        "## Alcance observado",
        "",
        f"- Fecha UTC: `{observation['observed_at_utc']}`.",
        f"- Filtro: `{territory['department_predicate']}`.",
        f"- Registros reportados para Antioquia: {territory['department_row_count']:,}.",
        f"- Muestra de perfilado: {observation['sample_rows']} registros "
        f"(límite {observation['sample_limit']}).",
        f"- Presupuesto usado: {observation['request_count']} de "
        f"{observation['max_requests']} solicitudes; máximo "
        f"{observation['max_query_rows']} filas por respuesta.",
        "",
        "## Decisiones verificadas",
        "",
        f"La llave natural es `{identity['fields'][0]}`. En "
        f"{identity['rows_evaluated']:,} registros de Antioquia se observaron "
        f"{identity['null_rows']} nulos y {identity['duplicate_groups']} grupos duplicados.",
        "",
        f"El watermark es `{cursor['field']}` y se desempata con "
        f"`{identity['fields'][0]}`. Tiene {cursor['non_null_count']:,} valores no nulos "
        f"de {cursor['rows_evaluated']:,} registros ({cursor['null_rate']:.2%} nulos), "
        f"con rango `{cursor['minimum']}` a `{cursor['maximum']}`. La fuente lo describe "
        "como la última actualización del contrato; no declara zona horaria.",
        "",
        "## Cobertura territorial",
        "",
        f"El departamento se filtra por `{territory['department_field']}` con el valor "
        f"exacto `{territory['department_value']}`. Los valores observados para el Valle "
        "de Aburrá son:",
        "",
        "| Municipio canónico | Valor observado | Registros |",
        "|---|---|---:|",
    ]
    for municipality in territory["valle_de_aburra"]:
        lines.append(
            f"| {municipality['canonical']} | `{municipality['observed']}` | "
            f"{municipality['row_count']:,} |"
        )
    lines.extend(
        [
            "",
            "La comparación territorial normaliza mayúsculas, tildes y espacios para "
            "detectar variantes; las consultas reproducibles conservan siempre los valores "
            "exactos publicados por la fuente.",
            "",
            "## Perfil de columnas",
            "",
            "Las métricas siguientes pertenecen a la muestra acotada. La cardinalidad no "
            "representa todo el dataset salvo cuando se indica una agregación específica.",
            "",
            "| Campo API | Etiqueta | Tipo declarado | Tipos observados | Nulos | Cardinalidad |",
            "|---|---|---|---|---:|---:|",
        ]
    )
    for column in contract["columns"]:
        observed = (
            ", ".join(f"{name}: {count}" for name, count in column["observed_types"].items())
            or "sin valor en muestra"
        )
        lines.append(
            f"| `{column['field']}` | {column['label']} | `{column['declared_type']}` | "
            f"{observed} | {column['null_count']} | {column['sample_cardinality']} |"
        )
    lines.extend(
        [
            "",
            "## Diferencias y limitaciones",
            "",
            "- El modelo analítico usa el concepto `source_updated_at`; la fuente real "
            "publica `ultima_actualizacion`. El contrato conserva el nombre real y deja el "
            "renombrado para una transformación posterior.",
            "- La fuente publica `ciudad`, mientras el modelo objetivo habla de municipios. "
            "Además, `Itagüí` aparece como `Itagui`; el mapeo queda explícito y no modifica "
            "el dato de origen.",
            f"- `{cursor['field']}` tiene {cursor['null_count']:,} nulos. Por tanto, una "
            "ingesta futura necesitará una estrategia adicional para esos registros.",
            "- La unicidad y los conteos son evidencia fechada, no garantías sobre cambios "
            "futuros de la fuente.",
            "",
            "## Reproducción y controles",
            "",
            "```bash",
            "make profile-source       # vuelve a observar y genera los tres artefactos",
            "make validate-source      # valida contrato y fixture sin red",
            "make check-source-live    # comprueba columnas críticas contra la fuente",
            "make test                 # ejecuta todas las pruebas offline",
            "```",
            "",
            "La comprobación en vivo está separada de CI. Un fallo de red nunca reemplaza "
            "el contrato, la fixture ni este reporte.",
            "",
        ]
    )
    return "\n".join(lines)


def write_artifacts(
    contract: dict[str, Any],
    fixture: list[dict[str, Any]],
    report: str,
    *,
    contract_path: Path,
    fixture_path: Path,
    report_path: Path,
) -> None:
    payloads = {
        contract_path: yaml.safe_dump(contract, allow_unicode=True, sort_keys=False),
        fixture_path: json.dumps(fixture, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        report_path: report,
    }
    temporary_paths: list[tuple[Path, Path]] = []
    try:
        for target, content in payloads.items():
            target.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                dir=target.parent, prefix=f".{target.name}.", text=True
            )
            temporary = Path(temporary_name)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(content)
            temporary_paths.append((temporary, target))
        for temporary, target in temporary_paths:
            temporary.replace(target)
    except Exception:
        for temporary, _target in temporary_paths:
            temporary.unlink(missing_ok=True)
        raise


def _soql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _compatible_types(declared: str) -> set[str]:
    return {
        "number": {"number"},
        "calendar_date": {"calendar_date"},
        "floating_timestamp": {"calendar_date"},
        "checkbox": {"boolean", "text"},
        "url": {"object", "text", "url"},
        "text": {"text"},
    }.get(declared, {declared})
