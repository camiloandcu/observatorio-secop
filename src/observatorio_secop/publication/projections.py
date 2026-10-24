"""Explicit safe views over sealed tables; source grants never reach readers."""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg import sql

from observatorio_secop.population import PopulationScope

from .models import OUTPUT_RELATIONS, OWNER_ROLE, namespaces
from .models import publication_id as canonical_id

IDENTITY = ("silver_version_id", "silver_manifest_sha256", "scope_id")
DIGESTS = ("catalog_sha256", "policy_sha256")
QUANTILES = ("value_p25_cop", "value_median_cop", "value_p75_cop", "distribution_state")
PUBLIC_COLUMNS: dict[str, tuple[str, ...]] = {
    "fact_contract": (
        "contract_sk",
        "contract_key",
        "contract_id",
        "process_id",
        "entity_identity_status",
        "entity_identity_reason",
        "supplier_identity_status",
        "supplier_identity_reason",
        "identity_rule_version",
        "source_contract_sha256",
        "source_projection_version",
        "bronze_run_id",
        "bronze_row_ordinal",
        "entity_sk",
        "supplier_sk",
        "municipality_sk",
        "modality_sk",
        "signing_date_sk",
        "start_date_sk",
        "end_date_sk",
        "signing_date",
        "start_date",
        "end_date",
        "contract_value_cop",
        "duration_days",
        "status",
        "source_url",
        "source_updated_at",
        "bronze_fetched_at_utc",
        "silver_version_id",
        "silver_manifest_sha256",
        "payload_sha256",
        "loaded_at",
    ),
    "dim_entity": (
        "entity_sk",
        "entity_natural_key",
        "source_dataset_id",
        "entity_code",
        "entity_name",
        "member_status",
    ),
    "dim_supplier": (
        "supplier_sk",
        "supplier_natural_key",
        "source_dataset_id",
        "supplier_code",
        "supplier_name",
        "member_status",
    ),
    "dim_municipality": (
        "municipality_sk",
        "municipality_natural_key",
        "municipality_name",
        "member_status",
    ),
    "dim_modality": ("modality_sk", "modality_natural_key", "modality_name", "member_status"),
    "dim_date": (
        "date_sk",
        "calendar_date",
        "year",
        "quarter",
        "month",
        "month_name",
        "day_of_month",
        "day_of_week",
        "day_name",
        "member_status",
    ),
    "mart_contract_detail": (
        "contract_sk",
        "contract_key",
        "contract_id",
        "contract_reference",
        "process_id",
        "entity_sk",
        "entity_name",
        "entity_code",
        "entity_identity_status",
        "entity_identity_reason",
        "supplier_sk",
        "supplier_name",
        "supplier_code",
        "supplier_identity_status",
        "supplier_identity_reason",
        "municipality_sk",
        "modality_sk",
        "signing_date_sk",
        "start_date_sk",
        "end_date_sk",
        "signing_date",
        "start_date",
        "end_date",
        "signing_month",
        "contract_value_cop",
        "duration_days",
        "status_raw",
        "status_bucket",
        "status_profile_state",
        "base_cohort",
        "source_process_url",
        "source_link_state",
        "source_updated_at",
        "acquisition_observed_at_utc",
        "pipeline_completed_at_utc",
        "bronze_run_id",
        "bronze_row_ordinal",
        "payload_sha256",
        "condition_state",
        "observation_condition",
        "attributable_rejection_evidence",
        "quality_warning_codes",
        "quality_evidence_sha256",
        *IDENTITY,
        "coverage_state",
        "coverage_known",
        "coverage_metadata",
        *DIGESTS,
        "policy_version",
        "candidate_built_at_utc",
        "temporal_basis",
        "value_unit",
        "source_time_basis",
    ),
    "mart_contract_overview": (
        *IDENTITY,
        "signing_month",
        "municipality_sk",
        "modality_sk",
        "status_bucket",
        *DIGESTS,
        "contract_count",
        "contract_value_cop",
        "missing_value_count",
        "missing_signing_date_count",
        *QUANTILES,
        "selection_metadata",
        "denominator_basis",
        "contract_count_share",
        "contract_value_share",
        "count_share_state",
        "value_share_state",
    ),
    "mart_entity_monthly": (
        *IDENTITY,
        "entity_sk",
        "signing_month",
        *DIGESTS,
        "contract_count",
        "contract_value_cop",
        "missing_value_count",
        "status_distribution",
        *QUANTILES,
    ),
    "mart_supplier_concentration": (
        *IDENTITY,
        "entity_sk",
        "signing_month",
        *DIGESTS,
        "total_contract_count",
        "known_contract_count",
        "total_value_cop",
        "known_value_cop",
        "count_identity_coverage",
        "value_identity_coverage",
        "selected_supplier_reference_share",
        "selected_contract_count",
        "hhi_state",
        "selected_supplier_reference_share_state",
        "hhi",
        "count_coverage_state",
        "value_coverage_state",
        "base_cohort",
        "hhi_denominator",
        "selection_metadata",
        "reference_metadata",
    ),
    "mart_data_quality": (
        *IDENTITY,
        "control_id",
        "numerator",
        "denominator",
        "evaluation_grain",
        "filter_applicability",
        "control_state",
        "reason",
        "pipeline_completed_at_utc",
        "evidence_sha256",
        *DIGESTS,
    ),
}

SUMMARY_FIELDS = (
    "schema_version",
    "scope_id",
    "silver_version_id",
    "contract_key",
    "state",
    "winner_payload_sha256",
    "winner_run_id",
    "winner_page_sha256",
    "winner_row_ordinal",
    "winner_observed_at_utc",
    "winner_profile_sha256",
    "winner_projection_version",
    "validation_version",
    "later_invalid_count",
    "equal_invalid_count",
)
LINK_FIELDS = (
    "schema_version",
    "scope_id",
    "silver_version_id",
    "contract_key",
    "link_id",
    "temporal_relation",
    "winner_payload_sha256",
    "winner_run_id",
    "winner_page_sha256",
    "winner_row_ordinal",
    "winner_observed_at_utc",
    "winner_profile_sha256",
    "winner_projection_version",
    "rejected_payload_sha256",
    "rejected_run_id",
    "rejected_page_sha256",
    "rejected_row_ordinal",
    "rejected_observed_at_utc",
    "rejected_profile_sha256",
    "rejected_projection_version",
    "validation_version",
    "rejection_codes",
)
SELECTION_FIELDS = (
    "entity_keys",
    "municipality_keys",
    "modality_keys",
    "statuses",
    "exclude_statuses",
    "start_date",
    "end_date",
    "missing_signatures",
    "selected_supplier",
)
ACQUISITION_FIELDS = (
    "schema_version",
    "campaign_id",
    "plan_id",
    "scope_id",
    "state",
    "limits",
    "consumed",
    "scope_scan_complete",
    "baseline_check_complete",
    "unconfirmed_ids",
    "observed_from_utc",
    "observed_to_utc",
    "truncated",
    "limitations",
)


def _json_object(expression: sql.Composable, fields: tuple[str, ...]) -> sql.Composable:
    """Whitelist keys and preserve explicit null/absent legacy objects."""
    return sql.SQL(
        "(SELECT jsonb_object_agg(j.key,j.value) FROM jsonb_each("
        "CASE WHEN jsonb_typeof({})='object' THEN {} ELSE '{{}}'::jsonb END) j "
        "WHERE j.key=ANY({}::text[]))"
    ).format(expression, expression, sql.Literal(list(fields)))


def _json_array(expression: sql.Composable, fields: tuple[str, ...]) -> sql.Composable:
    value = _json_object(sql.SQL("a.value"), fields)
    return sql.SQL(
        "CASE WHEN jsonb_typeof({})='array' THEN "
        "(SELECT coalesce(jsonb_agg({} ORDER BY a.ordinality),'[]'::jsonb) "
        "FROM jsonb_array_elements(CASE WHEN jsonb_typeof({})='array' THEN {} "
        "ELSE '[]'::jsonb END) WITH ORDINALITY a(value,ordinality)) END"
    ).format(expression, value, expression, expression)


def _coverage(expression: sql.Composable) -> sql.Composable:
    fields = ("state", "basis", "coverage_known", "scope_id", "limitations")
    base = _json_object(expression, fields)
    acquisition = _json_object(
        sql.SQL("a.value"),
        tuple(key for key in ACQUISITION_FIELDS if key not in ("limits", "consumed")),
    )
    baseline = _json_object(
        sql.SQL("a.value->'baseline'"), ("silver_version_id", "manifest_sha256", "keys_sha256")
    )
    limits = _json_object(sql.SQL("a.value->'limits'"), ("rows", "attempts"))
    consumed = _json_object(sql.SQL("a.value->'consumed'"), ("rows", "attempts"))
    acquisition = sql.SQL(
        "coalesce({},'{{}}'::jsonb)||jsonb_build_object('baseline',{},'limits',{},'consumed',{})"
    ).format(acquisition, baseline, limits, consumed)
    acquisitions = sql.SQL(
        "CASE WHEN jsonb_typeof({}->'acquisitions')='array' THEN "
        "(SELECT coalesce(jsonb_agg({} ORDER BY a.ordinality),'[]'::jsonb) "
        "FROM jsonb_array_elements(CASE WHEN jsonb_typeof({}->'acquisitions')='array' "
        "THEN {}->'acquisitions' ELSE '[]'::jsonb END) "
        "WITH ORDINALITY a(value,ordinality)) END"
    ).format(expression, acquisition, expression, expression)
    return sql.SQL("coalesce({},'{{}}'::jsonb)||jsonb_build_object('acquisitions',{})").format(
        base, acquisitions
    )


def projection_expression(column: str) -> sql.Composable:
    expression: sql.Composable = sql.Identifier("data", column)
    if column in ("source_url", "source_process_url"):
        expression = sql.SQL(
            "CASE WHEN {} ~ '^https://(www[.])?community[.]secop[.]gov[.]co(/|$)' THEN {} END"
        ).format(expression, expression)
    elif column == "observation_condition":
        expression = _json_object(expression, SUMMARY_FIELDS)
    elif column == "attributable_rejection_evidence":
        expression = _json_array(expression, LINK_FIELDS)
    elif column == "coverage_metadata":
        expression = _coverage(expression)
    elif column in ("selection_metadata", "reference_metadata"):
        fields = tuple(
            key
            for key in SELECTION_FIELDS
            if column != "reference_metadata" or key != "selected_supplier"
        )
        expression = _json_object(expression, fields)
    return sql.SQL("{} AS {}").format(expression, sql.Identifier(column))


def safe_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Project a fixed metadata dialect, excluding paths, receipts and secrets."""
    fields = (
        "silver_version_id",
        "silver_manifest_sha256",
        "scope_id",
        "source_dataset_id",
        "coverage_state",
        "coverage_known",
        "catalog_sha256",
        "dbt_graph_sha256",
        "policy_sha256",
        "policy_version",
        "identity_rule_version",
        "source_schema_version",
        "code_revision",
        "built_at",
    )
    result = {name: metadata.get(name) for name in fields}
    result["observed_at_utc"] = metadata.get("observed_at", metadata.get("observed_at_utc"))
    result["pipeline_completed_at_utc"] = metadata.get(
        "pipeline_completed_at", metadata.get("pipeline_completed_at_utc")
    )
    scope = metadata.get("scope")
    result["scope"] = PopulationScope.from_dict(scope).as_dict() if scope is not None else None
    result["scoring_state"] = "ABSENT"
    result["metadata_schema_version"] = "analytical-publication-metadata-v1"
    return result


def create_views(
    cursor: psycopg.Cursor[Any],
    publication_id: str,
    data_schema: str,
    view_schema: str,
    metadata: dict[str, Any],
) -> None:
    """Create owner-rights projections with no candidate or consumer grants."""
    publication_id = canonical_id(publication_id)
    expected = namespaces(publication_id)
    if data_schema != expected["data"] or view_schema != expected["read"]:
        raise ValueError("PUBLICATION_PROJECTION_NAMESPACE")
    if set(PUBLIC_COLUMNS) != set(OUTPUT_RELATIONS):
        raise ValueError("PUBLICATION_PROJECTION_INVENTORY")
    for schema in (data_schema, view_schema):
        cursor.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC,secop_publication_builder").format(
                sql.Identifier(schema)
            )
        )
    for relation in OUTPUT_RELATIONS:
        selections: list[sql.Composable] = [
            sql.SQL("{}::text AS publication_id").format(sql.Literal(publication_id))
        ]
        selections.extend(projection_expression(name) for name in PUBLIC_COLUMNS[relation])
        cursor.execute(
            sql.SQL("CREATE VIEW {}.{} AS SELECT {} FROM {}.{} data").format(
                sql.Identifier(view_schema),
                sql.Identifier(relation),
                sql.SQL(",").join(selections),
                sql.Identifier(data_schema),
                sql.Identifier(relation),
            )
        )
        _secure_view(cursor, view_schema, relation)
    value = safe_metadata(metadata)
    types = {
        "metadata_schema_version": "text",
        "silver_version_id": "text",
        "silver_manifest_sha256": "text",
        "scope_id": "text",
        "scope": "jsonb",
        "built_at": "timestamptz",
        "observed_at_utc": "timestamptz",
        "pipeline_completed_at_utc": "timestamptz",
        "coverage_state": "text",
        "coverage_known": "boolean",
        "source_dataset_id": "text",
        "catalog_sha256": "text",
        "dbt_graph_sha256": "text",
        "policy_sha256": "text",
        "policy_version": "text",
        "identity_rule_version": "text",
        "source_schema_version": "integer",
        "code_revision": "text",
        "scoring_state": "text",
    }
    selections = [
        sql.SQL("{}::text AS publication_id").format(sql.Literal(publication_id)),
        sql.SQL("{}::text AS read_namespace").format(sql.Literal(view_schema)),
    ]
    for name, kind in types.items():
        operator = "->" if kind == "jsonb" else "->>"
        selections.append(
            sql.SQL("(value {} {})::{} AS {}").format(
                sql.SQL(operator), sql.Literal(name), sql.SQL(kind), sql.Identifier(name)
            )
        )
    cursor.execute(
        sql.SQL(
            "CREATE VIEW {}.publication_metadata AS SELECT {} "
            "FROM (VALUES ({}::jsonb)) metadata(value)"
        ).format(
            sql.Identifier(view_schema),
            sql.SQL(",").join(selections),
            sql.Literal(json.dumps(value, ensure_ascii=False)),
        )
    )
    _secure_view(cursor, view_schema, "publication_metadata")


def _secure_view(cursor: psycopg.Cursor[Any], schema: str, relation: str) -> None:
    view = sql.Identifier(schema, relation)
    cursor.execute(sql.SQL("ALTER VIEW {} OWNER TO {}").format(view, sql.Identifier(OWNER_ROLE)))
    cursor.execute(sql.SQL("REVOKE ALL ON {} FROM PUBLIC,secop_publication_builder").format(view))
