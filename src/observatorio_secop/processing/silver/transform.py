"""Declarative Spark transformation, diagnostics, and deterministic deduplication."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from observatorio_secop.processing.silver.config import (
    SilverConfig,
    TerritoryCatalog,
    normalize_text,
    territory_key,
)
from observatorio_secop.processing.silver.schemas import (
    REJECTED_SCHEMA,
    REJECTION_REASONS,
    SILVER_SCHEMA,
)


@dataclass(frozen=True)
class TransformationResult:
    valid: DataFrame
    rejected: DataFrame
    counts: dict[str, int | float]


def transform_bronze(
    bronze: DataFrame,
    config: SilverConfig,
    catalog: TerritoryCatalog,
    silver_version_id: str,
) -> TransformationResult:
    annotated = _annotate(bronze, config, catalog, silver_version_id).cache()
    input_rows = annotated.count()
    rejected = _rejected(annotated, silver_version_id).cache()
    rejected_rows = rejected.count()
    candidates = annotated.where(F.size("_rejection_details") == 0).cache()

    collision_groups_df = (
        candidates.groupBy("contract_key")
        .agg(
            F.countDistinct("source_contract_id").alias("raw_identifiers"),
            F.count("*").alias("rows"),
        )
        .where(F.col("raw_identifiers") > 1)
    )
    collision_groups = collision_groups_df.count()
    collision_rows_value = (
        collision_groups_df.agg(F.coalesce(F.sum("rows"), F.lit(0))).first()[0]
        if collision_groups
        else 0
    )

    order = _version_order()
    exact_window = Window.partitionBy("contract_key", "payload_sha256").orderBy(*order)
    exact_ranked = candidates.withColumn("_exact_rank", F.row_number().over(exact_window))
    exact_duplicates = exact_ranked.where(F.col("_exact_rank") > 1).count()
    distinct_payloads = exact_ranked.where(F.col("_exact_rank") == 1).drop("_exact_rank")

    version_window = Window.partitionBy("contract_key").orderBy(*order)
    version_ranked = distinct_payloads.withColumn(
        "_version_rank", F.row_number().over(version_window)
    )
    superseded_versions = version_ranked.where(F.col("_version_rank") > 1).count()
    selected = version_ranked.where(F.col("_version_rank") == 1).drop("_version_rank")
    valid = selected.select(*[field.name for field in SILVER_SCHEMA.fields]).cache()
    output_rows = valid.count()
    warning_rows = valid.where(F.size("quality_warning_codes") > 0).count()
    distinct_keys = valid.select("contract_key").distinct().count()
    duplicate_rows = exact_duplicates + superseded_versions
    rejection_rate = rejected_rows / input_rows if input_rows else 0.0
    counts: dict[str, int | float] = {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "rejected_rows": rejected_rows,
        "exact_duplicates": exact_duplicates,
        "superseded_versions": superseded_versions,
        "duplicate_rows": duplicate_rows,
        "warning_rows": warning_rows,
        "distinct_keys": distinct_keys,
        "collision_groups": collision_groups,
        "collision_rows": int(collision_rows_value),
        "rejection_rate": rejection_rate,
    }
    return TransformationResult(valid=valid, rejected=rejected, counts=counts)


def _annotate(
    bronze: DataFrame,
    config: SilverConfig,
    catalog: TerritoryCatalog,
    silver_version_id: str,
) -> DataFrame:
    normalize_text_udf = F.udf(_normalized_nfc, T.StringType())
    territory_key_udf = F.udf(territory_key, T.StringType())
    extract_url_udf = F.udf(_extract_url, T.StringType())
    catalog_map = _literal_map(catalog.canonical_by_key)

    result = (
        bronze.withColumn("silver_version_id", F.lit(silver_version_id))
        .withColumn("source_dataset_id", F.lit(config.dataset_id))
        .withColumn("source_contract_id", F.col("id_contrato"))
        .withColumn("_normalized_id", normalize_text_udf("id_contrato"))
        .withColumn(
            "contract_key",
            F.when(
                F.col("_normalized_id").isNotNull(),
                F.sha2(
                    F.concat_ws(
                        "\u001f",
                        F.lit(config.key_rule_version),
                        F.lit(config.dataset_id),
                        F.col("_normalized_id"),
                    ),
                    256,
                ),
            ),
        )
        .withColumn("contract_reference", normalize_text_udf("referencia_del_contrato"))
        .withColumn("procurement_process_id", normalize_text_udf("proceso_de_compra"))
        .withColumn("entity_name", normalize_text_udf("nombre_entidad"))
        .withColumn("department_raw", F.col("departamento"))
        .withColumn("department_key", territory_key_udf("departamento"))
        .withColumn(
            "department_name",
            F.when(
                F.col("department_key") == F.lit(territory_key(catalog.department)), "Antioquia"
            ),
        )
        .withColumn("municipality_raw", F.col("ciudad"))
        .withColumn("municipality_key", territory_key_udf("ciudad"))
        .withColumn("municipality_name", catalog_map[F.col("municipality_key")])
        .withColumn(
            "municipality_status",
            F.when(F.col("municipality_name").isNotNull(), F.lit("KNOWN")).otherwise(
                F.lit("UNKNOWN")
            ),
        )
        .withColumn(
            "is_medellin",
            F.coalesce(F.col("municipality_name") == F.lit("Medellín"), F.lit(False)),
        )
        .withColumn(
            "is_valle_de_aburra",
            F.coalesce(
                F.col("municipality_name").isin(sorted(catalog.valle_de_aburra)), F.lit(False)
            ),
        )
        .withColumn("contract_status", normalize_text_udf("estado_contrato"))
        .withColumn("main_category_code", normalize_text_udf("codigo_de_categoria_principal"))
        .withColumn("process_description", normalize_text_udf("descripcion_del_proceso"))
        .withColumn("contract_type", normalize_text_udf("tipo_de_contrato"))
        .withColumn("procurement_method", normalize_text_udf("modalidad_de_contratacion"))
        .withColumn("signing_date_raw", F.col("fecha_de_firma"))
        .withColumn("start_date_raw", F.col("fecha_de_inicio_del_contrato"))
        .withColumn("end_date_raw", F.col("fecha_de_fin_del_contrato"))
        .withColumn("contract_value_raw", F.col("valor_del_contrato"))
        .withColumn("source_updated_at_raw", F.col("ultima_actualizacion"))
        .withColumn("signing_date", _business_date("fecha_de_firma", config))
        .withColumn("start_date", _business_date("fecha_de_inicio_del_contrato", config))
        .withColumn("end_date", _business_date("fecha_de_fin_del_contrato", config))
        .withColumn(
            "source_updated_at", _timestamp("ultima_actualizacion", config.source_datetime_format)
        )
        .withColumn("contract_value_cop", _amount_value("valor_del_contrato"))
        .withColumn("source_url", extract_url_udf("urlproceso"))
        .withColumn("payload_sha256", F.sha2("projected_payload_json", 256))
        .withColumn(
            "bronze_fetched_at_utc",
            F.try_to_timestamp("bronze_fetched_at_utc", F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX")),
        )
        .withColumn(
            "signing_year",
            F.when(F.col("signing_date").isNull(), F.lit("unknown")).otherwise(
                F.date_format("signing_date", "yyyy")
            ),
        )
    )
    warnings = F.array_compact(
        F.array(F.when(F.col("municipality_status") == "UNKNOWN", F.lit("UNKNOWN_MUNICIPALITY")))
    )
    result = result.withColumn("quality_warning_codes", F.array_sort(F.array_distinct(warnings)))
    return result.withColumn("_rejection_details", _rejection_details(config))


def _rejection_details(config: SilverConfig) -> Column:
    amount = F.col("valor_del_contrato")
    amount_trim = F.trim(amount)
    accepted_amount = amount_trim.rlike(r"^\+?\d+(\.\d{1,2}0*)?$")
    details = [
        _detail(
            "IDENTIFIER_NULL",
            "id_contrato",
            F.col("_normalized_id").isNull(),
            F.col("id_contrato"),
        ),
        _detail(
            "AMOUNT_NULL",
            "valor_del_contrato",
            amount.isNull() | (amount_trim == ""),
            amount,
        ),
        _detail(
            "AMOUNT_NEGATIVE",
            "valor_del_contrato",
            amount_trim.rlike(r"^-\d+(\.\d+)?$"),
            amount,
        ),
        _detail(
            "AMOUNT_SCALE_EXCEEDED",
            "valor_del_contrato",
            amount_trim.rlike(r"^\+?\d+\.\d{2}\d*[1-9]\d*$"),
            amount,
        ),
        _detail(
            "AMOUNT_INVALID_FORMAT",
            "valor_del_contrato",
            amount.isNotNull()
            & (amount_trim != "")
            & ~amount_trim.rlike(r"^-\d+(\.\d+)?$")
            & ~amount_trim.rlike(r"^\+?\d+\.\d{2}\d*[1-9]\d*$")
            & ~accepted_amount,
            amount,
        ),
        _detail(
            "AMOUNT_OVERFLOW",
            "valor_del_contrato",
            accepted_amount & F.col("contract_value_cop").isNull(),
            amount,
        ),
    ]
    for source, parsed in (
        ("fecha_de_firma", "signing_date"),
        ("fecha_de_inicio_del_contrato", "start_date"),
        ("fecha_de_fin_del_contrato", "end_date"),
        ("ultima_actualizacion", "source_updated_at"),
    ):
        raw = F.col(source)
        details.append(
            _detail(
                "DATE_INVALID",
                source,
                raw.isNotNull() & (F.trim(raw) != "") & F.col(parsed).isNull(),
                raw,
            )
        )
    details.extend(
        [
            _detail(
                "DATE_INCONSISTENT",
                "fecha_de_inicio_del_contrato",
                F.col("start_date").isNotNull()
                & F.col("end_date").isNotNull()
                & (F.col("start_date") > F.col("end_date")),
                F.col("fecha_de_inicio_del_contrato"),
            ),
            _detail(
                "DATE_INCONSISTENT",
                "fecha_de_firma",
                F.col("signing_date").isNotNull()
                & F.col("end_date").isNotNull()
                & (F.col("signing_date") > F.col("end_date")),
                F.col("fecha_de_firma"),
            ),
            _detail(
                "DEPARTMENT_OUT_OF_SCOPE",
                "departamento",
                F.col("department_name").isNull(),
                F.col("departamento"),
            ),
        ]
    )
    return F.array_sort(F.array_compact(F.array(*details)))


def _rejected(annotated: DataFrame, silver_version_id: str) -> DataFrame:
    return (
        annotated.where(F.size("_rejection_details") > 0)
        .withColumn(
            "rejection_codes",
            F.array_sort(
                F.array_distinct(F.transform("_rejection_details", lambda item: item["code"]))
            ),
        )
        .select(
            F.lit(silver_version_id).alias("silver_version_id"),
            "bronze_run_id",
            "bronze_page_path",
            "bronze_lane",
            "bronze_page_sequence",
            "bronze_row_ordinal",
            "bronze_fetched_at_utc",
            "source_contract_id",
            "projected_payload_json",
            "rejection_codes",
            F.col("_rejection_details").alias("rejection_details"),
        )
        .select(*[field.name for field in REJECTED_SCHEMA.fields])
    )


def _business_date(field: str, config: SilverConfig) -> Column:
    value = F.col(field)
    parsed = _timestamp(field, config.source_datetime_format)
    exact_midnight = value.rlike(r"^\d{4}-\d{2}-\d{2}T00:00:00\.000$")
    return F.when(value.isNull() | (F.trim(value) == ""), F.lit(None).cast("date")).when(
        exact_midnight & parsed.isNotNull(), F.to_date(parsed)
    )


def _timestamp(field: str, source_format: str) -> Column:
    value = F.col(field)
    exact = value.rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$")
    parsed = F.try_to_timestamp(value, F.lit(source_format))
    return F.when(value.isNull() | (F.trim(value) == ""), F.lit(None).cast("timestamp")).when(
        exact, parsed
    )


def _amount_value(field: str) -> Column:
    value = F.trim(F.col(field))
    accepted = value.rlike(r"^\+?\d+(\.\d{1,2}0*)?$")
    return F.when(accepted, value.cast(T.DecimalType(20, 2)))


def _detail(code: str, field: str, condition: Column, raw: Column) -> Column:
    return F.when(
        condition,
        F.struct(
            F.lit(code).alias("code"),
            F.lit(field).alias("field"),
            F.lit(REJECTION_REASONS[code]).alias("reason"),
            F.substring(raw.cast("string"), 1, 256).alias("raw_value"),
        ),
    )


def _version_order() -> list[Column]:
    return [
        F.col("source_updated_at").isNotNull().desc(),
        F.col("source_updated_at").desc_nulls_last(),
        F.col("bronze_fetched_at_utc").desc(),
        F.col("bronze_run_id").desc(),
        F.col("bronze_page_path").desc(),
        F.col("bronze_row_ordinal").desc(),
        F.col("payload_sha256").desc(),
    ]


def _literal_map(values: dict[str, str]) -> Column:
    entries = []
    for key, value in sorted(values.items()):
        entries.extend((F.lit(key), F.lit(value)))
    return F.create_map(*entries)


def _normalized_nfc(value: str | None) -> str | None:
    normalized = normalize_text(value)
    return unicodedata.normalize("NFC", normalized) if normalized else None


def _extract_url(value: str | None) -> str | None:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    try:
        parsed: Any = json.loads(normalized)
    except json.JSONDecodeError:
        return normalized
    if isinstance(parsed, dict):
        for key in ("url", "uri"):
            candidate = parsed.get(key)
            if isinstance(candidate, str):
                return normalize_text(candidate)
        return None
    return normalized if isinstance(parsed, str) else None
