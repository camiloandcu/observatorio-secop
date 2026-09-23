"""Explicit Bronze, Silver, quarantine, and metrics schemas."""

from __future__ import annotations

from pyspark.sql import types as T

BRONZE_FIELDS = (
    "id_contrato",
    "referencia_del_contrato",
    "proceso_de_compra",
    "nombre_entidad",
    "departamento",
    "ciudad",
    "estado_contrato",
    "codigo_de_categoria_principal",
    "descripcion_del_proceso",
    "tipo_de_contrato",
    "modalidad_de_contratacion",
    "fecha_de_firma",
    "fecha_de_inicio_del_contrato",
    "fecha_de_fin_del_contrato",
    "valor_del_contrato",
    "ultima_actualizacion",
    "urlproceso",
)

BRONZE_SCHEMA = T.StructType(
    [T.StructField(field, T.StringType(), True) for field in BRONZE_FIELDS]
    + [
        T.StructField("bronze_run_id", T.StringType(), False),
        T.StructField("bronze_page_path", T.StringType(), False),
        T.StructField("bronze_lane", T.StringType(), False),
        T.StructField("bronze_page_sequence", T.IntegerType(), False),
        T.StructField("bronze_row_ordinal", T.LongType(), False),
        T.StructField("bronze_fetched_at_utc", T.StringType(), False),
        T.StructField("projected_payload_json", T.StringType(), False),
    ]
)

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("silver_version_id", T.StringType(), False),
        T.StructField("contract_key", T.StringType(), False),
        T.StructField("source_dataset_id", T.StringType(), False),
        T.StructField("source_contract_id", T.StringType(), False),
        T.StructField("contract_reference", T.StringType(), True),
        T.StructField("procurement_process_id", T.StringType(), True),
        T.StructField("entity_name", T.StringType(), True),
        T.StructField("department_raw", T.StringType(), False),
        T.StructField("department_name", T.StringType(), False),
        T.StructField("department_key", T.StringType(), False),
        T.StructField("municipality_raw", T.StringType(), True),
        T.StructField("municipality_name", T.StringType(), True),
        T.StructField("municipality_key", T.StringType(), True),
        T.StructField("municipality_status", T.StringType(), False),
        T.StructField("is_medellin", T.BooleanType(), False),
        T.StructField("is_valle_de_aburra", T.BooleanType(), False),
        T.StructField("contract_status", T.StringType(), True),
        T.StructField("main_category_code", T.StringType(), True),
        T.StructField("process_description", T.StringType(), True),
        T.StructField("contract_type", T.StringType(), True),
        T.StructField("procurement_method", T.StringType(), True),
        T.StructField("signing_date_raw", T.StringType(), True),
        T.StructField("signing_date", T.DateType(), True),
        T.StructField("start_date_raw", T.StringType(), True),
        T.StructField("start_date", T.DateType(), True),
        T.StructField("end_date_raw", T.StringType(), True),
        T.StructField("end_date", T.DateType(), True),
        T.StructField("contract_value_raw", T.StringType(), False),
        T.StructField("contract_value_cop", T.DecimalType(20, 2), False),
        T.StructField("source_updated_at_raw", T.StringType(), True),
        T.StructField("source_updated_at", T.TimestampType(), True),
        T.StructField("source_url", T.StringType(), True),
        T.StructField("quality_warning_codes", T.ArrayType(T.StringType(), True), False),
        T.StructField("payload_sha256", T.StringType(), False),
        T.StructField("bronze_run_id", T.StringType(), False),
        T.StructField("bronze_page_path", T.StringType(), False),
        T.StructField("bronze_lane", T.StringType(), False),
        T.StructField("bronze_page_sequence", T.IntegerType(), False),
        T.StructField("bronze_row_ordinal", T.LongType(), False),
        T.StructField("bronze_fetched_at_utc", T.TimestampType(), False),
        T.StructField("signing_year", T.StringType(), False),
    ]
)

REJECTION_DETAIL = T.StructType(
    [
        T.StructField("code", T.StringType(), False),
        T.StructField("field", T.StringType(), False),
        T.StructField("reason", T.StringType(), False),
        T.StructField("raw_value", T.StringType(), True),
    ]
)

REJECTED_SCHEMA = T.StructType(
    [
        T.StructField("silver_version_id", T.StringType(), False),
        T.StructField("bronze_run_id", T.StringType(), False),
        T.StructField("bronze_page_path", T.StringType(), False),
        T.StructField("bronze_lane", T.StringType(), False),
        T.StructField("bronze_page_sequence", T.IntegerType(), False),
        T.StructField("bronze_row_ordinal", T.LongType(), False),
        T.StructField("bronze_fetched_at_utc", T.TimestampType(), False),
        T.StructField("source_contract_id", T.StringType(), True),
        T.StructField("projected_payload_json", T.StringType(), False),
        T.StructField("rejection_codes", T.ArrayType(T.StringType(), False), False),
        T.StructField("rejection_details", T.ArrayType(REJECTION_DETAIL, False), False),
    ]
)

METRICS_SCHEMA = T.StructType(
    [
        T.StructField("input_rows", T.LongType(), False),
        T.StructField("output_rows", T.LongType(), False),
        T.StructField("rejected_rows", T.LongType(), False),
        T.StructField("exact_duplicates", T.LongType(), False),
        T.StructField("superseded_versions", T.LongType(), False),
        T.StructField("duplicate_rows", T.LongType(), False),
        T.StructField("warning_rows", T.LongType(), False),
        T.StructField("distinct_keys", T.LongType(), False),
        T.StructField("collision_groups", T.LongType(), False),
        T.StructField("collision_rows", T.LongType(), False),
        T.StructField("rejection_rate", T.DoubleType(), False),
    ]
)

REJECTION_REASONS = {
    "IDENTIFIER_NULL": "id_contrato is null, empty, or whitespace",
    "AMOUNT_NULL": "valor_del_contrato is null or empty",
    "AMOUNT_NEGATIVE": "valor_del_contrato is negative",
    "AMOUNT_INVALID_FORMAT": "valor_del_contrato does not use the accepted decimal format",
    "AMOUNT_SCALE_EXCEEDED": "valor_del_contrato has more than two decimal places",
    "AMOUNT_OVERFLOW": "valor_del_contrato exceeds DecimalType(20,2)",
    "DATE_INVALID": "date value does not match the exact source format and semantics",
    "DATE_INCONSISTENT": "contract signing or start date occurs after end date",
    "DEPARTMENT_OUT_OF_SCOPE": "departamento does not normalize to Antioquia",
}

WARNING_REASONS = {
    "UNKNOWN_MUNICIPALITY": "ciudad is not present in the versioned Antioquia catalog",
}
