"""Pandera-backed Silver quality checks and deterministic evidence."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

import pandera.pyspark as pa
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from observatorio_secop.processing.silver.config import SilverConfig, TerritoryCatalog
from observatorio_secop.processing.silver.models import QualityResult
from observatorio_secop.processing.silver.schemas import METRICS_SCHEMA, SILVER_SCHEMA


def validate_quality(
    spark: SparkSession,
    valid: DataFrame,
    counts: dict[str, int | float],
    config: SilverConfig,
    catalog: TerritoryCatalog,
) -> list[QualityResult]:
    silver_schema = _pandera_silver_schema(catalog)
    validated = silver_schema.validate(valid)
    pandera_errors = _plain_errors(validated.pandera.errors)
    silver_column_order = valid.columns == [field.name for field in SILVER_SCHEMA.fields]

    metrics = spark.createDataFrame(
        [tuple(counts[field.name] for field in METRICS_SCHEMA.fields)], METRICS_SCHEMA
    )
    metrics_schema = _pandera_metrics_schema(config)
    validated_metrics = metrics_schema.validate(metrics)
    metrics_errors = _plain_errors(validated_metrics.pandera.errors)
    metrics_column_order = metrics.columns == [field.name for field in METRICS_SCHEMA.fields]

    critical_nulls = sum(
        valid.where(F.col(field.name).isNull()).count()
        for field in SILVER_SCHEMA.fields
        if not field.nullable
    )
    duplicates = valid.groupBy("contract_key").count().where(F.col("count") > 1).count()
    invalid_amounts = valid.where(
        F.col("contract_value_cop").isNull() | (F.col("contract_value_cop") < 0)
    ).count()
    invalid_dates = valid.where(
        (F.col("end_date").isNotNull())
        & (
            ((F.col("start_date").isNotNull()) & (F.col("start_date") > F.col("end_date")))
            | ((F.col("signing_date").isNotNull()) & (F.col("signing_date") > F.col("end_date")))
        )
    ).count()
    invalid_territory = valid.where(
        (F.col("department_name") != "Antioquia")
        | (F.col("department_key") != "ANTIOQUIA")
        | (F.col("is_medellin") != (F.col("municipality_name") == "Medellín"))
        | (
            F.col("is_valle_de_aburra")
            != F.coalesce(
                F.col("municipality_name").isin(sorted(catalog.valle_de_aburra)), F.lit(False)
            )
        )
        | (
            F.col("signing_year")
            != F.when(F.col("signing_date").isNull(), "unknown").otherwise(
                F.date_format("signing_date", "yyyy")
            )
        )
    ).count()
    reconciled = counts["input_rows"] == (
        counts["output_rows"]
        + counts["rejected_rows"]
        + counts["exact_duplicates"]
        + counts["superseded_versions"]
    )
    non_empty_passed = config.allow_empty_output or counts["output_rows"] > 0
    results = [
        QualityResult(
            "PANDERA_SILVER_SCHEMA",
            "critical",
            not pandera_errors,
            pandera_errors or "valid",
            "strict Silver schema and dataframe checks",
        ),
        QualityResult(
            "PANDERA_METRICS_SCHEMA",
            "critical",
            not metrics_errors,
            metrics_errors or "valid",
            "typed metrics and aggregate checks",
        ),
        QualityResult(
            "SILVER_COLUMN_ORDER",
            "critical",
            silver_column_order,
            valid.columns,
            [field.name for field in SILVER_SCHEMA.fields],
        ),
        QualityResult(
            "METRICS_COLUMN_ORDER",
            "critical",
            metrics_column_order,
            metrics.columns,
            [field.name for field in METRICS_SCHEMA.fields],
        ),
        QualityResult(
            "CRITICAL_NULLS",
            "critical",
            critical_nulls == 0,
            critical_nulls,
            0,
            critical_nulls,
        ),
        QualityResult(
            "UNIQUE_CONTRACT_KEY", "critical", duplicates == 0, duplicates, 0, duplicates
        ),
        QualityResult(
            "AMOUNT_VALID", "critical", invalid_amounts == 0, invalid_amounts, 0, invalid_amounts
        ),
        QualityResult(
            "DATE_COHERENCE", "critical", invalid_dates == 0, invalid_dates, 0, invalid_dates
        ),
        QualityResult(
            "TERRITORY_CONSISTENCY",
            "critical",
            invalid_territory == 0,
            invalid_territory,
            0,
            invalid_territory,
        ),
        QualityResult(
            "KEY_COLLISION",
            "critical",
            counts["collision_groups"] == 0,
            {
                "groups": counts["collision_groups"],
                "rows": counts["collision_rows"],
            },
            {"groups": 0, "rows": 0},
            int(counts["collision_rows"]),
        ),
        QualityResult(
            "COUNT_RECONCILIATION",
            "critical",
            reconciled,
            counts["input_rows"],
            counts["output_rows"]
            + counts["rejected_rows"]
            + counts["exact_duplicates"]
            + counts["superseded_versions"],
        ),
        QualityResult(
            "REJECTION_RATE",
            "critical",
            counts["rejection_rate"] <= config.max_rejection_rate,
            {
                "rejected_rows": counts["rejected_rows"],
                "input_rows": counts["input_rows"],
                "rate": counts["rejection_rate"],
            },
            {"maximum": config.max_rejection_rate},
            int(counts["rejected_rows"]),
        ),
        QualityResult(
            "NON_EMPTY_OUTPUT",
            "critical",
            non_empty_passed,
            counts["output_rows"],
            "> 0" if not config.allow_empty_output else ">= 0",
        ),
        QualityResult(
            "UNKNOWN_MUNICIPALITY",
            "warning",
            counts["warning_rows"] == 0,
            counts["warning_rows"],
            0,
            int(counts["warning_rows"]),
        ),
    ]
    return sorted(results, key=lambda item: item.check_id)


def _pandera_silver_schema(catalog: TerritoryCatalog) -> pa.DataFrameSchema:
    columns: dict[str, pa.Column] = {}
    for field in SILVER_SCHEMA.fields:
        checks = None
        if field.name == "contract_value_cop":
            checks = pa.Check.greater_than_or_equal_to(Decimal("0.00"))
        columns[field.name] = pa.Column(field.dataType, checks=checks, nullable=field.nullable)
    return pa.DataFrameSchema(
        columns,
        checks=[
            pa.Check(
                lambda frame: frame.where(
                    (F.col("end_date").isNotNull())
                    & (
                        (F.col("start_date") > F.col("end_date"))
                        | (F.col("signing_date") > F.col("end_date"))
                    )
                )
                .limit(1)
                .count()
                == 0,
                name="coherent_dates",
            ),
            pa.Check(
                lambda frame: frame.where(
                    (F.col("department_name") != "Antioquia")
                    | (F.col("department_key") != "ANTIOQUIA")
                )
                .limit(1)
                .count()
                == 0,
                name="antioquia_scope",
            ),
            pa.Check(
                lambda frame: frame.where(
                    F.col("is_valle_de_aburra")
                    != F.coalesce(
                        F.col("municipality_name").isin(sorted(catalog.valle_de_aburra)),
                        F.lit(False),
                    )
                )
                .limit(1)
                .count()
                == 0,
                name="valle_de_aburra_flag",
            ),
        ],
        strict=True,
        ordered=False,
        unique=["contract_key"],
        coerce=False,
    )


def _pandera_metrics_schema(config: SilverConfig) -> pa.DataFrameSchema:
    columns = {field.name: pa.Column(field.dataType, nullable=False) for field in METRICS_SCHEMA}
    return pa.DataFrameSchema(
        columns,
        checks=[
            pa.Check(
                lambda frame: frame.where(
                    F.col("input_rows")
                    != F.col("output_rows")
                    + F.col("rejected_rows")
                    + F.col("exact_duplicates")
                    + F.col("superseded_versions")
                )
                .limit(1)
                .count()
                == 0,
                name="count_reconciliation",
            ),
            pa.Check(
                lambda frame: frame.where(F.col("collision_groups") != 0).limit(1).count() == 0,
                name="no_key_collisions",
            ),
            pa.Check(
                lambda frame: frame.where(
                    F.col("rejection_rate") > F.lit(config.max_rejection_rate)
                )
                .limit(1)
                .count()
                == 0,
                name="rejection_rate",
            ),
        ],
        strict=True,
        ordered=False,
        coerce=False,
    )


def _plain_errors(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    normalized = _normalize(value)
    return json.loads(json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str))


def _normalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize(item) for key, item in sorted(value.items())}
    if isinstance(value, list | tuple):
        return [_normalize(item) for item in value]
    return value
