"""Versioned floating-clock business dates preserve the source calendar day."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest
import yaml
from conftest import ROOT, row, source_contract_hash, write_bronze_run
from pyspark.sql import functions as F

from observatorio_secop.processing.silver.bronze import read_bronze_dataframe, resolve_bronze_runs
from observatorio_secop.processing.silver.config import load_silver_config, load_territory_catalog
from observatorio_secop.processing.silver.errors import SilverConfigurationError
from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.processing.silver.transform import _business_date, transform_bronze


def test_default_policy_is_versioned_calendar_date():
    assert (
        load_silver_config(
            ROOT / "config/silver_quality.yaml", environment={}
        ).transformation_version
        == "silver-v2-calendar-date"
    )


def test_unknown_policy_rejected_before_outputs(silver_config, tmp_path):
    with pytest.raises(SilverConfigurationError, match="Unsupported Silver transformation_version"):
        replace(silver_config, transformation_version="silver-v999")
    document = yaml.safe_load((ROOT / "config/silver_quality.yaml").read_text())
    document["silver"]["transformation_version"] = "silver-v999"
    document["silver"]["silver_root"] = str(tmp_path / "must-not-exist")
    config_path = tmp_path / "bad-policy.yaml"
    config_path.write_text(yaml.safe_dump(document))
    with pytest.raises(SilverConfigurationError):
        load_silver_config(config_path, environment={})
    # Duck-typed callers must fail before catalog/storage/cache or Spark access.
    unknown = SimpleNamespace(transformation_version="silver-v999")
    with pytest.raises(SilverConfigurationError):
        SilverPipeline(unknown, config_path=config_path)
    with pytest.raises(SilverConfigurationError):
        transform_bronze(None, unknown, None, "must-not-build")
    assert not (tmp_path / "must-not-exist").exists()


def test_strict_clock_boundaries_and_historical_policy(silver_config, spark):
    values = [
        None,
        "",
        "2026-01-01T00:00:00.000",
        "2026-01-31T23:59:59.999",
        "2024-02-29T12:34:56.789",
        "2026-02-29T12:34:56.789",
        "2026-02-30T00:00:00.000",
        "2026-01-01T24:00:00.000",
        "2026-01-01T12:60:00.000",
        "2026-01-01T12:00:60.000",
        "2026-01-01",
        "2026-01-01T12:00:00Z",
        "2026-01-01T12:00:00.000Z",
        "2026-01-01T12:00:00.000-05:00",
        "2026-01-01T12:00:00.0000",
        " 2026-01-01T12:00:00.000",
    ]
    frame = spark.createDataFrame(list(enumerate(values)), "ordinal int, raw string")
    current = replace(silver_config, transformation_version="silver-v2-calendar-date")
    legacy = replace(silver_config, transformation_version="silver-v1")
    results = (
        frame.select(
            "ordinal",
            _business_date("raw", current).alias("current"),
            _business_date("raw", legacy).alias("legacy"),
        )
        .orderBy("ordinal")
        .collect()
    )
    assert [r.current for r in results[:5]] == [
        None,
        None,
        date(2026, 1, 1),
        date(2026, 1, 31),
        date(2024, 2, 29),
    ]
    assert all(r.current is None for r in results[5:])
    assert [r.ordinal for r in results if r.legacy is not None] == [2]


def test_calendar_date_uses_written_day_in_different_session_zones(silver_config, spark):
    config = replace(silver_config, transformation_version="silver-v2-calendar-date")
    original = spark.conf.get("spark.sql.session.timeZone")
    try:
        for zone in ["UTC", "America/Bogota", "Pacific/Kiritimati"]:
            spark.conf.set("spark.sql.session.timeZone", zone)
            result = (
                spark.createDataFrame([("2026-01-31T23:59:59.999",)], "raw string")
                .select(_business_date("raw", config).alias("day"))
                .first()
            )
            assert result.day == date(2026, 1, 31)
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)


def test_full_transform_preserves_raw_clocks_and_calendar_consistency(silver_config, spark):
    config = replace(silver_config, transformation_version="silver-v2-calendar-date")
    signed, start, end = (
        "2026-01-31T23:59:59.999",
        "2026-01-31T12:00:00.000",
        "2026-01-31T00:00:00.001",
    )
    updated = "2026-02-01T16:53:42.594"
    write_bronze_run(
        config,
        "calendar-policy",
        [
            row("SAME-DAY", signing=signed, start=start, end=end, updated=updated),
            row("MISSING", signing=None, start=None, end=None),
            row("NEXT-DAY", signing="2026-02-01T00:00:00.000", end=end),
            row("BAD-TIME", signing="2026-01-31T24:00:00.000"),
        ],
    )
    runs = resolve_bronze_runs(
        config.bronze_root,
        dataset_id=config.dataset_id,
        contract_sha256=source_contract_hash(),
        run_ids=("calendar-policy",),
    )
    bronze = read_bronze_dataframe(spark, runs)
    catalog = load_territory_catalog(config.territory_catalog_path)
    result = transform_bronze(bronze, config, catalog, "calendar-policy-version")
    try:
        valid = {r.source_contract_id: r for r in result.valid.collect()}
        same = valid["SAME-DAY"]
        assert (same.signing_date, same.start_date, same.end_date) == (date(2026, 1, 31),) * 3
        assert (same.signing_date_raw, same.start_date_raw, same.end_date_raw) == (
            signed,
            start,
            end,
        )
        assert same.source_updated_at_raw == updated
        # Spark TimestampType collect uses the Python host's local timezone.
        # Verify the existing source-clock semantics inside the UTC Spark session.
        updated_text = (
            result.valid.where(F.col("source_contract_id") == "SAME-DAY")
            .select(F.date_format("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("clock"))
            .first()
        )
        assert updated_text.clock == updated
        assert valid["MISSING"].signing_date is None
        rejected = {r.source_contract_id: r for r in result.rejected.collect()}
        assert "DATE_INCONSISTENT" in rejected["NEXT-DAY"].rejection_codes
        assert "DATE_INVALID" in rejected["BAD-TIME"].rejection_codes
        assert (
            result.counts["input_rows"]
            == result.counts["output_rows"] + result.counts["rejected_rows"]
        )
    finally:
        result.valid.unpersist()
        result.rejected.unpersist()
