from __future__ import annotations

from dataclasses import replace

import pytest
from conftest import row, source_contract_hash, write_bronze_run

from observatorio_secop.processing.silver.bronze import (
    read_bronze_dataframe,
    resolve_bronze_runs,
)
from observatorio_secop.processing.silver.config import load_territory_catalog
from observatorio_secop.processing.silver.quality import validate_quality
from observatorio_secop.processing.silver.transform import transform_bronze


def _transform(spark, config, run_ids):
    runs = resolve_bronze_runs(
        config.bronze_root,
        dataset_id=config.dataset_id,
        contract_sha256=source_contract_hash(),
        run_ids=tuple(run_ids),
    )
    bronze = read_bronze_dataframe(spark, runs)
    catalog = load_territory_catalog(config.territory_catalog_path)
    return transform_bronze(bronze, config, catalog, "version-test"), catalog


def test_conversion_territory_rejections_and_warning(silver_config, spark) -> None:
    invalid = row(
        None,
        amount="12.345",
        signing="2027-01-01T00:00:00.000",
        end="2026-01-01T00:00:00.000",
    )
    write_bronze_run(
        silver_config,
        "run-1",
        [
            row("VALID", city="Itagui", amount="100.000000"),
            row("UNKNOWN", city="Municipio inventado"),
            invalid,
        ],
    )
    result, _ = _transform(spark, silver_config, ["run-1"])
    assert result.counts["input_rows"] == 3
    assert result.counts["output_rows"] == 2
    assert result.counts["rejected_rows"] == 1
    valid = {item.source_contract_id: item for item in result.valid.collect()}
    assert valid["VALID"].municipality_name == "Itagüí"
    assert valid["VALID"].is_valle_de_aburra is True
    assert valid["UNKNOWN"].municipality_status == "UNKNOWN"
    assert valid["UNKNOWN"].quality_warning_codes == ["UNKNOWN_MUNICIPALITY"]
    rejected = result.rejected.first()
    assert set(rejected.rejection_codes) == {
        "AMOUNT_SCALE_EXCEEDED",
        "DATE_INCONSISTENT",
        "IDENTIFIER_NULL",
    }


@pytest.mark.parametrize(
    ("amount", "code"),
    [
        (None, "AMOUNT_NULL"),
        ("-1", "AMOUNT_NEGATIVE"),
        ("COP 10", "AMOUNT_INVALID_FORMAT"),
        ("1.001", "AMOUNT_SCALE_EXCEEDED"),
        ("1234567890123456789.00", "AMOUNT_OVERFLOW"),
    ],
)
def test_amount_diagnostics(silver_config, spark, amount, code) -> None:
    write_bronze_run(silver_config, "run-1", [row("A", amount=amount)])
    result, _ = _transform(spark, silver_config, ["run-1"])
    assert code in result.rejected.first().rejection_codes


def test_invalid_date_and_department_are_not_corrected(silver_config, spark) -> None:
    write_bronze_run(
        silver_config,
        "run-1",
        [
            row("DATE", signing="2026-02-30T00:00:00.000"),
            row("DEPT", department="Cundinamarca"),
        ],
    )
    result, _ = _transform(spark, silver_config, ["run-1"])
    codes = {
        item.source_contract_id: set(item.rejection_codes) for item in result.rejected.collect()
    }
    assert "DATE_INVALID" in codes["DATE"]
    assert codes["DEPT"] == {"DEPARTMENT_OUT_OF_SCOPE"}


def test_exact_duplicates_and_late_versions_use_total_order(silver_config, spark) -> None:
    old = row("A", updated="2026-01-01T00:00:00.000", status="Old")
    write_bronze_run(
        silver_config,
        "run-1",
        [old, old, row("B", updated=None, status="First")],
        fetched_at="2026-01-02T00:00:00.000Z",
    )
    write_bronze_run(
        silver_config,
        "run-2",
        [
            row("A", updated="2026-02-01T00:00:00.000", status="Newest"),
            row("A", updated="2025-12-01T00:00:00.000", status="Late but old"),
            row("B", updated=None, status="Second"),
        ],
        fetched_at="2026-03-01T00:00:00.000Z",
    )
    result, _ = _transform(spark, silver_config, ["run-1", "run-2"])
    selected = {item.source_contract_id: item.contract_status for item in result.valid.collect()}
    assert selected == {"A": "Newest", "B": "Second"}
    assert result.counts["exact_duplicates"] == 1
    assert result.counts["superseded_versions"] == 3
    assert result.counts["input_rows"] == sum(
        result.counts[key]
        for key in ("output_rows", "rejected_rows", "exact_duplicates", "superseded_versions")
    )


def test_key_is_stable_and_collision_is_measured(silver_config, spark) -> None:
    write_bronze_run(silver_config, "run-1", [row("A"), row(" A ")])
    first, _ = _transform(spark, silver_config, ["run-1"])
    second, _ = _transform(spark, silver_config, ["run-1"])
    assert first.valid.first().contract_key == second.valid.first().contract_key
    assert first.counts["collision_groups"] == 1
    assert first.counts["collision_rows"] == 2


def test_quality_threshold_boundary_and_warning(silver_config, spark) -> None:
    write_bronze_run(
        silver_config,
        "run-1",
        [row("A", city="Municipio inventado")],
    )
    transformed, catalog = _transform(spark, silver_config, ["run-1"])
    counts = {
        **transformed.counts,
        "input_rows": 50,
        "output_rows": 49,
        "rejected_rows": 1,
        "rejection_rate": 0.02,
    }
    results = validate_quality(spark, transformed.valid, counts, silver_config, catalog)
    by_id = {item.check_id: item for item in results}
    assert by_id["REJECTION_RATE"].passed is True
    assert by_id["UNKNOWN_MUNICIPALITY"].passed is False
    assert all(item.passed for item in results if item.severity == "critical")

    above = {**counts, "input_rows": 49, "output_rows": 48, "rejection_rate": 1 / 49}
    results = validate_quality(spark, transformed.valid, above, silver_config, catalog)
    assert {item.check_id: item for item in results}["REJECTION_RATE"].passed is False


def test_empty_output_is_critical(silver_config, spark) -> None:
    write_bronze_run(silver_config, "run-1", [])
    transformed, catalog = _transform(spark, silver_config, ["run-1"])
    results = validate_quality(spark, transformed.valid, transformed.counts, silver_config, catalog)
    by_id = {item.check_id: item for item in results}
    assert by_id["NON_EMPTY_OUTPUT"].passed is False
    allowed = replace(silver_config, allow_empty_output=True)
    results = validate_quality(spark, transformed.valid, transformed.counts, allowed, catalog)
    assert {item.check_id: item for item in results}["NON_EMPTY_OUTPUT"].passed is True
