from __future__ import annotations

import json
import time
from dataclasses import replace

import pytest
from pyspark.sql import functions as F

from observatorio_secop.identity import key_set_digest
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.models import VerifiedBronzeRun
from observatorio_secop.processing.silver.refresh import reconcile_refresh, refresh_baseline
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from warehouse.conftest import identity_row, rewrite_manifest, write_identity_bundle


def run_record(tmp_path, name, *, baseline=None):
    parameters, evidence = {}, None
    if baseline:
        parameters = {
            "baseline_version_id": baseline.version_id,
            "baseline_manifest_sha256": baseline.manifest_sha256,
            "requested_ids": ["CO1.PCCNTR.1"],
            "ids_sha256": key_set_digest(["CO1.PCCNTR.1"]),
        }
        evidence = {
            "complete": True,
            "baseline_bronze_runs": json.loads(baseline.manifest_path.read_text())["bronze_runs"],
        }
    return VerifiedBronzeRun(
        run_id=name,
        dataset_id="jbjy-vk9h",
        contract_sha256="c" * 64,
        manifest_path=tmp_path / name,
        manifest_sha256=name * 32,
        pages=(),
        purpose="identity_refresh" if baseline else "incremental",
        parameters=parameters,
        refresh=evidence,
        range_complete=True,
    )


def make_baseline(root, version, inputs, winner, refresh=None):
    row = identity_row(version)
    row["bronze_run_id"] = winner
    path = write_identity_bundle(root, version, [row])
    rewrite_manifest(
        path,
        lambda manifest: manifest.update(
            bronze_runs=[
                {"run_id": run.run_id, "manifest_sha256": run.manifest_sha256} for run in inputs
            ],
            input_purposes={run.run_id: run.purpose for run in inputs},
            refresh=refresh,
        ),
    )
    return verify_manifest_bundle(path)


def test_refresh_can_extend_a_baseline_that_already_contains_a_refresh(tmp_path):
    seed = run_record(tmp_path, "s0")
    original = make_baseline(tmp_path, "base-1", [seed], seed.run_id)
    first = run_record(tmp_path, "r1", baseline=original)
    _, evidence = refresh_baseline((seed, first), tmp_path)
    evidence["candidate_keys_sha256"] = evidence["baseline_keys_sha256"]
    completed = make_baseline(tmp_path, "base-2", [seed, first], first.run_id, evidence)
    second = run_record(tmp_path, "r2", baseline=completed)
    baseline, evidence = refresh_baseline((seed, first, second), tmp_path)
    assert baseline.version_id == completed.version_id
    assert {item["run_id"] for item in evidence["refresh_runs"]} == {"r1", "r2"}
    assert {item["run_id"] for item in evidence["baseline_bronze_runs"]} == {"s0", "r1"}


def test_incomplete_refresh_or_missing_baseline_dependencies_are_blocked(tmp_path):
    seed = run_record(tmp_path, "s0")
    original = make_baseline(tmp_path, "base", [seed], seed.run_id)
    refresh = run_record(tmp_path, "r1", baseline=original)
    with pytest.raises(PublicationError, match="baseline Bronze dependencies"):
        refresh_baseline((refresh,), tmp_path)
    with pytest.raises(PublicationError, match="incomplete"):
        refresh_baseline((seed, replace(refresh, range_complete=False)), tmp_path)
    with pytest.raises(PublicationError, match="pinned baseline"):
        refresh_baseline(
            (
                seed,
                replace(refresh, parameters={**refresh.parameters, "baseline_version_id": ".."}),
            ),
            tmp_path,
        )


@pytest.mark.parametrize("timezone", ["UTC", "Etc/GMT+5"])
@pytest.mark.parametrize("changed_value", [False, True])
def test_reconciliation_compares_source_clock_without_host_timezone_shift(
    tmp_path,
    spark,
    monkeypatch,
    timezone,
    changed_value,
):
    seed = run_record(tmp_path, "s0")
    baseline = make_baseline(tmp_path, "base", [seed], seed.run_id)
    raw = baseline.table.to_pylist()[0]
    frame = spark.read.json(spark.sparkContext.parallelize([json.dumps(raw, default=str)]))
    frame = frame.withColumn("source_updated_at", F.to_timestamp("source_updated_at"))
    frame = frame.withColumn(
        "contract_value_cop", F.col("contract_value_cop").cast("decimal(20,2)")
    )
    for field in ("signing_date", "start_date", "end_date"):
        frame = frame.withColumn(field, F.to_date(field))
    if changed_value:
        frame = frame.withColumn("contract_value_cop", F.lit("123.45").cast("decimal(20,2)"))
    try:
        with monkeypatch.context() as environment:
            environment.setenv("TZ", timezone)
            time.tzset()
            evidence = reconcile_refresh(
                frame,
                (
                    baseline,
                    {
                        "baseline_keys_sha256": key_set_digest([raw["contract_key"]]),
                    },
                ),
            )
            assert evidence["business_changed_contracts"] == int(changed_value)
            if changed_value:
                assert evidence["changed_contracts"][0]["business_changed_fields"] == [
                    "contract_value_cop"
                ]
                assert evidence["changed_contracts"][0]["before_value_cop"] == "100.00"
                assert evidence["changed_contracts"][0]["after_value_cop"] == "123.45"
            else:
                assert evidence["changed_contracts"] == []
    finally:
        time.tzset()
