from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from conftest import ROOT, row, write_bronze_run

from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.processing.silver.storage import SilverStorage


def _pipeline(config, spark):
    return SilverPipeline(
        config,
        config_path=ROOT / "config/silver_quality.yaml",
        spark=spark,
    )


def test_pipeline_publishes_partitioned_snappy_and_is_idempotent(silver_config, spark) -> None:
    write_bronze_run(
        silver_config,
        "run-1",
        [row("A"), row("B", city="Municipio inventado")],
    )
    pipeline = _pipeline(silver_config, spark)
    first = pipeline.run(("run-1",))
    second = pipeline.run(("run-1",))
    assert first.status == "valid"
    assert first.silver_version_id == second.silver_version_id
    assert first.counts == second.counts
    version = silver_config.silver_root / "versions" / first.silver_version_id
    assert (version / "data/signing_year=2026").is_dir()
    assert (version / "rejected").is_dir()
    assert (
        json.loads((silver_config.silver_root / "current.json").read_text())["silver_version_id"]
        == first.silver_version_id
    )
    parquet = next((version / "data").rglob("*.parquet"))
    metadata = pq.ParquetFile(parquet).metadata
    assert metadata.row_group(0).column(0).compression == "SNAPPY"


def test_critical_failure_preserves_current_and_writes_invalid_bundle(silver_config, spark) -> None:
    write_bronze_run(silver_config, "good", [row("GOOD")])
    pipeline = _pipeline(silver_config, spark)
    good = pipeline.run(("good",))
    pointer_before = (silver_config.silver_root / "current.json").read_bytes()

    write_bronze_run(silver_config, "bad", [row("BAD", amount="invalid")])
    bad = pipeline.run(("bad",))
    assert bad.status == "invalid"
    assert bad.critical_passed is False
    assert (silver_config.silver_root / "current.json").read_bytes() == pointer_before
    assert (silver_config.silver_root / "invalid" / bad.silver_version_id).is_dir()
    assert good.silver_version_id != bad.silver_version_id


def test_staging_is_not_current_and_cleanup_is_scoped(silver_config) -> None:
    storage = SilverStorage(silver_config.silver_root)
    staging = storage.staging_path("candidate")
    staging.mkdir(parents=True)
    (staging / "partial").write_text("partial")
    protected = silver_config.silver_root / "versions" / "valid"
    protected.mkdir(parents=True)
    assert storage.current() == {"current": None}
    assert storage.clean_staging() == 1
    assert protected.exists()


def test_rollback_repoints_only_to_valid_version(silver_config, spark) -> None:
    write_bronze_run(silver_config, "run-1", [row("A")])
    manifest = _pipeline(silver_config, spark).run(("run-1",))
    storage = SilverStorage(silver_config.silver_root)
    (silver_config.silver_root / "current.json").unlink()
    pointer = storage.rollback(manifest.silver_version_id)
    assert pointer["silver_version_id"] == manifest.silver_version_id


def test_manifest_contains_deterministic_quality_evidence(silver_config, spark) -> None:
    write_bronze_run(silver_config, "run-1", [row("A")])
    manifest = _pipeline(silver_config, spark).run(("run-1",))
    check_ids = [item.check_id for item in manifest.quality_results]
    assert check_ids == sorted(check_ids)
    assert {"REJECTION_RATE", "KEY_COLLISION", "COUNT_RECONCILIATION"} <= set(check_ids)
    assert manifest.counts["input_rows"] == manifest.counts["output_rows"]
    version = silver_config.silver_root / "versions" / manifest.silver_version_id
    assert (version / "quality/results.json").exists()
    assert (version / "manifest.json").exists()


def test_retry_repairs_pointer_after_crash_between_promotion_and_pointer(
    silver_config, spark, monkeypatch
) -> None:
    write_bronze_run(silver_config, "run-1", [row("A")])
    pipeline = _pipeline(silver_config, spark)
    original = pipeline.storage._write_current

    def interrupt(*_args, **_kwargs):
        raise OSError("simulated pointer interruption")

    monkeypatch.setattr(pipeline.storage, "_write_current", interrupt)
    with pytest.raises(OSError, match="pointer interruption"):
        pipeline.run(("run-1",))
    versions = list((silver_config.silver_root / "versions").iterdir())
    assert len(versions) == 1
    assert not (silver_config.silver_root / "current.json").exists()

    monkeypatch.setattr(pipeline.storage, "_write_current", original)
    recovered = pipeline.run(("run-1",))
    assert recovered.status == "valid"
    assert (
        json.loads((silver_config.silver_root / "current.json").read_text())["silver_version_id"]
        == recovered.silver_version_id
    )


def test_idempotent_retry_rejects_tampered_published_file(silver_config, spark) -> None:
    write_bronze_run(silver_config, "run-1", [row("A")])
    pipeline = _pipeline(silver_config, spark)
    manifest = pipeline.run(("run-1",))
    version = silver_config.silver_root / "versions" / manifest.silver_version_id
    parquet = next((version / "data").rglob("*.parquet"))
    with parquet.open("ab") as stream:
        stream.write(b"tampered")
    with pytest.raises(PublicationError, match="failed verification"):
        pipeline.run(("run-1",))
