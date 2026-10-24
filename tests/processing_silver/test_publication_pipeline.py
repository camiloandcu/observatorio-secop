"""Real Spark-produced population and quality reach an isolated, pinned publication."""

from __future__ import annotations

import os
import shutil
from dataclasses import asdict, replace
from decimal import Decimal

import pytest
import yaml
from conftest import ROOT, row
from psycopg.conninfo import conninfo_to_dict
from test_population_pipeline import SCOPE, acquire

from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.publication.build import run_candidate_build
from observatorio_secop.publication.capture import capture_inputs
from observatorio_secop.publication.readers import read_publication, resolve
from observatorio_secop.publication.store import PublicationStore
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader
from warehouse.test_dbt_postgres import _warehouse_config
from warehouse.test_publication_acceptance import (
    BUDGET,
    _activate,
    _drop_owned_test_schemas,
    _read_all,
)

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires real Spark/disposable PostgreSQL")


def test_real_population_capture_build_publication_and_retained_quality(
    silver_config, spark, tmp_path, monkeypatch
):
    assert TEST_DSN
    config = replace(silver_config, max_rejection_rate=0.5)
    config_path = tmp_path / "silver-publication.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "silver": {
                    k: str(v) if hasattr(v, "as_posix") else v for k, v in asdict(config).items()
                }
            }
        )
    )
    pipeline = SilverPipeline(config, config_path=config_path, spark=spark)
    warehouse = _warehouse_config(config.silver_root, conninfo_to_dict(TEST_DSN))
    loader = WarehouseLoader(warehouse)
    _drop_owned_test_schemas()
    store = PublicationStore(warehouse)
    store.bootstrap()

    def publish(bundle, *, fail_first=False):
        loader.load(bundle)
        reservation = store.reserve({})
        publication_id = reservation["publication_id"]
        metadata = capture_inputs(warehouse, bundle, publication_id, storage_budget_bytes=BUDGET)
        store.update_metadata(publication_id, metadata)
        if fail_first:
            failed_project = tmp_path / "failed-project"
            for directory in ("dbt", "contracts", "config", "scripts"):
                shutil.copytree(
                    ROOT / directory,
                    failed_project / directory,
                    ignore=shutil.ignore_patterns("target", "logs", "__pycache__"),
                )
            # A real critical test fails after real models have been materialized.
            (failed_project / "dbt/tests/publication_forced_failure.sql").write_text(
                "select 1 as synthetic_failure\n"
            )
            with pytest.raises(ValueError, match="PUBLICATION_DBT_BUILD_FAILED"):
                run_candidate_build(
                    warehouse,
                    publication_id,
                    artifact_root=tmp_path / "failed-builds",
                    timeout_seconds=180,
                    storage_budget_bytes=BUDGET,
                    metadata=metadata,
                    project_root=failed_project,
                )
            assert _read_all(warehouse, a_descriptor) == a_contents
            assert resolve(warehouse)["publication_id"] == a_publication
            before_retry = store.inspect(publication_id)
            with monkeypatch.context() as retry_guard:
                retry_guard.setattr(
                    "observatorio_secop.publication.build.subprocess.run",
                    lambda *_a, **_kw: pytest.fail("Changed-recipe retry reached dbt"),
                )
                # Switching from the failing copied graph to the original recipe
                # requires a new candidate identity, even though Silver is unchanged.
                with pytest.raises(ValueError, match="PUBLICATION_CONTRACT_CHANGED"):
                    run_candidate_build(
                        warehouse,
                        publication_id,
                        artifact_root=tmp_path / "changed-retry",
                        timeout_seconds=180,
                        storage_budget_bytes=BUDGET,
                        metadata=before_retry["metadata"],
                    )
            assert store.inspect(publication_id) == before_retry
            store.discard(publication_id)
            reservation = store.reserve({})
            publication_id = reservation["publication_id"]
            metadata = capture_inputs(
                warehouse, bundle, publication_id, storage_budget_bytes=BUDGET
            )
            store.update_metadata(publication_id, metadata)
        receipt = run_candidate_build(
            warehouse,
            publication_id,
            artifact_root=tmp_path / "builds",
            timeout_seconds=180,
            storage_budget_bytes=BUDGET,
            metadata=metadata,
        )
        store.update_metadata(publication_id, receipt["metadata"])
        store.seal(publication_id, receipt)
        _activate(store, publication_id)
        return publication_id

    try:
        first = acquire(config, "publication-original", [[row("A", updated=None)]])
        pipeline.run_population(SCOPE, (first.run_id,), mode="authoritative_candidate")
        a = verify_current_bundle(config.silver_root)
        a_publication = publish(a)
        a_descriptor = resolve(warehouse)
        a_contents = _read_all(warehouse, a_descriptor)
        assert a_contents["fact_contract"][0]["contract_value_cop"] == Decimal("100.00")

        second = acquire(
            config,
            "publication-correction",
            [
                [row("A", updated=None, amount="200.00"), row("B", updated=None, amount="300.00")],
                [row("A", updated=None, amount="-1.00")],
            ],
            baseline=a.manifest_path,
            day=2,
        )
        pipeline.run_population(
            SCOPE,
            (second.run_id,),
            baseline_version_id=a.version_id,
            baseline_manifest_sha256=a.manifest_sha256,
            mode="authoritative_candidate",
        )
        b = verify_current_bundle(config.silver_root)
        b_publication = publish(b, fail_first=True)
        assert b_publication != a_publication
        detail = read_publication(warehouse, resolve(warehouse), "mart_contract_detail")
        assert len(detail) == 2
        assert sum(item["contract_value_cop"] for item in detail) == Decimal("500.00")
        assert {item["publication_id"] for item in detail} == {b_publication}
        assert {item["silver_version_id"] for item in detail} == {b.version_id}
        assert "LATER_INVALID_OBSERVED" in {item["condition_state"] for item in detail}
        quality = read_publication(warehouse, resolve(warehouse), "mart_data_quality")
        controls = {item["control_id"]: item for item in quality}
        assert controls["input_observations"]["numerator"] == 4
        assert controls["rejected_observations"]["numerator"] == 1
        assert controls["superseded_valid_observations"]["numerator"] == 1
        assert _read_all(warehouse, a_descriptor) == a_contents
        _activate(store, a_publication, restore=True)
        assert _read_all(warehouse, resolve(warehouse)) == a_contents
    finally:
        _drop_owned_test_schemas()
