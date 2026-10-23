"""Actual bounded acquisition -> Spark -> PostgreSQL -> dbt acceptance boundary."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
import yaml
from conftest import ROOT, row
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.reconcile import PopulationReconciliation
from observatorio_secop.ingestion.transport import SocrataTransport
from observatorio_secop.population import PopulationScope
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.processing.silver.selection import identity
from observatorio_secop.warehouse.bundle import verify_current_bundle, verify_manifest_bundle
from observatorio_secop.warehouse.errors import WarehouseConflictError
from observatorio_secop.warehouse.loader import WarehouseLoader
from warehouse.test_dbt_postgres import _dbt, _dbt_environment, _drop_schemas, _warehouse_config

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires dedicated disposable PostgreSQL")
SCOPE = PopulationScope("2026-01-01", "2026-02-01")


class Response:
    headers: dict = {}
    status = 200

    def __init__(self, payload):
        self.raw = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self, limit):
        return self.raw[:limit]


def acquire(config, campaign_id, observations, *, baseline=None, day=1):
    ingestion = replace(
        load_ingestion_config(ROOT / "config/bronze_ingestion.yaml"),
        bronze_root=config.bronze_root,
        page_size=50,
        max_rows=100,
        contract_path=ROOT / "contracts/secop_source.yaml",
    )
    contract = load_bronze_contract(ingestion.contract_path, ingestion.projection)
    metadata = {
        "id": contract.dataset_id,
        "columns": [
            {"fieldName": name, "dataTypeName": value["declared_type"]}
            for name, value in contract.columns.items()
        ],
    }
    replies = iter([metadata, *observations])
    clock_tick = iter(range(100))
    transport = SocrataTransport(
        ingestion,
        contract,
        opener=lambda *_args, **_kwargs: Response(next(replies)),
        sleeper=lambda _: None,
    )
    runner = PopulationReconciliation(
        ingestion,
        contract,
        transport=transport,
        clock=lambda: datetime(2026, 10, day, tzinfo=UTC) + timedelta(seconds=next(clock_tick)),
        uuid_factory=lambda: campaign_id,
    )
    with runner.state.connect() as database:
        database.execute(
            "INSERT INTO checkpoints VALUES (?, 'primary', ?, 'unchanged') ON CONFLICT DO NOTHING",
            (contract.dataset_id, "2026-10-01T00:00:00Z"),
        )
        checkpoint_before = database.execute(
            "SELECT * FROM checkpoints ORDER BY dataset_id,lane"
        ).fetchall()
    manifest = runner.reconcile(
        SCOPE.signing_from,
        SCOPE.signing_to,
        total_rows=100,
        total_requests=20,
        grant_rows=100,
        grant_requests=20,
        baseline_path=baseline,
    )
    with runner.state.connect() as database:
        assert (
            database.execute("SELECT * FROM checkpoints ORDER BY dataset_id,lane").fetchall()
            == checkpoint_before
        )
    assert manifest.range_complete and manifest.checkpoint_candidate is None
    return manifest


def logical_rows():
    with psycopg.connect(TEST_DSN) as database:
        return database.execute(
            "SELECT to_jsonb(c) FROM silver.contract_current c ORDER BY contract_key"
        ).fetchall()


def test_population_bootstrap_and_preview_use_real_verified_boundary(
    silver_config, spark, tmp_path, monkeypatch
):
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    loader = WarehouseLoader(_warehouse_config(silver_config.silver_root, connection))
    pipeline = SilverPipeline(
        silver_config, config_path=ROOT / "config/silver_quality.yaml", spark=spark
    )
    campaign = acquire(silver_config, "scope-bootstrap", [[row("A", updated=None)]])
    _drop_schemas()
    try:
        baseline = pipeline.run_population(
            SCOPE, (campaign.run_id,), mode="authoritative_candidate"
        )
        assert baseline.critical_passed
        bundle = verify_current_bundle(silver_config.silver_root)
        assert bundle.schema_version == 3 and bundle.row_count == 1
        assert bundle.coverage["state"] == "complete"
        assert bundle.coverage["basis"] == "bounded_scan_observed"
        assert bundle.condition_summaries[0]["state"] == "NO_LATER_INVALID_OBSERVED"
        assert loader.load(bundle).status == "applied"
        before_rows = logical_rows()
        before_pointer = pipeline.storage.current_path.read_bytes()
        preview = pipeline.run_population(SCOPE, (campaign.run_id,), mode="preview")
        preview_path = pipeline.storage.version_path(preview.silver_version_id) / "manifest.json"
        preview_bundle = verify_manifest_bundle(preview_path)
        assert preview_bundle.coverage["mode"] == "preview"
        with pytest.raises(PublicationError, match="Preview"):
            pipeline.storage.rollback(preview.silver_version_id)
        with monkeypatch.context() as guard:
            guard.setattr(
                psycopg, "connect", lambda **_: pytest.fail("Preview reached database connection")
            )
            with pytest.raises(WarehouseConflictError, match="Preview"):
                loader.load(preview_bundle)
        assert pipeline.storage.current_path.read_bytes() == before_pointer
        assert logical_rows() == before_rows
        # Existing-version preview reuse must not invoke rollback either.
        assert (
            pipeline.run_population(SCOPE, (campaign.run_id,), mode="preview").silver_version_id
            == preview.silver_version_id
        )
        assert pipeline.storage.current_path.read_bytes() == before_pointer
        environment = _dbt_environment(tmp_path, connection)
        _dbt(environment, "build", "--full-refresh")
        with psycopg.connect(TEST_DSN) as database:
            assert database.execute(
                "SELECT count(*),sum(contract_value_cop) FROM analytics.fact_contract"
            ).fetchone() == (1, Decimal("100.00"))
            assert database.execute(
                "SELECT scope_id,silver_version_id,coverage_known,coverage_state,basis "
                "FROM staging.stg_population_coverage"
            ).fetchall() == [
                (SCOPE.scope_id, bundle.version_id, True, "complete", "bounded_scan_observed")
            ]
            assert database.execute(
                "SELECT condition_state FROM staging.stg_contract_observation_condition"
            ).fetchall() == [("NO_LATER_INVALID_OBSERVED",)]
    finally:
        _drop_schemas()


def test_population_recovery_later_invalid_and_registered_restore_are_exact(
    silver_config, spark, tmp_path
):
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    # This fixture produces one reject and one supersession across four observations.
    # Production's rejection threshold remains unchanged.
    config = replace(silver_config, max_rejection_rate=0.5)
    config_path = tmp_path / "population-silver.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "silver": {
                    name: str(value) if hasattr(value, "as_posix") else value
                    for name, value in asdict(config).items()
                }
            }
        )
    )
    pipeline = SilverPipeline(config, config_path=config_path, spark=spark)
    loader = WarehouseLoader(_warehouse_config(config.silver_root, connection))
    first = acquire(config, "scope-original", [[row("A", updated=None)]])
    _drop_schemas()
    try:
        initial = pipeline.run_population(SCOPE, (first.run_id,), mode="authoritative_candidate")
        a = verify_current_bundle(config.silver_root)
        loader.load(a)
        before_rows = logical_rows()
        assert a.quality_summary["state"] == "AVAILABLE"
        with psycopg.connect(TEST_DSN) as database:
            original_quality = database.execute(
                "SELECT summary,evidence_sha256 FROM silver.contract_current_quality "
                "WHERE silver_version_id=%s AND manifest_sha256=%s",
                (a.version_id, a.manifest_sha256),
            ).fetchone()
            assert original_quality[0] == a.quality_summary
        environment = _dbt_environment(tmp_path, connection)
        _dbt(environment, "build", "--full-refresh")
        with psycopg.connect(TEST_DSN) as database:
            original_facts = database.execute(
                "SELECT to_jsonb(f) FROM analytics.fact_contract f ORDER BY contract_key"
            ).fetchall()
            assert database.execute(
                "SELECT count(*),sum(contract_value_cop),min(silver_version_id),"
                "max(silver_version_id) FROM analytics.fact_contract"
            ).fetchone() == (1, Decimal("100.00"), a.version_id, a.version_id)
        original_hash = hashlib.sha256(a.manifest_path.read_bytes()).hexdigest()
        second = acquire(
            config,
            "scope-correction",
            [
                [
                    row("A", updated=None, amount="200.00"),
                    row("B", updated=None, amount="300.00"),
                ],
                [row("A", updated=None, amount="-1.00")],
            ],
            baseline=a.manifest_path,
            day=2,
        )
        corrected = pipeline.run_population(
            SCOPE,
            (second.run_id,),
            baseline_version_id=a.version_id,
            baseline_manifest_sha256=a.manifest_sha256,
            mode="authoritative_candidate",
        )
        assert corrected.critical_passed
        b = verify_current_bundle(config.silver_root)
        assert b.row_count == 2 and sum(b.table["contract_value_cop"].to_pylist()) == Decimal(
            "500.00"
        )
        a_key = a.table["contract_key"][0].as_py()
        condition = next(item for item in b.condition_summaries if item["contract_key"] == a_key)
        assert condition["state"] == "LATER_INVALID_OBSERVED"
        assert condition["later_invalid_count"] == 1
        assert len(b.condition_links) == 1
        assert b.condition_links[0]["rejection_codes"] == ["AMOUNT_NEGATIVE"]
        assert b.quality_summary["counts"] == {
            "input_observations": 4,
            "output_contracts": 2,
            "rejected_observations": 1,
            "exact_duplicates": 0,
            "superseded_valid_observations": 1,
            "accepted_valid_observations": 3,
        }
        assert loader.load(b).status == "applied"
        assert loader.load(b).status == "noop"
        assert hashlib.sha256(a.manifest_path.read_bytes()).hexdigest() == original_hash
        _dbt(environment, "build", "--full-refresh")
        with psycopg.connect(TEST_DSN) as database:
            assert database.execute(
                "SELECT count(*),sum(contract_value_cop) FROM analytics.fact_contract"
            ).fetchone() == (2, Decimal("500.00"))
            assert database.execute(
                "SELECT DISTINCT silver_version_id FROM analytics.fact_contract"
            ).fetchall() == [(b.version_id,)]
            assert database.execute(
                "SELECT count(*) FROM analytics.fact_contract f JOIN "
                "staging.stg_contract_observation_condition c "
                "USING(silver_version_id,contract_key)"
            ).fetchone() == (2,)
            assert database.execute(
                "SELECT condition_state FROM staging.stg_contract_observation_condition "
                "WHERE contract_key=%s",
                (a_key,),
            ).fetchone() == ("LATER_INVALID_OBSERVED",)
            assert database.execute(
                "SELECT count(*),count(distinct contract_key) FROM analytics.mart_contract_detail"
            ).fetchone() == (2, 2)
            assert database.execute(
                "SELECT condition_state FROM analytics.mart_contract_detail WHERE contract_key=%s",
                (a_key,),
            ).fetchone() == ("LATER_INVALID_OBSERVED",)
            assert database.execute(
                "SELECT control_id,numerator,denominator FROM analytics.mart_data_quality "
                "WHERE control_id IN ('input_observations','rejected_observations',"
                "'exact_duplicates','superseded_valid_observations') ORDER BY control_id"
            ).fetchall() == [
                ("exact_duplicates", 0, 4),
                ("input_observations", 4, 4),
                ("rejected_observations", 1, 4),
                ("superseded_valid_observations", 1, 4),
            ]
        # A verified baseline correction outside the signing interval must not
        # silently remove that member, even when another member remains valid.
        before_drift_rows = logical_rows()
        before_drift_pointer = pipeline.storage.current_path.read_bytes()
        drift = acquire(
            config,
            "scope-drift",
            [
                [row("B", updated=None, amount="300.00")],
                [
                    row("A", updated=None, signing="2026-02-01T00:00:00.000"),
                    row("B", updated=None, amount="300.00"),
                ],
            ],
            baseline=b.manifest_path,
            day=3,
        )
        denied = pipeline.run_population(
            SCOPE,
            (drift.run_id,),
            baseline_version_id=b.version_id,
            baseline_manifest_sha256=b.manifest_sha256,
            mode="authoritative_candidate",
        )
        assert not denied.critical_passed
        assert pipeline.storage.current_path.read_bytes() == before_drift_pointer
        assert logical_rows() == before_drift_rows
        pipeline.storage.rollback(
            initial.silver_version_id,
            manifest_sha256=a.manifest_sha256,
            expected_current={
                "silver_version_id": b.version_id,
                "manifest_sha256": b.manifest_sha256,
            },
            restore=True,
        )
        assert identity(pipeline.storage.current())["silver_version_id"] == a.version_id
        assert (
            loader.load(a, restore=True, expected_current=(b.version_id, b.manifest_sha256)).status
            == "applied"
        )
        assert logical_rows() == before_rows
        with psycopg.connect(TEST_DSN) as database:
            assert (
                database.execute(
                    "SELECT summary,evidence_sha256 FROM silver.contract_current_quality "
                    "WHERE silver_version_id=%s AND manifest_sha256=%s",
                    (a.version_id, a.manifest_sha256),
                ).fetchone()
                == original_quality
            )
            assert database.execute(
                "SELECT summary FROM silver.contract_current_quality "
                "WHERE silver_version_id=%s AND manifest_sha256=%s",
                (b.version_id, b.manifest_sha256),
            ).fetchone() == (b.quality_summary,)
        # Incremental reconciliation must remove B-only facts and recover A's
        # complete deterministic lineage, including manifest-derived loaded_at.
        _dbt(environment, "build")
        with psycopg.connect(TEST_DSN) as database:
            assert (
                database.execute(
                    "SELECT to_jsonb(f) FROM analytics.fact_contract f ORDER BY contract_key"
                ).fetchall()
                == original_facts
            )
            assert database.execute(
                "SELECT count(*),sum(contract_value_cop),min(silver_version_id),"
                "max(silver_version_id) FROM analytics.fact_contract"
            ).fetchone() == (1, Decimal("100.00"), a.version_id, a.version_id)
        assert (
            loader.load(a, restore=True, expected_current=(b.version_id, b.manifest_sha256)).status
            == "noop"
        )
    finally:
        _drop_schemas()
