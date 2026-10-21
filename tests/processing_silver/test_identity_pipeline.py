from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from conftest import ROOT, row, write_bronze_run
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.refresh import IdentityRefresh
from observatorio_secop.ingestion.transport import SocrataTransport
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.processing.silver.selection import adopt
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader
from warehouse.snapshot_helpers import advance_registered, seed_registered
from warehouse.test_dbt_postgres import (
    _dbt,
    _dbt_environment,
    _drop_schemas,
    _logical_hashes,
    _warehouse_config,
)

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires dedicated disposable PostgreSQL")


def file_hashes(root: Path):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


class Response:
    headers = {}
    status = 200

    def __init__(self, payload):
        self.raw = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self, limit):
        return self.raw[:limit]


def observed(identity, entity_code=None, supplier_code=None, name="Shared label", **kwargs):
    source = row(identity, **kwargs)
    source.update(
        codigo_entidad=entity_code,
        codigo_proveedor=supplier_code,
        proveedor_adjudicado=name,
        nombre_entidad=name,
    )
    return source


def star_identity():
    with psycopg.connect(TEST_DSN) as database:
        return {
            r[0]: r[1:]
            for r in database.execute(
                "SELECT f.contract_id, f.contract_key, f.contract_sk, f.entity_sk, f.supplier_sk, "
                "e.entity_code, s.supplier_code, f.entity_identity_reason, "
                "f.supplier_identity_reason, "
                "f.contract_value_cop, f.bronze_run_id, f.source_projection_version "
                "FROM analytics.fact_contract f JOIN analytics.dim_entity e USING(entity_sk) "
                "JOIN analytics.dim_supplier s USING(supplier_sk) ORDER BY f.contract_id"
            ).fetchall()
        }


def historical_snapshot(pipeline, manifest):
    """Model a retained pre-EN02 publication using real writer/refresh output.

    This fixture represents old snapshots, never an allowed new unscoped
    production activation. New scoped acquisition is exercised separately.
    """
    path = pipeline.storage.version_path(manifest.silver_version_id) / "manifest.json"
    document = json.loads(path.read_bytes())
    document.pop("selection_mode", None)
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))
    pipeline.storage._write_current(manifest.silver_version_id, path)
    return verify_current_bundle(pipeline.config.silver_root)


def test_mixed_identity_refresh_rename_and_restoration_reconcile_through_real_pipeline(
    silver_config,
    spark,
    tmp_path,
):
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    dbt_env = _dbt_environment(tmp_path, connection)
    warehouse_config = _warehouse_config(silver_config.silver_root, connection)
    loader = WarehouseLoader(warehouse_config)
    legacy_rows = [
        row(key, updated="2026-03-01T00:00:00.000" if key == "B" else "2026-01-02T00:00:00.000")
        for key in "ABCDE"
    ]
    write_bronze_run(silver_config, "legacy", legacy_rows, legacy=True)
    write_bronze_run(
        silver_config,
        "seed-current",
        [
            observed("F", supplier_code="invalid"),
            observed("G", "0", "N/A"),
        ],
    )
    original_bronze = file_hashes(silver_config.bronze_root)
    pipeline = SilverPipeline(
        silver_config, config_path=ROOT / "config/silver_quality.yaml", spark=spark
    )
    _drop_schemas()
    try:
        baseline = pipeline.run(("legacy", "seed-current"))
        assert baseline.status == "valid"
        base_bundle = historical_snapshot(pipeline, baseline)
        baseline_path = base_bundle.manifest_path
        baseline_bytes = file_hashes(baseline_path.parent)
        seed_registered(warehouse_config, base_bundle)
        assert loader.load(base_bundle).status == "noop"
        _dbt(dbt_env, "build", "--full-refresh")
        first_star = star_identity()
        assert set(first_star) == set("ABCDEFG")
        assert all(values[4] is None and values[5] is None for values in first_star.values())

        config = replace(
            load_ingestion_config(ROOT / "config/bronze_ingestion.yaml"),
            bronze_root=silver_config.bronze_root,
            page_size=2,
            max_rows=10,
            contract_path=ROOT / "contracts/secop_source.yaml",
        )
        contract = load_bronze_contract(config.contract_path, config.projection)
        metadata = {
            "id": contract.dataset_id,
            "columns": [
                {"fieldName": name, "dataTypeName": value["declared_type"]}
                for name, value in contract.columns.items()
            ],
        }
        pages = iter(
            [
                metadata,
                [
                    observed("A", "001", "100"),
                    observed("B", "003", "300", updated="2026-01-02T00:00:00.000"),
                ],
                [observed("C", "002", "200")],
                [observed("E", "001", "100", name=None)],
            ]
        )
        transport = SocrataTransport(
            config, contract, opener=lambda *a, **kw: Response(next(pages))
        )
        refresher = IdentityRefresh(
            config, contract, transport=transport, uuid_factory=lambda: "identity-refresh"
        )
        with refresher.state.connect() as database:
            database.execute(
                "INSERT INTO checkpoints VALUES (?, 'primary', ?, 'unchanged')",
                (contract.dataset_id, "2026-01-01T00:00:00Z"),
            )
        checkpoint = refresher.state.checkpoint(contract.dataset_id)
        refreshed = refresher.refresh(baseline_path, list("ABCDE"))
        assert refreshed.refresh["missing_ids"] == ["D"]
        assert refresher.state.checkpoint(contract.dataset_id) == checkpoint
        with pytest.raises(PublicationError, match="baseline Bronze dependencies"):
            pipeline.run((refreshed.run_id,))

        candidate = pipeline.run(("legacy", "seed-current", refreshed.run_id))
        assert candidate.refresh["business_changed_contracts"] == 0
        assert candidate.refresh["entity_identity_completed"] == 3
        assert {
            entry["source_contract_id"] for entry in candidate.refresh["changed_contracts"]
        } == {"A", "C", "E"}
        assert candidate.refresh["supplier_still_unknown"] == 4
        assert candidate.identity_coverage["entity"]["known_value_cop"] == "300.00"
        assert candidate.identity_coverage["supplier"]["unknown_count"] == 4
        current = historical_snapshot(pipeline, candidate)
        assert current.table.num_rows == 7
        assert sum(current.table["contract_value_cop"].to_pylist()) == Decimal("700.00")
        advance_registered(warehouse_config, current)
        assert loader.load(current).status == "noop"
        _dbt(dbt_env, "build")
        actual = star_identity()
        assert {key: values[:2] for key, values in actual.items()} == {
            key: values[:2] for key, values in first_star.items()
        }
        assert actual["A"][2:6] == actual["E"][2:6]
        assert actual["A"][2] != actual["C"][2] and actual["A"][3] != actual["C"][3]
        assert actual["E"][4:6] == ("001", "100")
        assert actual["B"][6:8] == ("LEGACY_PROJECTION", "LEGACY_PROJECTION")
        assert actual["D"][6:8] == ("LEGACY_PROJECTION", "LEGACY_PROJECTION")
        assert actual["F"][6:8] == ("MISSING_CODE", "INVALID_CODE")
        assert actual["G"][6:8] == ("PLACEHOLDER_CODE", "PLACEHOLDER_CODE")
        assert actual["A"][9:] == (refreshed.run_id, "secop-contracts-v2")
        assert actual["B"][9:] == ("legacy", "secop-contracts-v1")
        incremental_hashes = _logical_hashes()
        _dbt(dbt_env, "build", "--full-refresh")
        assert _logical_hashes() == incremental_hashes

        write_bronze_run(
            silver_config,
            "renamed",
            [
                observed(
                    "A",
                    "001",
                    "100",
                    name="Renamed label",
                    updated="2026-04-01T00:00:00.000",
                )
            ],
            fetched_at="2026-04-02T00:00:00.000Z",
        )
        renamed = pipeline.run(("legacy", "seed-current", refreshed.run_id, "renamed"))
        assert renamed.status == "valid"
        renamed_bundle = historical_snapshot(pipeline, renamed)
        advance_registered(warehouse_config, renamed_bundle)
        assert loader.load(renamed_bundle).status == "noop"
        _dbt(dbt_env, "build")
        after_rename = star_identity()
        assert after_rename["A"][:6] == actual["A"][:6]
        with psycopg.connect(TEST_DSN) as database:
            assert database.execute(
                "SELECT entity_name FROM analytics.dim_entity WHERE entity_code='001'"
            ).fetchone() == ("Renamed label",)
        assert all(
            file_hashes(silver_config.bronze_root)[key] == value
            for key, value in original_bronze.items()
        )
        assert file_hashes(baseline_path.parent) == baseline_bytes
        assert (
            loader.load(
                base_bundle,
                restore=True,
                expected_current=(renamed_bundle.version_id, renamed_bundle.manifest_sha256),
            ).status
            == "applied"
        )
        _dbt(dbt_env, "build")
        restored = star_identity()
        assert {key: values[:9] for key, values in restored.items()} == {
            key: values[:9] for key, values in first_star.items()
        }
        assert refresher.state.checkpoint(contract.dataset_id) == checkpoint
        # Transition a genuinely retained refresh graph into scoped acquisition.
        # The original refresh still has seven exact baseline keys; the outer
        # population may independently admit H after explicit legacy adoption.
        assert (
            loader.load(
                renamed_bundle,
                restore=True,
                expected_current=(base_bundle.version_id, base_bundle.manifest_sha256),
            ).status
            == "applied"
        )
        from processing_silver.test_population_pipeline import SCOPE, acquire

        record = adopt(
            pipeline.storage,
            renamed_bundle.version_id,
            renamed_bundle.manifest_sha256,
            SCOPE,
            bronze_root=config.bronze_root,
        )
        loader.adopt(renamed_bundle, record, bronze_root=config.bronze_root)
        campaign = acquire(
            silver_config,
            "population-after-refresh",
            [[row(key) for key in "ABCDEFGH"], [row(key) for key in "ABCDEFG"]],
            baseline=renamed_bundle.manifest_path,
            day=4,
        )
        population = pipeline.run_population(
            SCOPE,
            (campaign.run_id,),
            baseline_version_id=renamed_bundle.version_id,
            baseline_manifest_sha256=renamed_bundle.manifest_sha256,
            mode="authoritative_candidate",
        )
        assert population.critical_passed
        scoped = verify_current_bundle(silver_config.silver_root)
        assert scoped.row_count == 8
        assert len(scoped.reconciliation["historical_refresh_nodes"]) > 0
        assert loader.load(scoped).status == "applied"
        _dbt(dbt_env, "build")
        assert set(star_identity()) == set("ABCDEFGH")
        assert sum(values[8] for values in star_identity().values()) == Decimal("800.00")
        assert refresher.state.checkpoint(contract.dataset_id) == checkpoint
    finally:
        _drop_schemas()
