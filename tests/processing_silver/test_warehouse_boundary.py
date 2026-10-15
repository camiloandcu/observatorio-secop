from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
import pytest
import yaml
from conftest import ROOT, row, write_bronze_run
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.processing.silver import runner
from observatorio_secop.processing.silver.config import load_silver_config
from observatorio_secop.processing.silver.models import SilverManifest
from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig, load_warehouse_config
from observatorio_secop.warehouse.loader import WarehouseLoader

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")


class LegacyWriter(SilverPipeline):
    def _configure_spark(self, spark):
        super()._configure_spark(spark)
        spark.conf.set("spark.sql.parquet.outputTimestampType", "INT96")


def hashes(directory: Path) -> dict[str, str]:
    return {
        str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in directory.rglob("*")
        if p.is_file()
    }


@pytest.fixture(scope="module", params=["default", "exported"])
def publication(request, tmp_path_factory, spark):
    work = tmp_path_factory.mktemp("silver-warehouse")
    document = yaml.safe_load((ROOT / "config/silver_quality.yaml").read_text())
    for field in ["contract_path", "territory_catalog_path"]:
        document["silver"][field] = str(ROOT / document["silver"][field])
    document["silver"]["local_threads"] = 1
    document["silver"]["max_rejection_rate"] = 0.5
    config_path = work / "silver.yaml"
    config_path.write_text(yaml.safe_dump(document))
    with pytest.MonkeyPatch.context() as environment:
        environment.chdir(work)
        environment.delenv("SILVER_ROOT", raising=False)
        if request.param == "exported":
            environment.setenv("SILVER_ROOT", str(work / "explicit"))
        config = load_silver_config(config_path)
        database_environment = {
            "POSTGRES_DB": "test",
            "POSTGRES_USER": "test",
            "POSTGRES_PASSWORD": "test-only",
        }
        if request.param == "exported":
            database_environment["SILVER_ROOT"] = str(config.silver_root)
        warehouse = load_warehouse_config(
            ROOT / "config/warehouse.yaml", environment=database_environment
        )
        assert config.silver_root.resolve() == warehouse.silver_root.resolve()
        # The shared Spark JVM was started before this temporary chdir. Give it
        # absolute test paths after checking the real relative-root contract.
        config = replace(
            config,
            bronze_root=config.bronze_root.resolve(),
            silver_root=config.silver_root.resolve(),
        )
        warehouse = replace(warehouse, silver_root=warehouse.silver_root.resolve())
        write_bronze_run(
            config,
            "source-1",
            [
                row("A", updated="2026-01-02T12:34:56.123", amount="123.450000"),
                row("B", updated=None, signing=None, amount="0.00"),
                row("REJECTED", amount="1.001"),
            ],
            fetched_at="2026-01-03T15:16:17.456Z",
        )
        with pytest.MonkeyPatch.context() as legacy_profile:
            legacy_profile.setattr(runner, "WRITER_FORMAT_VERSION", "silver-parquet-v1")
            old = LegacyWriter(config, config_path=config_path, spark=spark).run()
        legacy_directory = config.silver_root / "versions" / old.silver_version_id
        legacy_manifest = legacy_directory / "manifest.json"
        old_document = json.loads(legacy_manifest.read_text())
        old_document.pop("writer_format_version")
        legacy_manifest.write_text(json.dumps(old_document, sort_keys=True))
        pointer_path = config.silver_root / "current.json"
        pointer = json.loads(pointer_path.read_text())
        pointer["manifest_sha256"] = hashlib.sha256(legacy_manifest.read_bytes()).hexdigest()
        pointer_path.write_text(json.dumps(pointer))
        legacy_hashes = hashes(legacy_directory)
        legacy_bundle = verify_current_bundle(warehouse.silver_root)
        assert SilverManifest.from_dict(old_document).writer_format_version is None
        pipeline = SilverPipeline(config, config_path=config_path, spark=spark)
        current = pipeline.run()
        repeated = pipeline.run()
        assert current.silver_version_id != old.silver_version_id
        assert repeated.silver_version_id == current.silver_version_id
        assert hashes(legacy_directory) == legacy_hashes
        yield config, current, verify_current_bundle(warehouse.silver_root), legacy_bundle


def test_real_publication_has_microseconds_and_exact_logical_values(publication) -> None:
    config, manifest, bundle, legacy_bundle = publication
    version = config.silver_root / "versions" / manifest.silver_version_id
    assert manifest.writer_format_version == "silver-parquet-v2"
    assert manifest.counts["rejected_rows"] == 1
    for path in list((version / "data").rglob("*.parquet")) + list(
        (version / "rejected").rglob("*.parquet")
    ):
        schema = pq.ParquetFile(path).schema
        for column in schema:
            if column.path in {"source_updated_at", "bronze_fetched_at_utc"}:
                assert column.physical_type == "INT64"
                assert "microseconds" in str(column.logical_type).lower()
    rows = sorted(bundle.table.to_pylist(), key=lambda item: item["source_contract_id"])
    assert rows[0]["source_updated_at"] == datetime(2026, 1, 2, 12, 34, 56, 123000)
    assert rows[0]["source_updated_at_raw"] == "2026-01-02T12:34:56.123"
    assert rows[0]["bronze_fetched_at_utc"] == datetime(2026, 1, 3, 15, 16, 17, 456000)
    assert rows[0]["contract_value_cop"] == Decimal("123.45")
    assert rows[0]["signing_date"] == date(2026, 1, 1)
    assert rows[1]["source_updated_at"] is None
    assert rows[1]["signing_year"] == "unknown"
    assert rows[1]["signing_date"] is None
    for current, legacy in zip(
        rows,
        sorted(legacy_bundle.table.to_pylist(), key=lambda r: r["source_contract_id"]),
        strict=True,
    ):
        assert {k: v for k, v in current.items() if k != "silver_version_id"} == {
            k: v for k, v in legacy.items() if k != "silver_version_id"
        }


@pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN must select a disposable database")
def test_actual_spark_and_legacy_output_load_and_repeat_without_changes(publication) -> None:
    assert TEST_DSN
    _, _, new_bundle, legacy_bundle = publication
    connection = conninfo_to_dict(TEST_DSN)
    for bundle in [new_bundle, legacy_bundle]:
        schema = "boundary_test_" + uuid.uuid4().hex[:12]
        config = WarehouseConfig(
            silver_root=bundle.manifest_path.parents[2],
            schema=schema,
            contract_table="contract_current",
            audit_table="load_audit",
            advisory_lock_id=7404997,
            host=connection.get("host", "127.0.0.1"),
            port=int(connection.get("port", "5432")),
            dbname=connection.get("dbname", "postgres"),
            user=connection.get("user", "postgres"),
            password=connection.get("password", ""),
        )
        before = hashes(bundle.manifest_path.parent)
        try:
            loader = WarehouseLoader(config)
            assert loader.load(bundle).inserted_rows == 2
            assert loader.load(bundle).status == "noop"
            with psycopg.connect(TEST_DSN) as database:
                cursor = database.execute(
                    sql.SQL(
                        "SELECT {} FROM {}.contract_current ORDER BY source_contract_id"
                    ).format(
                        sql.SQL(",").join(map(sql.Identifier, bundle.table.column_names)),
                        sql.Identifier(schema),
                    )
                )
                actual = cursor.fetchall()
            expected = sorted(bundle.table.to_pylist(), key=lambda r: r["source_contract_id"])
            assert actual == [
                tuple(row[name] for name in bundle.table.column_names) for row in expected
            ]
            assert hashes(bundle.manifest_path.parent) == before
        finally:
            with psycopg.connect(TEST_DSN, autocommit=True) as database:
                database.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
