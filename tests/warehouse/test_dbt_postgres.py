from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import silver_row, write_bundle

ROOT = Path(__file__).resolve().parents[2]
TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN is not configured")
STAR_TABLES = (
    "dim_entity",
    "dim_supplier",
    "dim_municipality",
    "dim_modality",
    "dim_date",
    "fact_contract",
)


def test_incremental_full_refresh_quality_and_docs_are_reproducible(tmp_path: Path) -> None:
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    environment = _dbt_environment(tmp_path, connection)
    config = _warehouse_config(tmp_path / "silver", connection)
    _drop_schemas()
    try:
        write_bundle(
            config.silver_root,
            "version-1",
            [
                silver_row("version-1", "contract-1"),
                silver_row(
                    "version-1",
                    "contract-2",
                    contract_id="CO1.PCCNTR.2",
                    municipality_status="UNKNOWN",
                    municipality_key=None,
                    municipality_name=None,
                    entity_name=None,
                    procurement_method=None,
                ),
            ],
        )
        loader = WarehouseLoader(config)
        assert loader.load(verify_current_bundle(config.silver_root)).status == "applied"

        _dbt(environment, "build")
        first_hashes = _logical_hashes()
        assert loader.load(verify_current_bundle(config.silver_root)).status == "noop"
        _dbt(environment, "build")
        assert _logical_hashes() == first_hashes
        _assert_star_contract()

        write_bundle(
            config.silver_root,
            "version-2",
            [
                silver_row("version-2", "contract-1", amount="125.00"),
                silver_row(
                    "version-2",
                    "contract-3",
                    contract_id="CO1.PCCNTR.3",
                    entity_name="Otra entidad",
                ),
            ],
        )
        result = loader.load(verify_current_bundle(config.silver_root))
        assert (result.inserted_rows, result.updated_rows, result.deleted_rows) == (1, 1, 1)
        _dbt(environment, "build")
        incremental_hashes = _logical_hashes()

        _dbt(environment, "build", "--full-refresh")
        assert _logical_hashes() == incremental_hashes

        with psycopg.connect(TEST_DSN, autocommit=True) as database:
            database.execute("DELETE FROM analytics.dim_modality WHERE member_status = 'KNOWN'")
        failed_relationship = _dbt(environment, "test", "--select", "fact_contract", check=False)
        assert failed_relationship.returncode != 0
        _dbt(environment, "build", "--full-refresh")

        with psycopg.connect(TEST_DSN, autocommit=True) as database:
            database.execute(
                "UPDATE analytics.fact_contract SET contract_id = NULL "
                "WHERE contract_key = (SELECT min(contract_key) FROM analytics.fact_contract)"
            )
        failed_null = _dbt(environment, "test", "--select", "fact_contract", check=False)
        assert failed_null.returncode != 0
        _dbt(environment, "build", "--full-refresh")

        with psycopg.connect(TEST_DSN, autocommit=True) as database:
            database.execute(
                "UPDATE analytics.fact_contract "
                "SET contract_id = (SELECT min(contract_id) FROM analytics.fact_contract) "
                "WHERE contract_id = (SELECT max(contract_id) FROM analytics.fact_contract)"
            )
        failed_duplicate = _dbt(environment, "test", "--select", "fact_contract", check=False)
        assert failed_duplicate.returncode != 0
        _dbt(environment, "build", "--full-refresh")

        with psycopg.connect(TEST_DSN, autocommit=True) as database:
            database.execute(
                "INSERT INTO analytics.dim_entity "
                "SELECT md5(entity_sk), entity_natural_key, source_dataset_id, "
                "entity_name, member_status "
                "FROM analytics.dim_entity WHERE member_status = 'KNOWN' LIMIT 1"
            )
        failed_collision = _dbt(environment, "test", "--select", "dim_entity", check=False)
        assert failed_collision.returncode != 0
        _dbt(environment, "build", "--full-refresh")

        with psycopg.connect(TEST_DSN, autocommit=True) as database:
            database.execute("UPDATE analytics.dim_supplier SET member_status = 'INVALID'")
        failed_value = _dbt(environment, "test", "--select", "dim_supplier", check=False)
        assert failed_value.returncode != 0
        _dbt(environment, "build", "--full-refresh")

        _dbt(environment, "docs", "generate")
        assert (ROOT / "dbt/target/manifest.json").is_file()
        assert (ROOT / "dbt/target/catalog.json").is_file()
    finally:
        _drop_schemas()


def _warehouse_config(root: Path, connection: dict[str, Any]) -> WarehouseConfig:
    return WarehouseConfig(
        silver_root=root,
        schema="silver",
        contract_table="contract_current",
        audit_table="load_audit",
        advisory_lock_id=7404001,
        host=connection.get("host", "127.0.0.1"),
        port=int(connection.get("port", "5432")),
        dbname=connection.get("dbname", "postgres"),
        user=connection.get("user", "postgres"),
        password=connection.get("password", ""),
    )


def _dbt_environment(tmp_path: Path, connection: dict[str, Any]) -> dict[str, str]:
    profile = {
        "observatorio_secop": {
            "target": "test",
            "outputs": {
                "test": {
                    "type": "postgres",
                    "host": connection.get("host", "127.0.0.1"),
                    "port": int(connection.get("port", "5432")),
                    "user": connection.get("user", "postgres"),
                    "password": connection.get("password", ""),
                    "dbname": connection.get("dbname", "postgres"),
                    "schema": "analytics",
                    "threads": 2,
                }
            },
        }
    }
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(yaml.safe_dump(profile), encoding="utf-8")
    environment = os.environ.copy()
    environment["DBT_TEST_PROFILES_DIR"] = str(profiles)
    return environment


def _dbt(
    environment: dict[str, str], *arguments: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    executable = shutil.which("dbt")
    assert executable, f"dbt is unavailable under {sys.executable}"
    result = subprocess.run(
        [
            executable,
            *arguments,
            "--no-partial-parse",
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            environment["DBT_TEST_PROFILES_DIR"],
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode:
        pytest.fail(f"dbt {' '.join(arguments)} failed:\n{result.stdout}\n{result.stderr}")
    return result


def _logical_hashes() -> dict[str, str]:
    assert TEST_DSN
    result = {}
    with psycopg.connect(TEST_DSN) as database:
        for table in STAR_TABLES:
            rows = database.execute(f"SELECT * FROM analytics.{table} ORDER BY 1").fetchall()
            content = json.dumps(rows, default=str, ensure_ascii=False, separators=(",", ":"))
            result[table] = hashlib.sha256(content.encode()).hexdigest()
    return result


def _assert_star_contract() -> None:
    assert TEST_DSN
    with psycopg.connect(TEST_DSN) as database:
        fact = database.execute(
            "SELECT count(*), count(distinct contract_sk), count(source_url), "
            "count(silver_version_id), count(loaded_at) FROM analytics.fact_contract"
        ).fetchone()
        unknown_supplier = database.execute(
            "SELECT count(*) FROM analytics.dim_supplier "
            "WHERE supplier_natural_key = '__UNKNOWN__' AND member_status = 'UNKNOWN'"
        ).fetchone()[0]
        unknown_municipality = database.execute(
            "SELECT count(*) FROM analytics.dim_municipality "
            "WHERE municipality_natural_key = '__UNKNOWN__'"
        ).fetchone()[0]
    assert fact == (2, 2, 2, 2, 2)
    assert unknown_supplier == 1
    assert unknown_municipality == 1


def _drop_schemas() -> None:
    assert TEST_DSN
    with psycopg.connect(TEST_DSN, autocommit=True) as database:
        for schema in ("analytics", "intermediate", "staging", "silver"):
            database.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
