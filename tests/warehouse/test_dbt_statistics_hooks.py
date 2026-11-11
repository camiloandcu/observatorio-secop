"""Dependent dbt models must see statistics before automatic analyze can run."""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from pathlib import Path

import psycopg
import pytest
import yaml
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

ROOT = Path(__file__).resolve().parents[2]


def project_config():
    config = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text())
    return config["models"]["observatorio_secop"]


def test_statistics_hooks_are_scoped_to_dimension_and_fact_models():
    config = project_config()
    for category in ["dimensions", "facts"]:
        assert config[category]["+post-hook"] == "ANALYZE {{ this }}"
    for category in ["staging", "intermediate", "marts"]:
        assert "+post-hook" not in config[category]


@pytest.mark.skipif(
    not os.environ.get("WAREHOUSE_TEST_DSN"), reason="Requires dedicated disposable PostgreSQL"
)
def test_actual_dbt_hooks_publish_statistics_before_dependents(tmp_path):
    connection = conninfo_to_dict(os.environ["WAREHOUSE_TEST_DSN"])
    schema = "us09_stats_" + uuid.uuid4().hex[:12]
    analytics = schema + "_analytics"
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    profile = {
        "stats_probe": {
            "target": "test",
            "outputs": {
                "test": {
                    "type": "postgres",
                    "host": connection.get("host", "127.0.0.1"),
                    "port": int(connection.get("port", 5432)),
                    "dbname": connection["dbname"],
                    "user": connection["user"],
                    "password": connection.get("password", ""),
                    "schema": schema,
                    "threads": 1,
                }
            },
        },
    }
    (profiles / "profiles.yml").write_text(yaml.safe_dump(profile))
    (profiles / "profiles.yml").chmod(0o600)
    actual_config = project_config()
    config = {
        "name": "observatorio_secop",
        "version": "1.0",
        "config-version": 2,
        "profile": "stats_probe",
        "model-paths": ["models"],
        "models": {
            "observatorio_secop": {
                name: actual_config[name] for name in ["dimensions", "facts", "marts"]
            }
        },
    }
    (tmp_path / "dbt_project.yml").write_text(yaml.safe_dump(config))
    for folder in ["dimensions", "facts", "marts"]:
        (tmp_path / "models" / folder).mkdir(parents=True)
    (tmp_path / "models/dimensions/dim_supplier.sql").write_text(
        "SELECT n::text AS supplier_sk FROM generate_series(1,1200) n\n"
    )
    # Fail at dependency execution, rather than merely inspect after run-end.
    # A freshly built relation has no pg_stats until its post-hook ANALYZE.
    (tmp_path / "models/facts/fact_contract.sql").write_text("""
SELECT supplier_sk AS contract_key, supplier_sk
FROM {{ ref('dim_supplier') }}
WHERE 1 / CASE WHEN EXISTS (
 SELECT 1 FROM pg_stats WHERE schemaname='{{ this.schema }}'
 AND tablename='dim_supplier' AND attname='supplier_sk' AND n_distinct <> 0
) THEN 1 ELSE 0 END = 1
""")
    (tmp_path / "models/marts/statistics_gate.sql").write_text("""
SELECT count(*) AS contracts,
 1 / CASE WHEN EXISTS (
  SELECT 1 FROM pg_stats WHERE schemaname='{{ this.schema }}'
  AND tablename='fact_contract' AND attname='contract_key' AND n_distinct <> 0
 ) THEN 1 ELSE 0 END AS statistics_ready
FROM {{ ref('fact_contract') }}
""")
    executable = shutil.which("dbt") or str(Path(__import__("sys").executable).parent / "dbt")
    try:
        result = subprocess.run(
            [
                executable,
                "run",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(profiles),
                "--no-use-colors",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout[-10000:] + result.stderr[-1000:]
        with psycopg.connect(os.environ["WAREHOUSE_TEST_DSN"]) as database:
            value = database.execute(
                sql.SQL("SELECT contracts,statistics_ready FROM {}.statistics_gate").format(
                    sql.Identifier(analytics)
                )
            ).fetchone()
            assert value == (1200, 1)
            analyzed = database.execute(
                """SELECT tablename,attname FROM pg_stats
                WHERE schemaname=%s AND tablename IN ('dim_supplier','fact_contract')""",
                (analytics,),
            ).fetchall()
            assert ("dim_supplier", "supplier_sk") in analyzed
            assert ("fact_contract", "contract_key") in analyzed
    finally:
        with psycopg.connect(os.environ["WAREHOUSE_TEST_DSN"], autocommit=True) as database:
            for owned in [analytics, schema]:
                database.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(owned))
                )
