"""Documented local CLI commands exercise the real isolated publication lifecycle."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from decimal import Decimal

import pytest
import yaml
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import population_row, write_population_bundle
from .test_publication_acceptance import BUDGET, TEST_DSN
from .test_publication_acceptance import publication_database as _publication_database

pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires disposable PostgreSQL/dbt")


@pytest.fixture
def cli_database(tmp_path):
    yield from _publication_database.__wrapped__(tmp_path)


def test_documented_cli_build_activate_resolve_read_query_and_safe_failures(cli_database):
    config, store, root = cli_database
    assert TEST_DSN
    source = conninfo_to_dict(TEST_DSN)
    environment = os.environ | {
        "POSTGRES_HOST": config.host,
        "POSTGRES_PORT": str(config.port),
        "POSTGRES_DB": config.dbname,
        "POSTGRES_USER": config.user,
        "POSTGRES_PASSWORD": config.password,
    }
    config_path = root / "warehouse.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "warehouse": {
                    "silver_root": str(config.silver_root),
                    "schema": config.schema,
                    "contract_table": config.contract_table,
                    "audit_table": config.audit_table,
                    "advisory_lock_id": config.advisory_lock_id,
                }
            }
        )
    )

    def invoke(*arguments, expected=0):
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "observatorio_secop.publication.cli",
                "--config",
                str(config_path),
                *map(str, arguments),
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        assert process.returncode == expected, process.stdout + process.stderr
        assert source["password"] not in process.stdout + process.stderr
        return json.loads(process.stdout if expected == 0 else process.stderr)

    assert invoke("bootstrap")["status"] == "ready"
    assert invoke("inspect")["active"]["publication_id"] is None
    write_population_bundle(
        config.silver_root, "cli-source", [population_row("cli-source", "one", amount="123.45")]
    )
    bundle = verify_current_bundle(config.silver_root)
    WarehouseLoader(config).load(bundle)
    built = invoke(
        "build",
        "--version",
        bundle.version_id,
        "--manifest-sha256",
        bundle.manifest_sha256,
        "--artifact-root",
        root / "build-artifacts",
        "--timeout-seconds",
        "120",
        "--storage-budget-bytes",
        BUDGET,
    )
    assert built["state"] == "sealed"
    value = built["publication_id"]
    activated = invoke("activate", "--publication-id", value, "--expected-generation", "0")
    assert activated["status"] == "activated"
    descriptor = invoke("resolve")
    assert descriptor["publication_id"] == value
    descriptor_path = root / "descriptor.json"
    descriptor_path.write_text(json.dumps(descriptor))
    assert invoke("validate-descriptor", "--descriptor", descriptor_path) == descriptor
    rows = invoke("read", "--descriptor", descriptor_path, "--relation", "fact_contract")
    assert len(rows) == 1
    assert Decimal(rows[0]["contract_value_cop"]) == Decimal("123.45")
    overview = invoke("query", "--descriptor", descriptor_path, "--metric", "overview")
    assert overview[0]["publication_id"] == value
    assert overview[0]["contract_count"] == 1
    assert Decimal(overview[0]["contract_value_cop"]) == Decimal("123.45")
    assert (
        invoke("activate", "--publication-id", value, "--expected-generation", "0")["status"]
        == "noop"
    )
    assert invoke("cleanup-preview") == []
    before = store.inspect()
    invalid = invoke("inspect", "--publication-id", "../candidate", expected=2)
    assert invalid["code"] == "PUBLICATION_INVALID"
    invalid = invoke(
        "build",
        "--version",
        bundle.version_id,
        "--manifest-sha256",
        bundle.manifest_sha256,
        "--timeout-seconds",
        "0",
        "--storage-budget-bytes",
        BUDGET,
        expected=2,
    )
    assert invalid["code"] == "PUBLICATION_INVALID"
    assert store.inspect() == before
