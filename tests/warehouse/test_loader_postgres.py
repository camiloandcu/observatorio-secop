from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import silver_row, write_bundle

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN is not configured")


@pytest.fixture
def postgres_config(tmp_path: Path) -> WarehouseConfig:
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    schema = f"silver_test_{uuid.uuid4().hex[:12]}"
    config = WarehouseConfig(
        silver_root=tmp_path / "silver",
        schema=schema,
        contract_table="contract_current",
        audit_table="load_audit",
        advisory_lock_id=7404999,
        host=connection.get("host", "127.0.0.1"),
        port=int(connection.get("port", "5432")),
        dbname=connection.get("dbname", "postgres"),
        user=connection.get("user", "postgres"),
        password=connection.get("password", ""),
    )
    yield config
    with psycopg.connect(TEST_DSN, autocommit=True) as database:
        database.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_load_repeat_correction_removal_and_conflict(postgres_config: WarehouseConfig) -> None:
    root = postgres_config.silver_root
    write_bundle(
        root,
        "version-1",
        [
            silver_row("version-1", "contract-1", amount="100.00"),
            silver_row("version-1", "contract-2", contract_id="CO1.PCCNTR.2"),
        ],
    )
    loader = WarehouseLoader(postgres_config)

    first = loader.load(verify_current_bundle(root))
    repeated = loader.load(verify_current_bundle(root))

    assert (first.status, first.inserted_rows, first.updated_rows, first.deleted_rows) == (
        "applied",
        2,
        0,
        0,
    )
    assert repeated.status == "noop"

    write_bundle(
        root,
        "version-2",
        [
            silver_row("version-2", "contract-1", amount="125.00"),
            silver_row("version-2", "contract-3", contract_id="CO1.PCCNTR.3"),
        ],
    )
    second_bundle = verify_current_bundle(root)
    second = loader.load(second_bundle)

    assert (second.inserted_rows, second.updated_rows, second.deleted_rows) == (1, 1, 1)
    with psycopg.connect(TEST_DSN) as database:
        rows = database.execute(
            f"SELECT contract_key, contract_value_cop "
            f'FROM "{postgres_config.schema}".contract_current ORDER BY 1'
        ).fetchall()
        audits = database.execute(
            f'SELECT count(*) FROM "{postgres_config.schema}".load_audit'
        ).fetchone()[0]
    assert rows == [("contract-1", 125), ("contract-3", 100)]
    assert audits == 2

    with pytest.raises(WarehouseConflictError):
        loader.load(replace(second_bundle, manifest_sha256="f" * 64))


def test_failed_transaction_rolls_back_and_retry_succeeds(
    postgres_config: WarehouseConfig,
) -> None:
    root = postgres_config.silver_root
    write_bundle(root, "version-1", [silver_row("version-1")])
    loader = WarehouseLoader(postgres_config)
    loader.load(verify_current_bundle(root))
    write_bundle(root, "version-2", [silver_row("version-2", amount="200.00")])
    bundle = verify_current_bundle(root)

    with psycopg.connect(TEST_DSN, autocommit=True) as database:
        database.execute(
            f"""
            CREATE FUNCTION "{postgres_config.schema}".reject_version() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'induced audit failure'; END $$
            """
        )
        database.execute(
            f"""
            CREATE TRIGGER reject_version BEFORE INSERT ON "{postgres_config.schema}".load_audit
            FOR EACH ROW EXECUTE FUNCTION "{postgres_config.schema}".reject_version()
            """
        )

    with pytest.raises(WarehouseLoadError):
        loader.load(bundle)
    with psycopg.connect(TEST_DSN) as database:
        value = database.execute(
            f'SELECT contract_value_cop FROM "{postgres_config.schema}".contract_current'
        ).fetchone()[0]
    assert value == 100

    with psycopg.connect(TEST_DSN, autocommit=True) as database:
        database.execute(f'DROP TRIGGER reject_version ON "{postgres_config.schema}".load_audit')
    assert loader.load(bundle).status == "applied"


def test_concurrent_repeat_is_serialized(postgres_config: WarehouseConfig) -> None:
    root = postgres_config.silver_root
    write_bundle(root, "version-1", [silver_row("version-1")])
    bundle = verify_current_bundle(root)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(lambda _: WarehouseLoader(postgres_config).load(bundle), range(2))
        )

    assert sorted(result.status for result in results) == ["applied", "noop"]
    with psycopg.connect(TEST_DSN) as database:
        assert (
            database.execute(
                f'SELECT count(*) FROM "{postgres_config.schema}".contract_current'
            ).fetchone()[0]
            == 1
        )
