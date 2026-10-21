from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import psycopg
import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import population_key
from .conftest import population_row as silver_row
from .conftest import write_population_bundle as write_bundle

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN is not configured")


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

    first_bundle = verify_current_bundle(root)
    first = loader.load(first_bundle)
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
            silver_row("version-2", "contract-2", contract_id="CO1.PCCNTR.2"),
            silver_row("version-2", "contract-3", contract_id="CO1.PCCNTR.3"),
        ],
    )
    second_bundle = verify_current_bundle(root)
    second = loader.load(second_bundle)

    assert (second.inserted_rows, second.updated_rows, second.deleted_rows) == (1, 2, 0)
    with psycopg.connect(TEST_DSN) as database:
        rows = database.execute(
            f"SELECT contract_key, contract_value_cop "
            f'FROM "{postgres_config.schema}".contract_current ORDER BY 1'
        ).fetchall()
        audits = database.execute(
            f'SELECT count(*) FROM "{postgres_config.schema}".load_audit'
        ).fetchone()[0]
    assert rows == sorted(
        [
            (population_key("contract-1"), 125),
            (population_key("contract-2"), 100),
            (population_key("contract-3"), 100),
        ]
    )
    assert audits == 2
    restored = loader.load(
        first_bundle,
        restore=True,
        expected_current=(second_bundle.version_id, second_bundle.manifest_sha256),
    )
    assert restored.deleted_rows == 1

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
