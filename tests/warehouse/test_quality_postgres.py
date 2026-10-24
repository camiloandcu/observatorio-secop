"""Immutable quality evidence shares snapshot no-op, recovery and rollback."""

import os
from dataclasses import replace

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.errors import WarehouseConflictError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import population_row, write_population_bundle

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN is not configured")


def test_quality_immutable_noop_and_failed_restore_are_atomic(postgres_config):
    root = postgres_config.silver_root
    loader = WarehouseLoader(postgres_config)
    write_population_bundle(root, "quality-a", [population_row("quality-a", "A")])
    a = verify_current_bundle(root)
    loader.load(a)
    assert loader.load(a).status == "noop"
    relation = sql.Identifier(postgres_config.schema, "contract_current_quality")
    with psycopg.connect(TEST_DSN) as database:
        original = database.execute(sql.SQL("SELECT * FROM {}").format(relation)).fetchall()
    assert original[0][3]["state"] == "UNAVAILABLE_LEGACY"
    # Verified identity cannot be reused with different quality even on the no-op path.
    mutated = replace(a, quality_summary={**a.quality_summary, "reason": "changed"})
    with pytest.raises(WarehouseConflictError, match="quality evidence"):
        loader.load(mutated)
    write_population_bundle(root, "quality-b", [population_row("quality-b", "A", amount="200.00")])
    b = verify_current_bundle(root)
    loader.load(b)
    # A restoration failing immutable evidence must leave B active and untouched.
    with pytest.raises(WarehouseConflictError, match="quality evidence"):
        loader.load(mutated, restore=True, expected_current=(b.version_id, b.manifest_sha256))
    with psycopg.connect(TEST_DSN) as database:
        assert database.execute(
            sql.SQL("SELECT silver_version_id FROM {}.contract_current").format(
                sql.Identifier(postgres_config.schema)
            )
        ).fetchall() == [(b.version_id,)]
        assert (
            database.execute(
                sql.SQL("SELECT * FROM {} WHERE silver_version_id=%s").format(relation),
                (a.version_id,),
            ).fetchall()
            == original
        )
    assert (
        loader.load(a, restore=True, expected_current=(b.version_id, b.manifest_sha256)).status
        == "applied"
    )
    assert loader.load(a).status == "noop"
