"""Population evidence shares activation's transactional and immutable boundary."""

from dataclasses import replace

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import population_row, write_population_bundle


def snapshot(config):
    names = [
        "contract_current",
        "load_audit",
        "load_audit_active",
        "load_audit_activation",
        "contract_current_population",
        "contract_current_population_binding",
        "contract_current_observation_condition",
        "contract_current_observation_link",
    ]
    with psycopg.connect(**config.connection_kwargs) as connection:
        return {
            name: connection.execute(
                sql.SQL("SELECT to_jsonb(t) FROM {} t ORDER BY 1").format(
                    sql.Identifier(config.schema, name)
                )
            ).fetchall()
            for name in names
        }


def test_last_condition_write_failure_reverts_all_activation_effects(postgres_config):
    config = postgres_config
    root = config.silver_root
    loader = WarehouseLoader(config)
    write_population_bundle(root, "A", [population_row("A")])
    a = verify_current_bundle(root)
    loader.load(a)
    before = snapshot(config)
    write_population_bundle(root, "B", [population_row("B", amount="200.00")])
    b = verify_current_bundle(root)
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            sql.SQL(
                "CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS "
                "$$ BEGIN RAISE EXCEPTION 'induced condition failure'; END $$"
            ).format(sql.Identifier(config.schema, "reject_condition"))
        )
        connection.execute(
            sql.SQL(
                "CREATE TRIGGER reject_condition BEFORE INSERT ON {} "
                "FOR EACH ROW EXECUTE FUNCTION {}()"
            ).format(
                sql.Identifier(config.schema, "contract_current_observation_condition"),
                sql.Identifier(config.schema, "reject_condition"),
            )
        )
    with pytest.raises(WarehouseLoadError):
        loader.load(b)
    assert snapshot(config) == before
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            sql.SQL("DROP TRIGGER reject_condition ON {}").format(
                sql.Identifier(config.schema, "contract_current_observation_condition")
            )
        )
    assert loader.load(b).status == "applied"
    assert loader.load(b).status == "noop"
    assert (
        loader.load(a, restore=True, expected_current=(b.version_id, b.manifest_sha256)).status
        == "applied"
    )
    assert snapshot(config)["contract_current"] == before["contract_current"]


def test_modified_evidence_blocks_exact_active_retry_and_preserves_rows(postgres_config):
    config = postgres_config
    write_population_bundle(config.silver_root, "A", [population_row("A")])
    a = verify_current_bundle(config.silver_root)
    loader = WarehouseLoader(config)
    loader.load(a)
    with psycopg.connect(**config.connection_kwargs) as connection:
        connection.execute(
            sql.SQL("UPDATE {} SET condition = condition || '{{\"tampered\":true}}'::jsonb").format(
                sql.Identifier(config.schema, "contract_current_observation_condition")
            )
        )
    before = snapshot(config)
    with pytest.raises(WarehouseConflictError, match="evidence differs"):
        loader.load(a)
    assert snapshot(config) == before


def test_equal_count_different_keys_candidate_does_not_replace_population(postgres_config):
    config = postgres_config
    write_population_bundle(config.silver_root, "A", [population_row("A")])
    a = verify_current_bundle(config.silver_root)
    loader = WarehouseLoader(config)
    loader.load(a)
    before = snapshot(config)
    # The SQL guard independently checks actual active keys, beyond bundle verification.
    changed = population_row("B", "other", contract_id="CO1.PCCNTR.other")
    table = a.table.from_pylist([changed], schema=a.table.schema)
    coverage = dict(
        a.coverage,
        silver_version_id="B",
        expected_parent={
            "silver_version_id": a.version_id,
            "manifest_sha256": a.manifest_sha256,
        },
        baseline={
            "silver_version_id": a.version_id,
            "manifest_sha256": a.manifest_sha256,
            "keys_sha256": a.coverage["candidate_keys_sha256"],
        },
        baseline_keys_sha256=a.coverage["candidate_keys_sha256"],
    )
    b = replace(a, version_id="B", manifest_sha256="b" * 64, coverage=coverage, table=table)
    coverage["acquisitions"] = [
        dict(item, baseline=coverage["baseline"]) for item in coverage["acquisitions"]
    ]
    with pytest.raises(WarehouseConflictError, match="preserve"):
        loader.load(b)
    assert snapshot(config) == before
