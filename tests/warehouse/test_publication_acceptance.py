"""Independent publication acceptance through verified inputs, dbt and real roles."""

from __future__ import annotations

import copy
import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.publication.build import run_candidate_build
from observatorio_secop.publication.capture import capture_inputs
from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.models import OUTPUT_RELATIONS
from observatorio_secop.publication.queries import query_metric
from observatorio_secop.publication.readers import (
    read_publication,
    resolve,
    scoring_compatibility,
    validate_descriptor,
)
from observatorio_secop.publication.store import PublicationStore
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import identity_row, population_row, write_identity_bundle, write_population_bundle
from .snapshot_helpers import seed_registered
from .test_dbt_postgres import _warehouse_config

TEST_DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires disposable PostgreSQL and dbt")
BUDGET = 64 * 1024 * 1024


def _drop_owned_test_schemas():
    """This suite owns the entire dedicated disposable database's test schemas."""
    assert TEST_DSN
    with psycopg.connect(TEST_DSN, autocommit=True) as database:
        schemas = database.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname IN "
            "('published','publication_control','analytics','staging','intermediate','silver') "
            "OR nspname LIKE 'candidate_%' OR nspname LIKE 'publication_%'"
        ).fetchall()
        for (schema,) in schemas:
            database.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def publication_database(tmp_path):
    assert TEST_DSN
    _drop_owned_test_schemas()
    config = _warehouse_config(tmp_path / "silver", conninfo_to_dict(TEST_DSN))
    store = PublicationStore(config)
    store.bootstrap()
    try:
        yield config, store, tmp_path
    finally:
        _drop_owned_test_schemas()


def _candidate(fixture, label, amount, *, extra=False):
    config, _, _ = fixture
    rows = [population_row(label, "one", amount=amount)]
    # These values must never cross the safe projection boundary.
    rows[0]["bronze_page_path"] = "PRIVATE_CANARY/pages/restricted.json"
    rows[0]["source_url"] = "https://community.secop.gov.co.evil.invalid/leak"
    if extra:
        rows.append(population_row(label, "two", contract_id="CO1.PCCNTR.2", amount="50.00"))
    write_population_bundle(config.silver_root, label, rows)
    bundle = verify_current_bundle(config.silver_root)
    return _candidate_from_bundle(fixture, bundle)


def _candidate_from_bundle(fixture, bundle):
    config, store, root = fixture
    WarehouseLoader(config).load(bundle)
    reservation = store.reserve({})
    publication_id = reservation["publication_id"]
    metadata = capture_inputs(config, bundle, publication_id, storage_budget_bytes=BUDGET)
    store.update_metadata(publication_id, metadata)
    receipt = run_candidate_build(
        config,
        publication_id,
        artifact_root=root / "builds",
        timeout_seconds=180,
        storage_budget_bytes=BUDGET,
        metadata=metadata,
    )
    store.update_metadata(publication_id, receipt["metadata"])
    store.seal(publication_id, receipt)
    return publication_id, reservation, receipt


def test_supported_legacy_unknown_evidence_remains_null_after_publication(publication_database):
    config, store, _ = publication_database
    write_identity_bundle(
        config.silver_root,
        "legacy-publication",
        [identity_row("legacy-publication", "legacy-one")],
    )
    bundle = verify_current_bundle(config.silver_root)
    seed_registered(config, bundle)
    publication_id, _, _ = _candidate_from_bundle(publication_database, bundle)
    _activate(store, publication_id)
    descriptor = resolve(config)
    detail = read_publication(config, descriptor, "mart_contract_detail")
    assert len(detail) == 1
    assert detail[0]["condition_state"] == "LEGACY_UNASSESSED"
    assert detail[0]["scope_id"] is None
    quality = read_publication(config, descriptor, "mart_data_quality")
    observations = [row for row in quality if row["evaluation_grain"] == "OBSERVATION"]
    assert observations
    assert {row["control_state"] for row in observations} == {"UNAVAILABLE_LEGACY"}
    assert all(row["numerator"] is None and row["denominator"] is None for row in observations)


def _active(store):
    return store.inspect()["active"]


def _activate(store, publication_id, *, restore=False):
    active = _active(store)
    return store.activate(
        publication_id,
        expected_publication_id=active["publication_id"],
        expected_generation=active["generation"],
        restore=restore,
    )


def _read_all(config, descriptor):
    return {
        relation: read_publication(config, descriptor, relation, limit=100)
        for relation in (*OUTPUT_RELATIONS, "publication_metadata")
    }


@contextmanager
def _assert_code(code):
    with pytest.raises(PublicationError) as failure:
        yield
    assert failure.value.code == code


def test_pinned_separate_connections_and_exact_complete_restore(publication_database):
    config, store, _ = publication_database
    a, _, _ = _candidate(publication_database, "publication-a", "100.00")
    _activate(store, a)
    descriptor = resolve(config)
    original = _read_all(config, descriptor)
    assert len(original["fact_contract"]) == 1
    assert original["fact_contract"][0]["contract_value_cop"] == Decimal("100.00")
    overview = query_metric(config, descriptor, "overview")[0]
    assert overview["publication_id"] == a
    assert (overview["contract_count"], overview["contract_value_cop"]) == (1, Decimal("100.00"))
    empty = query_metric(config, descriptor, "overview", {"statuses": ["nonexistent"]})[0]
    assert (empty["contract_count"], empty["contract_value_cop"]) == (0, Decimal("0.00"))
    assert empty["distribution_state"] == "NO_CONTRACTS"
    with _assert_code("PUBLICATION_INVALID_FILTER"):
        query_metric(config, descriptor, "overview", {"unapproved": "ignored?"})
    with _assert_code("PUBLICATION_INVALID_FILTER"):
        query_metric(
            config,
            descriptor,
            "overview",
            {"missing_signatures": True, "start_date": "2026-01-01"},
        )
    b, _, _ = _candidate(publication_database, "publication-b", "200.00", extra=True)
    _activate(store, b)
    assert resolve(config)["publication_id"] == b
    # Each helper opens its own connection; none may resolve latest again.
    assert _read_all(config, descriptor) == original
    assert query_metric(config, descriptor, "overview") == [overview]
    assert (
        read_publication(config, descriptor, "mart_contract_detail", limit=1, offset=0)
        == (original["mart_contract_detail"][:1])
    )
    restored = _activate(store, a, restore=True)
    assert restored["status"] == "restored"
    assert _read_all(config, resolve(config)) == original
    before = store.inspect()
    assert _activate(store, a)["status"] == "noop"
    assert store.inspect() == before


def test_actual_reader_roles_deny_raw_data_writes_and_ownership(publication_database):
    config, store, _ = publication_database
    publication_id, _, _ = _candidate(publication_database, "permissions-a", "100.00")
    _activate(store, publication_id)
    for role in ("secop_bi_reader", "secop_api_reader"):
        password = uuid.uuid4().hex
        with psycopg.connect(**config.connection_kwargs, autocommit=True) as database:
            database.execute(
                sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
        reader = replace(config, user=role, password=password)
        descriptor = resolve(reader)
        all_rows = _read_all(reader, descriptor)
        rendered = json.dumps(all_rows, default=str)
        assert "PRIVATE_CANARY" not in rendered
        assert "evil.invalid" not in rendered
        assert all(
            row["publication_id"] == publication_id for rows in all_rows.values() for row in rows
        )
        for statement in (
            "SELECT * FROM silver.contract_current",
            "SELECT * FROM publication_control.publications",
            f"SELECT * FROM publication_data_{publication_id}.fact_contract",
            f"SELECT * FROM candidate_{publication_id}_input.contract_current",
            "INSERT INTO published.fact_contract DEFAULT VALUES",
            "UPDATE published.fact_contract SET contract_value_cop=0",
            "DELETE FROM published.fact_contract",
            f"TRUNCATE publication_data_{publication_id}.fact_contract",
            "CREATE TABLE published.unapproved(id int)",
            "ALTER VIEW published.fact_contract RENAME TO unapproved",
            "DROP VIEW published.fact_contract",
            "SET ROLE secop_publication_owner",
            "SET ROLE secop_publication_builder",
        ):
            with (
                psycopg.connect(**reader.connection_kwargs) as connection,
                pytest.raises(psycopg.Error) as failure,
            ):
                connection.execute(statement)
            assert failure.value.sqlstate in ("42501", "3F000", "42P01")
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute("SET ROLE secop_publication_builder")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            database.execute(
                sql.SQL("UPDATE {}.fact_contract SET contract_value_cop=0").format(
                    sql.Identifier("publication_data_" + publication_id)
                )
            )


@pytest.mark.parametrize(
    "fault_point", ["activation_after_views", "activation_after_pointer", "activation_after_event"]
)
def test_failed_first_activation_has_no_partial_publication(publication_database, fault_point):
    config, store, _ = publication_database
    publication_id, _, _ = _candidate(publication_database, "failed-first", "100.00")

    def fail(point):
        if point == fault_point:
            raise RuntimeError("synthetic activation failure")

    failing = PublicationStore(config, fault=fail)
    with pytest.raises(RuntimeError, match="synthetic activation failure"):
        _activate(failing, publication_id)
    with _assert_code("PUBLICATION_UNAVAILABLE"):
        resolve(config)
    assert _active(store)["generation"] == 0
    assert _activate(store, publication_id)["status"] == "activated"


def test_active_generation_rejects_aba_during_slow_candidate(publication_database):
    _, store, _ = publication_database
    a, _, _ = _candidate(publication_database, "generation-a", "100.00")
    _activate(store, a)
    c, _, _ = _candidate(publication_database, "generation-c", "300.00")
    _activate(store, c)
    _activate(store, a, restore=True)
    # C is already retained: the single private candidate slot stays with B.
    b, reserved, _ = _candidate(publication_database, "generation-b", "200.00")
    _activate(store, c, restore=True)
    _activate(store, a, restore=True)
    with _assert_code("PUBLICATION_STALE"):
        store.activate(
            b,
            expected_publication_id=reserved["expected_publication_id"],
            expected_generation=reserved["expected_generation"],
        )
    assert _active(store)["publication_id"] == a


def test_old_rollback_descriptor_survives_loss_of_protection(publication_database):
    config, store, _ = publication_database
    a, _, _ = _candidate(publication_database, "retention-a", "100.00")
    _activate(store, a)
    b, _, _ = _candidate(publication_database, "retention-b", "200.00")
    _activate(store, b)
    # Only the test owner ages control timestamps; no production clock override.
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(
            "UPDATE publication_control.publications SET "
            "deactivated_at=clock_timestamp()-interval '49 hours', "
            "retain_until=clock_timestamp()-interval '25 hours' WHERE publication_id=%s",
            (a,),
        )
    descriptor = resolve(config, a)
    original = _read_all(config, descriptor)
    c, _, _ = _candidate(publication_database, "retention-c", "300.00")
    _activate(store, c)
    assert store.cleanup_preview() == []
    assert _read_all(config, descriptor) == original
    with psycopg.connect(**config.connection_kwargs) as database:
        retained = database.execute(
            "SELECT retain_until>=clock_timestamp()+interval '23 hours' "
            "FROM publication_control.publications WHERE publication_id=%s",
            (a,),
        ).fetchone()
    assert retained == (True,)
    with _assert_code("PUBLICATION_CAPACITY"):
        store.reserve({})


def test_expired_unknown_and_forged_descriptors_never_follow_latest(publication_database):
    config, store, _ = publication_database
    publication_id, _, _ = _candidate(publication_database, "descriptor-a", "100.00")
    _activate(store, publication_id)
    original = resolve(config)
    expired = copy.deepcopy(original)
    stale_start = datetime.now(UTC) - timedelta(hours=2)
    expired["resolved_at"] = stale_start.isoformat()
    expired["operation_expires_at"] = (stale_start + timedelta(hours=1)).isoformat()
    with _assert_code("PUBLICATION_EXPIRED"):
        validate_descriptor(config, expired)
    forged = copy.deepcopy(original)
    forged["read_namespace"] = "silver"
    with pytest.raises(PublicationError):
        validate_descriptor(config, forged)
    with pytest.raises(PublicationError):
        resolve(config, "candidate_" + publication_id)
    with pytest.raises(PublicationError):
        resolve(config, uuid.uuid4().hex)
    assert resolve(config)["publication_id"] == publication_id


def test_optional_scoring_failure_and_mismatch_preserve_analytics(publication_database):
    config, store, _ = publication_database
    publication_id, _, _ = _candidate(publication_database, "scoring-a", "100.00")
    _activate(store, publication_id)
    descriptor = resolve(config)
    original = _read_all(config, descriptor)
    before = store.inspect()
    assert scoring_compatibility(publication_id)["state"] == "ABSENT"
    assert scoring_compatibility(publication_id, failed=True)["state"] == "FAILED"
    assert scoring_compatibility(publication_id, uuid.uuid4().hex)["state"] == "MISMATCHED"
    assert scoring_compatibility(publication_id, publication_id)["state"] == "COMPATIBLE"
    assert _read_all(config, descriptor) == original
    assert store.inspect() == before
