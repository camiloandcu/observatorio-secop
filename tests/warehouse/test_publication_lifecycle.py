"""Lifecycle fault boundaries; analytical producer proof lives in acceptance tests."""

from __future__ import annotations

import copy

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.inventory import relation_inventory
from observatorio_secop.publication.models import OUTPUT_RELATIONS, candidate_lock_id, namespaces
from observatorio_secop.publication.store import PublicationStore

from .test_publication_acceptance import (
    TEST_DSN,
    _activate,
    _candidate,
)
from .test_publication_acceptance import publication_database as _publication_database

pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires dedicated disposable PostgreSQL/dbt")


@pytest.fixture
def lifecycle_database(tmp_path):
    yield from _publication_database.__wrapped__(tmp_path)


def _clone_candidate(config, store, source, original):
    """Copy a verified fixture output to test lifecycle without rerunning formulas."""
    row = store.reserve(original["metadata"])
    value = row["publication_id"]
    receipt = copy.deepcopy(original)
    receipt["publication_id"] = value
    store.register_expected_nodes(value, receipt["required_nodes"])
    with psycopg.connect(**config.connection_kwargs) as database, database.cursor() as cursor:
        space = namespaces(value)
        cursor.execute(
            sql.SQL("CREATE SCHEMA {} AUTHORIZATION secop_publication_builder").format(
                sql.Identifier(space["analytics"])
            )
        )
        for name in OUTPUT_RELATIONS:
            cursor.execute(
                sql.SQL("CREATE TABLE {}.{} AS TABLE {}.{}").format(
                    sql.Identifier(space["analytics"]),
                    sql.Identifier(name),
                    sql.Identifier(namespaces(source)["data"]),
                    sql.Identifier(name),
                )
            )
            cursor.execute(
                sql.SQL("ALTER TABLE {}.{} OWNER TO secop_publication_builder").format(
                    sql.Identifier(space["analytics"]), sql.Identifier(name)
                )
            )
        receipt["inventory"] = relation_inventory(cursor, space["analytics"])
    return value, receipt


def test_receipt_tamper_sealing_failure_and_orphan_recovery(lifecycle_database):
    config, store, _ = lifecycle_database
    a, _, receipt = _candidate(lifecycle_database, "lifecycle-source", "100.00")
    _activate(store, a)
    b, cloned = _clone_candidate(config, store, a, receipt)
    before = store.inspect()
    for mutation in (
        "missing_result",
        "skipped_test",
        "missing_all_references",
        "wrong_identity",
        "wrong_hash",
    ):
        invalid = copy.deepcopy(cloned)
        if mutation == "missing_result":
            invalid["results"] = invalid["results"][1:]
        elif mutation == "skipped_test":
            next(item for item in invalid["results"] if item["unique_id"] in invalid["test_nodes"])[
                "status"
            ] = "skipped"
        elif mutation == "missing_all_references":
            omitted = invalid["test_nodes"].pop()
            invalid["required_nodes"].remove(omitted)
            invalid["results"] = [
                item for item in invalid["results"] if item["unique_id"] != omitted
            ]
        elif mutation == "wrong_identity":
            invalid["metadata"]["silver_manifest_sha256"] = "f" * 64
        else:
            invalid["manifest_sha256"] = "not-a-digest"
        with pytest.raises(PublicationError) as failure:
            store.seal(b, invalid)
        assert failure.value.code == "PUBLICATION_VALIDATION"
        assert store.inspect() == before

    def fail(point):
        if point == "seal_after_transfer":
            raise RuntimeError("sealing failure")

    with pytest.raises(RuntimeError, match="sealing failure"):
        PublicationStore(config, fault=fail).seal(b, cloned)
    assert store.inspect() == before
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(
            sql.SQL("UPDATE {}.fact_contract SET contract_value_cop=123").format(
                sql.Identifier(namespaces(b)["analytics"])
            )
        )
    with pytest.raises(PublicationError) as changed:
        store.seal(b, cloned)
    assert changed.value.code == "PUBLICATION_CONFLICT"
    with pytest.raises(PublicationError) as busy:
        store.reserve({})
    assert busy.value.code == "PUBLICATION_CANDIDATE_BUSY"
    # A running build holds this session lock; an orphan does not.
    with psycopg.connect(**config.connection_kwargs, autocommit=True) as running:
        running.execute("SELECT pg_advisory_lock(%s)", (candidate_lock_id(b),))
        with pytest.raises(PublicationError) as live:
            store.discard(b)
        assert live.value.code == "PUBLICATION_CANDIDATE_BUSY"
        with pytest.raises(PublicationError) as sealing_live:
            store.seal(b, cloned)
        assert sealing_live.value.code == "PUBLICATION_CANDIDATE_BUSY"
    assert store.discard(b)["status"] == "discarded"
    assert store.inspect()["candidate"] is None
    assert store.inspect()["active"]["publication_id"] == a
    _assert_noop_checks_projection_metadata_and_acl(config, store, a)


def _assert_noop_checks_projection_metadata_and_acl(config, store, value):
    original = store.inspect(value)
    active = store.inspect()["active"]
    for schema, inventory in (
        (namespaces(value)["read"], original["projection_inventory"]),
        ("published", active["view_inventory"]),
    ):
        definition = inventory["fact_contract"]["definition"].rstrip().removesuffix(";")
        with psycopg.connect(**config.connection_kwargs) as database:
            database.execute(
                sql.SQL(
                    "CREATE OR REPLACE VIEW {}.fact_contract AS SELECT * FROM ({}) t WHERE false"
                ).format(sql.Identifier(schema), sql.SQL(definition))
            )
        with pytest.raises(PublicationError) as altered:
            _activate(store, value)
        assert altered.value.code == "PUBLICATION_CONFLICT"
        with psycopg.connect(**config.connection_kwargs) as database:
            database.execute(
                sql.SQL("CREATE OR REPLACE VIEW {}.fact_contract AS {}").format(
                    sql.Identifier(schema), sql.SQL(definition)
                )
            )
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(
            "UPDATE publication_control.publications SET metadata=metadata||"
            '\'{"observed_at":"tampered"}\'::jsonb WHERE publication_id=%s',
            (value,),
        )
    with pytest.raises(PublicationError) as metadata:
        _activate(store, value)
    assert metadata.value.code == "PUBLICATION_CONFLICT"
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(
            "UPDATE publication_control.publications SET metadata=%s WHERE publication_id=%s",
            (Jsonb(original["metadata"]), value),
        )
        database.execute("GRANT CREATE ON SCHEMA published TO secop_bi_reader")
    with pytest.raises(PublicationError) as privilege:
        _activate(store, value)
    assert privilege.value.code == "PUBLICATION_ROLE_UNSAFE"
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute("REVOKE CREATE ON SCHEMA published FROM secop_bi_reader")
    assert _activate(store, value)["status"] == "noop"


def test_cleanup_capacity_dependency_lock_and_pruned_identity(lifecycle_database):
    config, store, _ = lifecycle_database
    a, _, receipt = _candidate(lifecycle_database, "cleanup-source", "100.00")
    _activate(store, a)
    b, b_receipt = _clone_candidate(config, store, a, receipt)
    store.seal(b, b_receipt)
    _activate(store, b)
    c, c_receipt = _clone_candidate(config, store, a, receipt)
    store.seal(c, c_receipt)
    _activate(store, c)
    with pytest.raises(PublicationError) as capacity:
        store.reserve({})
    assert capacity.value.code == "PUBLICATION_CAPACITY"
    for protected in (b, c):
        with pytest.raises(PublicationError) as failure:
            store.cleanup([protected])
        assert failure.value.code == "PUBLICATION_PROTECTED"
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(
            "UPDATE publication_control.publications SET retain_until="
            "clock_timestamp()-interval '1 hour' WHERE publication_id=%s",
            (a,),
        )
        database.execute(
            sql.SQL(
                "CREATE VIEW public.unapproved_dependency AS "
                "SELECT contract_sk FROM {}.fact_contract"
            ).format(sql.Identifier(namespaces(a)["read"]))
        )
    assert [row["publication_id"] for row in store.cleanup_preview()] == [a]
    with pytest.raises(PublicationError) as dependency:
        store.cleanup([a])
    assert dependency.value.code == "PUBLICATION_DEPENDENCY"
    assert store.inspect(a)["state"] == "retained"
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute("DROP VIEW public.unapproved_dependency")
    with psycopg.connect(**config.connection_kwargs) as reader:
        reader.execute(
            sql.SQL("SELECT contract_sk FROM {}.fact_contract").format(
                sql.Identifier(namespaces(a)["read"])
            )
        ).fetchall()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            store.cleanup([a])
        assert store.inspect(a)["state"] == "retained"
    assert store.cleanup([a]) == {"status": "cleaned", "publication_ids": [a]}
    assert store.inspect(a)["state"] == "pruned"
    active = store.inspect()["active"]
    with pytest.raises(PublicationError) as unavailable:
        store.activate(a, active["publication_id"], active["generation"], restore=True)
    assert unavailable.value.code == "PUBLICATION_UNAVAILABLE"
    assert store.inspect()["active"] == active
