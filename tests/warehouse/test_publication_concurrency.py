"""Observed PostgreSQL lock contention, rather than sequential concurrency substitutes."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event, Lock

import psycopg
import pytest
from psycopg import sql

from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.models import PUBLICATION_LOCK_ID, namespaces
from observatorio_secop.publication.readers import read_publication, resolve
from observatorio_secop.publication.store import PublicationStore

from .test_publication_acceptance import TEST_DSN, _activate, _candidate, _read_all
from .test_publication_acceptance import publication_database as _publication_database
from .test_publication_lifecycle import _clone_candidate

pytestmark = pytest.mark.skipif(not TEST_DSN, reason="Requires disposable PostgreSQL/dbt")


@pytest.fixture
def concurrency_database(tmp_path):
    yield from _publication_database.__wrapped__(tmp_path)


def _clone_sealed(config, store, source, receipt):
    value, copied = _clone_candidate(config, store, source, receipt)
    store.seal(value, copied)
    return value


def _wait_for_lock(config, statement, parameters):
    deadline = time.monotonic() + 4
    with psycopg.connect(**config.connection_kwargs, autocommit=True) as monitor:
        while time.monotonic() < deadline:
            row = monitor.execute(statement, parameters).fetchone()
            if row is not None:
                return row[0]
            time.sleep(0.01)
    pytest.fail("Expected actual PostgreSQL lock contention was not observed")


def _wait_for_publication_lock(config):
    return _wait_for_lock(
        config,
        "SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=%s AND NOT granted",
        (PUBLICATION_LOCK_ID,),
    )


def _paused_first_activation():
    reached, release = Event(), Event()
    claimed = Lock()

    def fault(point):
        if point == "activation_after_views" and claimed.acquire(blocking=False):
            reached.set()
            if not release.wait(8):
                raise AssertionError("Test did not release the observed activation barrier")

    return fault, reached, release


def test_concurrent_activations_same_generation_have_one_winner(concurrency_database):
    config, store, _ = concurrency_database
    a, _, receipt = _candidate(concurrency_database, "concurrent-source", "100.00")
    _activate(store, a)
    c = _clone_sealed(config, store, a, receipt)
    _activate(store, c)
    _activate(store, a, restore=True)
    b = _clone_sealed(config, store, a, receipt)
    expected = store.inspect()["active"]
    fault, reached, release = _paused_first_activation()
    start = Barrier(3)

    def activate(value, restore):
        start.wait(timeout=5)
        try:
            return PublicationStore(config, fault=fault).activate(
                value,
                expected["publication_id"],
                expected["generation"],
                restore=restore,
            )
        except PublicationError as failure:
            return {"code": failure.code}

    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = [pool.submit(activate, b, False), pool.submit(activate, c, True)]
        try:
            start.wait(timeout=5)
            assert reached.wait(5)
            assert _wait_for_publication_lock(config)
        finally:
            release.set()
        results = [future.result(timeout=8) for future in pending]
    successes = [result for result in results if "status" in result]
    assert len(successes) == 1
    assert [result for result in results if "code" in result] == [{"code": "PUBLICATION_STALE"}]
    active = store.inspect()["active"]
    assert active["generation"] == expected["generation"] + 1
    assert active["publication_id"] == successes[0]["publication_id"]
    contents = _read_all(config, resolve(config))
    assert all(
        row["publication_id"] == active["publication_id"]
        for rows in contents.values()
        for row in rows
    )


def test_blocked_pinned_data_read_releases_registry_lock_and_has_timeout(concurrency_database):
    config, store, _ = concurrency_database
    a, _, receipt = _candidate(concurrency_database, "blocked-reader-source", "100.00")
    _activate(store, a)
    descriptor = resolve(config)
    b = _clone_sealed(config, store, a, receipt)
    table = sql.Identifier(namespaces(a)["data"], "fact_contract")
    with (
        psycopg.connect(**config.connection_kwargs) as blocker,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        blocker.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(table))
        pending = pool.submit(read_publication, config, descriptor, "fact_contract")
        try:
            pid = _wait_for_lock(
                config,
                "SELECT pid FROM pg_locks WHERE relation=%s::regclass "
                "AND mode='AccessShareLock' AND NOT granted",
                (table.as_string(),),
            )
            with psycopg.connect(**config.connection_kwargs) as monitor:
                assert monitor.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=%s "
                    "AND locktype='advisory' AND objid=%s)",
                    (pid, PUBLICATION_LOCK_ID),
                ).fetchone() == (False,)
            # Actual activation must complete while the pinned data SQL remains blocked.
            activated = pool.submit(_activate, store, b)
            assert activated.result(timeout=3)["status"] == "activated"
        finally:
            blocker.commit()
        assert {row["publication_id"] for row in pending.result(timeout=5)} == {a}

    with psycopg.connect(**config.connection_kwargs) as blocker:
        blocker.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(table))
        with pytest.raises(PublicationError) as timeout:
            read_publication(config, descriptor, "fact_contract", statement_timeout_ms=100)
        assert timeout.value.code == "PUBLICATION_QUERY_TIMEOUT"
    assert resolve(config)["publication_id"] == b


def test_cleanup_waits_for_restore_and_rechecks_new_protection(concurrency_database):
    config, store, _ = concurrency_database
    a, _, receipt = _candidate(concurrency_database, "cleanup-race-source", "100.00")
    _activate(store, a)
    b = _clone_sealed(config, store, a, receipt)
    _activate(store, b)
    c = _clone_sealed(config, store, a, receipt)
    _activate(store, c)
    with psycopg.connect(**config.connection_kwargs) as database:
        database.execute(
            "UPDATE publication_control.publications SET retain_until="
            "clock_timestamp()-interval '1 hour' WHERE publication_id=%s",
            (a,),
        )
    assert [row["publication_id"] for row in store.cleanup_preview()] == [a]
    expected = store.inspect()["active"]
    fault, reached, release = _paused_first_activation()
    restoring = PublicationStore(config, fault=fault)

    def cleanup():
        try:
            store.cleanup([a])
        except PublicationError as failure:
            return failure.code
        pytest.fail("Cleanup deleted a concurrently restored publication")

    with ThreadPoolExecutor(max_workers=2) as pool:
        restoration = pool.submit(
            restoring.activate, a, expected["publication_id"], expected["generation"], restore=True
        )
        try:
            assert reached.wait(5)
            deletion = pool.submit(cleanup)
            assert _wait_for_publication_lock(config)
        finally:
            release.set()
        assert restoration.result(timeout=8)["status"] == "restored"
        assert deletion.result(timeout=8) == "PUBLICATION_PROTECTED"
    assert store.inspect(a)["state"] == "retained"
    assert resolve(config)["publication_id"] == a
