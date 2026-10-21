from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import psycopg
import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConflictError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import population_row, silver_row, write_bundle, write_population_bundle
from .snapshot_helpers import bundles, expected_rows, seed_legacy, seed_registered, snapshot


@pytest.mark.parametrize("mode", ["fresh_identical", "restore_identical", "distinct", "legacy"])
def test_publication_lock_serializes_state_reads_and_migration(
    postgres_config: WarehouseConfig, mode: str
) -> None:
    a, b, _ = bundles(postgres_config)
    requests = [a, a]
    expected_events = 1
    options = {}
    if mode == "fresh_identical":
        postgres_config = replace(
            postgres_config, silver_root=postgres_config.silver_root / "fresh"
        )
        write_population_bundle(
            postgres_config.silver_root, "scoped-A", [population_row("scoped-A")]
        )
        a = verify_current_bundle(postgres_config.silver_root)
        requests = [a, a]
    if mode == "restore_identical":
        seed_registered(postgres_config, a, b)
        options = {"restore": True, "expected_current": (b.version_id, b.manifest_sha256)}
        expected_events = 3
    elif mode == "legacy":
        seed_legacy(postgres_config, a, b)
        options = {"restore": True, "expected_current": (b.version_id, b.manifest_sha256)}
        expected_events = 2
    elif mode == "distinct":
        write_bundle(postgres_config.silver_root, "C", [silver_row("C")])
        c = verify_current_bundle(postgres_config.silver_root)
        seed_registered(postgres_config, a, b, c)
        options = {"restore": True, "expected_current": (c.version_id, c.manifest_sha256)}
        requests = [a, b]
        expected_events = 4

    with ThreadPoolExecutor(max_workers=2) as executor:
        # The controlling connection releases even on assertion failure before
        # the executor joins workers. No sleeps decide ordering or correctness.
        with psycopg.connect(**postgres_config.connection_kwargs) as blocker:
            blocker.execute("SELECT pg_advisory_xact_lock(%s)", (postgres_config.advisory_lock_id,))
            futures = [
                executor.submit(WarehouseLoader(postgres_config).load, r, **options)
                for r in requests
            ]
            deadline = time.monotonic() + 10
            while True:
                waiting = blocker.execute(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' "
                    "AND NOT granted AND objid = %s AND classid = 0",
                    (postgres_config.advisory_lock_id,),
                ).fetchone()[0]
                if waiting == 2:
                    break
                assert time.monotonic() < deadline, "loaders must wait for the lock"
                assert not any(f.done() for f in futures), "load returned before acquiring the lock"
                time.sleep(0.01)
            blocker.commit()
        results, conflicts = [], []
        for future in futures:
            try:
                results.append(future.result(timeout=15))
            except WarehouseConflictError as error:
                conflicts.append(error)

    if mode == "distinct":
        assert [r.status for r in results] == ["applied"]
        assert len(conflicts) == 1
    else:
        assert not conflicts
        assert sorted(r.status for r in results) == ["applied", "noop"]
    after = snapshot(postgres_config)
    events = after["load_audit_activation"]
    assert len(events) == expected_events
    final = a if events[-1][1] == a.version_id else b
    assert after["contract_current"] == expected_rows(final)
    assert after["load_audit_active"][0][1:] == (
        final.version_id,
        final.manifest_sha256,
        events[-1][0],
    )
    if mode == "distinct":
        assert events[-1][1] in {"A", "B"}
        assert events[-1][3:5] == events[-2][1:3]
    if mode == "legacy":
        assert [e[5] for e in events] == ["legacy_baseline", "restoration"]
