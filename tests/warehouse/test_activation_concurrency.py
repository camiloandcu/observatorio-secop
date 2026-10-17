from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import silver_row, write_bundle
from .snapshot_helpers import bundles, expected_rows, seed_legacy, snapshot


@pytest.mark.parametrize("mode", ["fresh_identical", "restore_identical", "distinct", "legacy"])
def test_publication_lock_serializes_state_reads_and_migration(
    postgres_config: WarehouseConfig, mode: str
) -> None:
    a, b, _ = bundles(postgres_config)
    loader = WarehouseLoader(postgres_config)
    requests = [a, a]
    expected_events = 1
    if mode == "restore_identical":
        loader.load(a)
        loader.load(b)
        expected_events = 3
    elif mode == "legacy":
        seed_legacy(postgres_config, a, b)
        expected_events = 2
    elif mode == "distinct":
        loader.load(a)
        loader.load(b)
        write_bundle(postgres_config.silver_root, "C", [silver_row("C")])
        loader.load(verify_current_bundle(postgres_config.silver_root))
        requests = [a, b]
        expected_events = 5

    with ThreadPoolExecutor(max_workers=2) as executor:
        # The controlling connection releases even on assertion failure before
        # the executor joins workers. No sleeps decide ordering or correctness.
        with psycopg.connect(**postgres_config.connection_kwargs) as blocker:
            blocker.execute("SELECT pg_advisory_xact_lock(%s)", (postgres_config.advisory_lock_id,))
            futures = [executor.submit(WarehouseLoader(postgres_config).load, r) for r in requests]
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
        results = [future.result(timeout=15) for future in futures]

    if mode == "distinct":
        assert [r.status for r in results] == ["applied", "applied"]
    else:
        assert sorted(r.status for r in results) == ["applied", "noop"]
    after = snapshot(postgres_config)
    events = after["load_audit_activation"]
    assert len(events) == expected_events
    final = a if events[-1][1] == "A" else b
    assert after["contract_current"] == expected_rows(final)
    assert after["load_audit_active"][0][1:] == (
        final.version_id,
        final.manifest_sha256,
        events[-1][0],
    )
    if mode == "distinct":
        assert {e[1] for e in events[-2:]} == {"A", "B"}
        assert events[-1][3:5] == events[-2][1:3]
    if mode == "legacy":
        assert [e[5] for e in events] == ["legacy_baseline", "restoration"]
