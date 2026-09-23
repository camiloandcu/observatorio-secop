from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from observatorio_secop.ingestion.errors import ConcurrentRunError, ResumeError
from observatorio_secop.ingestion.models import PageRecord, RunManifest
from observatorio_secop.ingestion.state import (
    DatasetLock,
    IngestionState,
    Observation,
    observations_from_pages,
)
from observatorio_secop.ingestion.storage import BronzeStorage, canonical_json, sha256_bytes


def manifest(run_id: str = "run-1") -> RunManifest:
    return RunManifest(
        run_id=run_id,
        dataset_id="jbjy-vk9h",
        contract_sha256="a" * 64,
        parameters={"from": "2026-01-01", "projection": ["id_contrato"]},
        checkpoint_before=None,
        started_at_utc="2026-01-01T00:00:00.000Z",
        finished_at_utc="2026-01-01T01:00:00.000Z",
        range_complete=True,
        checkpoint_candidate="2026-01-02T00:00:00.000Z",
    )


def test_storage_preserves_raw_bytes_hashes_and_separate_metadata(tmp_path: Path) -> None:
    storage = BronzeStorage(tmp_path / "bronze")
    run = manifest()
    storage.create_staging(run)
    raw = b'[{"id_contrato":"A","departamento":"Antioquia"}]\n'
    path, digest = storage.write_page(run.run_id, "primary", 1, raw)
    run.pages.append(
        PageRecord(
            lane="primary",
            sequence=1,
            path=path,
            sha256=digest,
            row_count=1,
            status=200,
            duration_ms=4,
            retries=0,
            requested_cursor=None,
            final_cursor={"timestamp": "2026-01-01", "identity": "A"},
            fetched_at_utc="2026-01-01T00:00:01.000Z",
            query={"$limit": 1},
        )
    )
    storage.write_page_state(run.run_id, run)

    loaded = storage.load_staging(run.run_id)
    assert (storage.staging_path(run.run_id) / path).read_bytes() == raw
    assert loaded.pages[0].sha256 == sha256_bytes(raw)
    assert "fetched_at_utc" not in json.loads(raw)[0]


def test_tampered_or_incoherent_staging_is_rejected(tmp_path: Path) -> None:
    storage = BronzeStorage(tmp_path / "bronze")
    run = manifest()
    storage.create_staging(run)
    path, digest = storage.write_page(run.run_id, "primary", 1, b"[]")
    run.pages.append(PageRecord("primary", 1, path, digest, 1, 200, 1, 0, None, None, "now", {}))
    storage.write_page_state(run.run_id, run)
    with pytest.raises(ResumeError, match="row count"):
        storage.load_staging(run.run_id)

    (storage.staging_path(run.run_id) / path).write_bytes(b"[{}]")
    with pytest.raises(ResumeError, match="hash"):
        storage.load_staging(run.run_id)


def test_state_commit_is_atomic_and_classifies_observations(tmp_path: Path) -> None:
    state = IngestionState(tmp_path / "state.sqlite")
    first = manifest("first")
    observations = [Observation("A", "hash-1", "page.json", 0)]
    counts = state.commit(first, observations, manifest_path="runs/first/manifest.json")
    assert (counts.new, counts.repeated, counts.new_versions) == (1, 0, 0)
    assert state.checkpoint("jbjy-vk9h") == first.checkpoint_candidate

    repeated = manifest("second")
    counts = state.commit(repeated, observations, manifest_path="runs/second/manifest.json")
    assert (counts.new, counts.repeated, counts.new_versions) == (0, 1, 0)

    changed = manifest("third")
    counts = state.commit(
        changed,
        [Observation("A", "hash-2", "page.json", 0)],
        manifest_path="runs/third/manifest.json",
    )
    assert (counts.new, counts.repeated, counts.new_versions) == (1, 0, 1)


def test_failed_state_transaction_rolls_back_checkpoint(tmp_path: Path) -> None:
    state = IngestionState(tmp_path / "state.sqlite")
    initial = manifest("initial")
    state.commit(initial, [], manifest_path="initial")
    bad = manifest("bad")
    bad.checkpoint_candidate = "2026-02-01T00:00:00.000Z"
    duplicate_identity = [
        Observation("A", "hash", "page", 0),
        Observation("A", "hash", "page", 0),
    ]
    # Repeated observations within one transaction are deliberately idempotent.
    state.commit(bad, duplicate_identity, manifest_path="bad")
    assert state.checkpoint("jbjy-vk9h") == bad.checkpoint_candidate

    broken = manifest("broken")
    broken.finished_at_utc = None
    with pytest.raises(sqlite3.IntegrityError):
        state.commit(broken, [], manifest_path="broken")
    assert state.checkpoint("jbjy-vk9h") == bad.checkpoint_candidate
    assert not state.has_run("broken")


def test_dataset_lock_rejects_concurrent_writer(tmp_path: Path) -> None:
    path = tmp_path / "ingestion.lock"
    with DatasetLock(path), pytest.raises(ConcurrentRunError), DatasetLock(path):
        pass


def test_observation_hash_is_canonical(tmp_path: Path) -> None:
    storage = BronzeStorage(tmp_path / "bronze")
    run = manifest()
    storage.create_staging(run)
    raw = b'[{"b":2,"id_contrato":"A","a":1}]'
    path, digest = storage.write_page(run.run_id, "primary", 1, raw)
    run.pages.append(PageRecord("primary", 1, path, digest, 1, 200, 1, 0, None, None, "now", {}))
    published = storage.prepare(run)
    item = observations_from_pages(published, run, "id_contrato")[0]
    assert item.payload_hash == sha256_bytes(canonical_json({"a": 1, "b": 2, "id_contrato": "A"}))
