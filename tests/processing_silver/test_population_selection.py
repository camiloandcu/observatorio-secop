"""Selection state tests isolate locks/CAS from the separately integrated bundle reader."""

from __future__ import annotations

import json

import pytest

from observatorio_secop.population import PopulationScope
from observatorio_secop.processing.silver import selection
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.storage import SilverStorage, canonical_json
from observatorio_secop.warehouse.inventory import sha256_path


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(selection, "verify_manifest_bundle", lambda *args, **kwargs: None)
    return SilverStorage(tmp_path / "silver")


def candidate(storage, version, *, parent=None, mode="authoritative_candidate", scope=None):
    scope = scope or PopulationScope("2026-01-01", "2027-01-01")
    path = storage.version_path(version)
    path.mkdir(parents=True)
    receipt = {
        "mode": mode,
        "state": "complete",
        "scope": scope.as_dict(),
        "scope_id": scope.scope_id,
        "expected_parent": parent,
        "baseline": parent,
    }
    evidence = path / "evidence.json"
    evidence.write_bytes(canonical_json(receipt))
    manifest = {
        "silver_version_id": version,
        "status": "valid",
        "quality_results": [],
        "files": [
            {
                "path": evidence.name,
                "size_bytes": evidence.stat().st_size,
                "sha256": sha256_path(evidence),
            }
        ],
        "population": {"mode": mode, "coverage_path": evidence.name, "scope_drift": False},
    }
    manifest_path = path / "manifest.json"
    manifest_path.write_bytes(canonical_json(manifest))
    return {"silver_version_id": version, "manifest_sha256": sha256_path(manifest_path)}


def test_preview_new_reuse_and_rollback_do_not_select(storage):
    current = candidate(storage, "A")
    storage.rollback("A")
    before = storage.current_path.read_bytes()
    preview = candidate(storage, "preview", parent=current, mode="preview")
    for _ in range(2):
        with pytest.raises(PublicationError, match="Preview"):
            storage.rollback("preview")
    with pytest.raises(PublicationError, match="Preview"):
        storage.rollback(
            "preview",
            manifest_sha256=preview["manifest_sha256"],
            expected_current=current,
            restore=True,
        )
    assert storage.current_path.read_bytes() == before


def test_legacy_selection_requires_full_bundle_verifier(storage, monkeypatch):
    candidate(storage, "legacy")
    path = storage.version_path("legacy") / "manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest.pop("population")
    path.write_bytes(canonical_json(manifest))

    def reject(*_args, **_kwargs):
        raise PublicationError("Undeclared legacy data")

    monkeypatch.setattr(selection, "verify_manifest_bundle", reject)
    with pytest.raises(PublicationError, match="Undeclared legacy data"):
        selection.select(storage, manifest)
    assert not storage.current_path.exists()


def test_stale_parent_blocks_and_registered_restore_uses_separate_current(storage):
    a = candidate(storage, "A")
    storage.rollback("A")
    b = candidate(storage, "B", parent=a)
    storage.rollback("B")
    before = storage.current_path.read_bytes()
    candidate(storage, "stale", parent=a)
    with pytest.raises(PublicationError, match="expected-parent"):
        storage.rollback("stale")
    with pytest.raises(PublicationError, match="expected-current"):
        storage.rollback(
            "A", manifest_sha256=a["manifest_sha256"], expected_current=a, restore=True
        )
    assert storage.current_path.read_bytes() == before
    storage.rollback("A", manifest_sha256=a["manifest_sha256"], expected_current=b, restore=True)
    assert selection.identity(storage.current()) == a
    # Exact-active retry does not compare A's historical parent with A or B.
    storage.rollback("A", manifest_sha256=a["manifest_sha256"], expected_current=b, restore=True)
    assert selection.identity(storage.current()) == a


def test_failed_pointer_does_not_register_a_stale_candidate_for_restore(storage, monkeypatch):
    a = candidate(storage, "A")
    storage.rollback("A")
    b = candidate(storage, "B", parent=a)
    original = storage._write_current

    def fail(*_args, **_kwargs):
        raise OSError("interrupted")

    monkeypatch.setattr(storage, "_write_current", fail)
    with pytest.raises(OSError, match="interrupted"):
        storage.rollback("B")
    assert selection.identity(storage.current()) == a
    registry = json.loads((storage.root / "selection_registry.json").read_bytes())
    assert "B" not in registry
    monkeypatch.setattr(storage, "_write_current", original)
    with pytest.raises(PublicationError, match="registered"):
        storage.rollback(
            "B", manifest_sha256=b["manifest_sha256"], expected_current=a, restore=True
        )


def test_different_scope_and_tampered_binding_block_before_pointer(storage):
    candidate(storage, "A")
    storage.rollback("A")
    before = storage.current_path.read_bytes()
    candidate(storage, "other", scope=PopulationScope("2025-01-01", "2026-01-01"))
    with pytest.raises(PublicationError, match="bound population scope"):
        storage.rollback("other")
    binding_path = storage.root / "population_scope.json"
    record = json.loads(binding_path.read_bytes())
    record["scope_id"] = "0" * 64
    binding_path.write_bytes(canonical_json(record))
    with pytest.raises(PublicationError, match="binding is inconsistent"):
        storage.rollback("A")
    assert storage.current_path.read_bytes() == before
