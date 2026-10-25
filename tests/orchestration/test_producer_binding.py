"""Real acquisition handoff binding and state-aware publication adapters."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from observatorio_secop.orchestration import operations, publication, worker
from observatorio_secop.orchestration.models import PipelineError, Runtime
from observatorio_secop.orchestration.receipts import Receipts
from observatorio_secop.warehouse.errors import WarehouseLoadError

from .test_execution import parameters

ROOT = Path(__file__).resolve().parents[2]


def test_committed_acquisition_hash_cannot_be_rebound(tmp_path):
    runtime = Runtime(
        ROOT,
        tmp_path / "state",
        tmp_path / "bronze",
        tmp_path / "silver",
        tmp_path / "reports",
        "demo",
    )
    receipts = Receipts(runtime.state, "operator-safe-name")
    with receipts.locked():
        document = receipts.create(parameters(), {})
    assert len(document["publication_id"]) == 32
    int(document["publication_id"], 16)
    output = operations.acquisition(runtime, document)
    document["stages"]["acquire"] = output | {"status": "success"}
    assert operations.acquisition(runtime, document) == output
    path = runtime.bronze / "runs" / document["campaign_id"] / "manifest.json"
    path.write_bytes(path.read_bytes() + b"\n")  # valid identical JSON, different committed bytes
    with pytest.raises(PipelineError, match="ARTIFACT_CHANGED"):
        operations.acquisition(runtime, document)


def test_wrapped_transient_is_retried_without_printing_cause():
    try:
        try:
            raise psycopg.OperationalError("password=PRIVATE_CANARY")
        except psycopg.Error as cause:
            raise WarehouseLoadError("PostgreSQL load failed") from cause
    except WarehouseLoadError as error:
        assert worker.code(error) == "PIPELINE_TRANSIENT"


@pytest.fixture
def publication_adapter(monkeypatch):
    bundle = SimpleNamespace(version_id="v", manifest_sha256="h", row_count=2)
    monkeypatch.setattr(publication, "bundle_for", lambda *_: bundle)
    monkeypatch.setattr(publication, "warehouse_config", lambda _: None)
    runtime = SimpleNamespace(reports=Path("/tmp/pipeline-test-reports"))
    document = {
        "publication_id": "a" * 32,
        "expected_active": {"publication_id": "b" * 32, "generation": 4},
        "parameters": {"build_seconds": 60, "storage_bytes": 1000000},
    }
    row = {
        "expected_publication_id": "b" * 32,
        "expected_generation": 4,
        "metadata": {"silver_version_id": "v", "silver_manifest_sha256": "h"},
        "state": "sealed",
        "ever_activated": False,
    }
    return runtime, document, row


def test_sealed_candidate_reused_without_rebuild(publication_adapter, monkeypatch):
    runtime, document, row = publication_adapter
    monkeypatch.setattr(
        publication, "PublicationStore", lambda _: SimpleNamespace(inspect=lambda _: row)
    )
    monkeypatch.setattr(publication, "_build", lambda *_: pytest.fail("Sealed output rebuilt"))
    assert publication.candidate(runtime, document)["publication_id"] == document["publication_id"]


def test_changed_activation_generation_stops_before_build(publication_adapter, monkeypatch):
    runtime, document, row = publication_adapter
    row["expected_generation"] += 1
    monkeypatch.setattr(
        publication, "PublicationStore", lambda _: SimpleNamespace(inspect=lambda _: row)
    )
    monkeypatch.setattr(publication, "_build", lambda *_: pytest.fail("Stale build reached"))
    with pytest.raises(PipelineError, match="STALE_PUBLICATION"):
        publication.candidate(runtime, document)


def test_superseded_activation_is_never_automatically_restored(publication_adapter, monkeypatch):
    runtime, document, row = publication_adapter
    row["ever_activated"] = True

    def inspect(identity=None):
        return row if identity else {"active": {"publication_id": "c" * 32}}

    monkeypatch.setattr(publication, "PublicationStore", lambda _: SimpleNamespace(inspect=inspect))
    with pytest.raises(PipelineError, match="ACTIVATION_SUPERSEDED"):
        publication.activate(runtime, document)


def test_deterministic_denial_does_not_inherit_transient_retry():
    try:
        try:
            raise psycopg.OperationalError("PRIVATE_CANARY")
        except psycopg.Error as cause:
            raise PipelineError("PIPELINE_RUNTIME_BINDING_CONFLICT") from cause
    except PipelineError as error:
        assert worker.code(error) == "PIPELINE_RUNTIME_BINDING_CONFLICT"
