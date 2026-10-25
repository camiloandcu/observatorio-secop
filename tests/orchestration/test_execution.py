"""Execution bindings, unsafe input rejection and recovery decisions."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from observatorio_secop.orchestration import worker
from observatorio_secop.orchestration.models import (
    PipelineError,
    Runtime,
    contained,
    validate_input,
)
from observatorio_secop.orchestration.receipts import Receipts, public_summary
from observatorio_secop.orchestration.recovery import extend_grant


def parameters():
    return dict(
        mode="demo",
        fixture_id="demo-a",
        signing_from="2026-01-01",
        signing_to="2026-02-01",
        baseline_version=None,
        baseline_sha256=None,
        total_rows=100,
        total_requests=20,
        grant_rows=100,
        grant_requests=20,
        page_size=50,
        build_seconds=180,
        storage_bytes=67108864,
    )


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    result = Runtime(
        tmp_path / "project",
        tmp_path / "state",
        tmp_path / "bronze",
        tmp_path / "silver",
        tmp_path / "reports",
        "demo",
    )
    monkeypatch.setattr(Runtime, "environment", classmethod(lambda _: result))
    monkeypatch.setattr(worker, "recipe", lambda _: {"code": "fixed"})
    return result


@pytest.mark.parametrize(
    "field,value",
    [
        ("mode", "$(touch secret)"),
        ("fixture_id", "../../private"),
        ("page_size", True),
        ("total_requests", 0),
        ("page_size", 251),
        ("baseline_version", "../current"),
        ("baseline_sha256", "forged"),
        ("build_seconds", 1801),
        ("grant_rows", 101),
        ("signing_to", "2025-12-31"),
    ],
)
def test_unsafe_trigger_rejected(field, value):
    with pytest.raises(ValueError):
        validate_input(parameters() | {field: value})


def test_unknown_connection_parameter_rejected():
    with pytest.raises(PipelineError, match="INVALID_INPUT"):
        validate_input(parameters() | {"connection": "postgres://CANARY"})


def test_valid_parameters_and_mode_separation(runtime):
    assert validate_input(parameters()) == parameters()
    runtime.check("demo")
    with pytest.raises(PipelineError, match="MODE_MISMATCH"):
        runtime.check("live")
    with pytest.raises(PipelineError, match="ROOT_OVERLAP"):
        replace(runtime, bronze=runtime.state / "nested").check("demo")


def test_symlink_escape_rejected(runtime, tmp_path):
    runtime.silver.mkdir()
    (runtime.silver / "escape").symlink_to(tmp_path / "private")
    with pytest.raises(PipelineError, match="UNSAFE_PATH"):
        contained(runtime.silver, "escape", "data")


def test_receipt_fixed_input_and_integrity(runtime):
    receipts = Receipts(runtime.state, "fixed")
    with receipts.locked():
        original = receipts.create(parameters(), {"code": "v1"})
        assert receipts.create(parameters(), {"code": "v1"}) == original
        with pytest.raises(PipelineError, match="INPUT_CHANGED"):
            receipts.create(parameters() | {"page_size": 1}, {"code": "v1"})
    corrupted = json.loads(receipts.path.read_text())
    corrupted["publication_id"] = "changed"
    receipts.path.write_text(json.dumps(corrupted))
    with pytest.raises(PipelineError, match="RECEIPT_CONFLICT"):
        receipts.read()


def test_same_execution_lock_prevents_concurrent_mutation(runtime):
    receipts = Receipts(runtime.state, "fixed")
    with receipts.locked(), pytest.raises(PipelineError, match="BUSY"), receipts.locked():
        pytest.fail("Concurrent mutation reached")


def test_stage_order_and_failed_outcome_remains_failure(runtime, monkeypatch):
    monkeypatch.setattr(worker, "dispatch", lambda *_: {})
    worker.run("validate", "fixed", parameters())
    with pytest.raises(PipelineError, match="UPSTREAM_INCOMPLETE"):
        worker.run("candidate", "fixed")

    def reject(*_):
        raise ValueError("password=SECRET_PATH_CANARY")

    monkeypatch.setattr(worker, "dispatch", reject)
    with pytest.raises(PipelineError, match="STAGE_REJECTED"):
        worker.run("acquire", "fixed")
    document = Receipts(runtime.state, "fixed").read()
    assert document["status"] == "failed"
    assert "SECRET_PATH_CANARY" not in json.dumps(public_summary(document))
    with pytest.raises(PipelineError, match="UPSTREAM_INCOMPLETE"):
        worker.run("outcome", "fixed")


def test_changed_recipe_rejected_before_dispatch(runtime, monkeypatch):
    monkeypatch.setattr(worker, "dispatch", lambda *_: {})
    worker.run("validate", "fixed", parameters())
    monkeypatch.setattr(worker, "recipe", lambda _: {"code": "changed"})
    monkeypatch.setattr(worker, "dispatch", lambda *_: pytest.fail("Effects reached"))
    with pytest.raises(PipelineError, match="INPUT_CHANGED"):
        worker.run("acquire", "fixed")


def test_explicit_grant_extension_preserves_total_plan(runtime, monkeypatch):
    monkeypatch.setattr(worker, "dispatch", lambda *_: {})
    worker.run("validate", "fixed", parameters() | {"grant_rows": 2})

    def incomplete(*_):
        raise PipelineError("PIPELINE_COVERAGE_INCOMPLETE")

    monkeypatch.setattr(worker, "dispatch", incomplete)
    with pytest.raises(PipelineError):
        worker.run("acquire", "fixed")
    report = extend_grant("fixed", "operator-2", 10, 5)
    assert report["status"] == "incomplete"
    document = Receipts(runtime.state, "fixed").read()
    assert document["grant"] == {"id": "operator-2", "rows": 10, "requests": 5}
    assert document["parameters"]["total_rows"] == 100
    assert document["grant_history"][0]["grant"]["id"] == "initial"
    with pytest.raises(PipelineError, match="GRANT_CONFLICT"):
        extend_grant("fixed", "initial", 10, 5)
    with pytest.raises(PipelineError, match="INVALID_LIMIT"):
        extend_grant("fixed", "operator-3", 101, 5)
