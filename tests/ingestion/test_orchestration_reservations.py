"""Named campaign/grant recovery closes producer-commit/task-output crash gaps."""

from __future__ import annotations

import json

import pytest

from ingestion.conftest import metadata
from ingestion.test_population_reconciliation import (
    acquire,
    checkpoint,
    checkpoint_bytes,
    runner,
    signed,
)
from observatorio_secop.ingestion.campaign import consumed
from observatorio_secop.ingestion.errors import ResumeError
from observatorio_secop.ingestion.storage import canonical_json, durable_write


def named(instance, **overrides):
    return acquire(
        instance,
        **{"campaign_id": "fixed-campaign", "invocation_key": "grant-1", **overrides},
    )


def test_campaign_is_reserved_before_first_effect_and_survives_initial_crash(
    config, contract, monkeypatch
):
    calls = []
    instance = runner(
        config, contract, [metadata(contract), []], campaign_id="fixed-campaign", calls=calls
    )
    before = checkpoint(instance)
    start = instance._start

    def crash(parameters, **kwargs):
        manifest = start(parameters, **kwargs)
        assert manifest.run_id == "fixed-campaign"
        raise KeyboardInterrupt()

    monkeypatch.setattr(instance, "_start", crash)
    with pytest.raises(KeyboardInterrupt):
        named(instance)
    assert not calls
    monkeypatch.setattr(instance, "_start", start)
    result = named(instance)
    assert result.range_complete and len(calls) == 2
    assert len(result.population["invocations"]) == 1
    assert result.population["invocations"][0]["reservation_key"] == "grant-1"
    assert checkpoint_bytes(instance) == before


def test_scheduler_retry_does_not_append_or_refill_a_paused_grant(config, contract):
    calls = []
    instance = runner(
        config,
        contract,
        [metadata(contract), [signed("A"), signed("B")], []],
        campaign_id="fixed-campaign",
        calls=calls,
    )
    paused = named(instance, grant_rows=2, grant_requests=2)
    assert paused.status == "paused_budget"
    retried = named(instance, grant_rows=2, grant_requests=2)
    assert retried.status == "paused_budget" and len(calls) == 2
    assert len(retried.population["invocations"]) == 1
    assert consumed(retried) == {"rows": 2, "attempts": 2}
    extended = named(instance, invocation_key="grant-2", grant_rows=10, grant_requests=10)
    assert extended.range_complete and len(calls) == 3
    assert [i["reservation_key"] for i in extended.population["invocations"]] == [
        "grant-1",
        "grant-2",
    ]
    with pytest.raises(ResumeError, match="superseded"):
        named(instance, grant_rows=2, grant_requests=2)


def test_uncertain_reservation_stays_charged_inside_original_named_grant(config, contract):
    calls = []
    instance = runner(
        config,
        contract,
        [metadata(contract), KeyboardInterrupt(), []],
        campaign_id="fixed-campaign",
        calls=calls,
    )
    before = checkpoint(instance)
    with pytest.raises(KeyboardInterrupt):
        named(instance, grant_rows=4, grant_requests=3)
    recovered = named(instance, grant_rows=4, grant_requests=3)
    assert recovered.range_complete and len(calls) == 3
    assert len(recovered.population["invocations"]) == 1
    assert consumed(recovered) == {"rows": 2, "attempts": 3}
    assert recovered.population["reservations"][1]["state"] == "uncertain"
    assert checkpoint_bytes(instance) == before


def test_committed_campaign_lost_output_is_exact_noop_and_checks_grant_identity(
    config, contract, monkeypatch
):
    calls = []
    instance = runner(
        config, contract, [metadata(contract), []], campaign_id="fixed-campaign", calls=calls
    )
    mark = instance.storage.mark_committed

    def lose_ack(manifest):
        mark(manifest)
        raise RuntimeError("lost output")

    monkeypatch.setattr(instance.storage, "mark_committed", lose_ack)
    with pytest.raises(RuntimeError, match="lost output"):
        named(instance)
    monkeypatch.setattr(instance.storage, "mark_committed", mark)
    recovered = named(instance)
    again = named(instance)
    assert recovered.status == "committed" and recovered.as_dict() == again.as_dict()
    assert len(calls) == 2 and len(recovered.population["invocations"]) == 1
    with pytest.raises(ResumeError, match="changed grant"):
        named(instance, grant_rows=19)
    with pytest.raises(ResumeError, match="changed"):
        named(instance, total_rows=21)


@pytest.mark.parametrize(
    "overrides",
    [
        {"campaign_id": "../outside"},
        {"invocation_key": "../outside"},
        {"campaign_id": None},
        {"invocation_key": None},
        {"resume": "another-campaign"},
    ],
)
def test_invalid_named_identity_fails_before_requests(config, contract, overrides):
    calls = []
    instance = runner(config, contract, [], campaign_id="fixed-campaign", calls=calls)
    with pytest.raises(ResumeError):
        named(instance, **overrides)
    assert not calls


def test_named_grant_tampering_fails_verification(config, contract):
    instance = runner(config, contract, [metadata(contract), []], campaign_id="fixed-campaign")
    result = named(instance)
    result.population["invocations"][0]["reservation_key"] = "../forged"
    durable_write(
        instance.storage.run_path(result.run_id) / "manifest.json",
        canonical_json(result.as_dict()),
    )
    with pytest.raises(ResumeError, match="named invocation"):
        instance.inspect_campaign(result.run_id)


def test_named_identity_is_persisted_before_physical_request(config, contract):
    instance = runner(config, contract, [metadata(contract), []], campaign_id="fixed-campaign")
    original = instance.transport._open

    def inspect(request, timeout):
        saved = json.loads(
            (config.bronze_root / "staging/fixed-campaign/page-state.json").read_bytes()
        )
        assert saved["run_id"] == "fixed-campaign"
        assert saved["parameters"]["reservation_mode"] == "named-v1"
        assert saved["population"]["invocations"][-1]["reservation_key"] == "grant-1"
        return original(request, timeout)

    instance.transport._open = inspect
    assert named(instance).range_complete
