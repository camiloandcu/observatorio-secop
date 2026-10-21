"""Deterministic HTTP-boundary proofs for finite population acquisition."""

from __future__ import annotations

import json
from collections import deque
from datetime import UTC, datetime
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest

from ingestion.conftest import metadata, row
from ingestion.test_transport import Response
from observatorio_secop.identity import key_set_digest
from observatorio_secop.ingestion.campaign import consumed
from observatorio_secop.ingestion.errors import PageValidationError, ResumeError
from observatorio_secop.ingestion.reconcile import PopulationReconciliation
from observatorio_secop.ingestion.runner import BronzeIngestion
from observatorio_secop.ingestion.storage import canonical_json, durable_write
from observatorio_secop.ingestion.transport import SocrataTransport


def signed(identity: str, signing: str = "2020-06-01T00:00:00.000") -> dict:
    return {**row(identity, None), "fecha_de_firma": signing}


def runner(config, contract, outcomes, *, campaign_id="campaign-1", calls=None):
    queue = deque(outcomes)
    urls = calls if calls is not None else []

    def opener(request, timeout):
        # Prove the reservation is already on durable storage at the physical boundary.
        saved = json.loads(
            (config.bronze_root / "staging" / campaign_id / "page-state.json").read_bytes()
        )
        assert saved["population"]["reservations"][-1]["state"] == "uncertain"
        assert saved["population"]["acquisition"]["consumed"]["attempts"] > len(urls)
        urls.append(request.full_url)
        outcome = queue.popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        return Response(json.dumps(outcome).encode())

    transport = SocrataTransport(config, contract, opener=opener, sleeper=lambda _: None)
    return PopulationReconciliation(
        config,
        contract,
        transport=transport,
        clock=lambda: datetime(2026, 10, 21, tzinfo=UTC),
        uuid_factory=lambda: campaign_id,
    )


def acquire(instance, **overrides):
    return instance.reconcile(
        "2020-01-01",
        "2021-01-01",
        **{
            "total_rows": 20,
            "total_requests": 20,
            "grant_rows": 20,
            "grant_requests": 20,
            **overrides,
        },
    )


def checkpoint(instance):
    with instance.state.connect() as connection:
        connection.execute(
            "INSERT INTO checkpoints VALUES ('jbjy-vk9h', 'primary', '2026-10-01Z', 'old')"
        )
    return checkpoint_bytes(instance)


def checkpoint_bytes(instance):
    with instance.state.connect() as connection:
        rows = connection.execute("SELECT * FROM checkpoints ORDER BY dataset_id, lane").fetchall()
    return canonical_json(rows)


def test_old_signature_null_update_and_complete_resume_make_no_watermark_query(config, contract):
    calls = []
    instance = runner(config, contract, [metadata(contract), [signed("A")]], calls=calls)
    before = checkpoint(instance)
    manifest = acquire(instance)
    assert manifest.status == "committed" and manifest.range_complete
    assert manifest.purpose == "population_reconciliation"
    query = parse_qs(urlsplit(calls[1]).query)
    assert "ultima_actualizacion" not in query["$where"][0]
    assert "fecha_de_firma >= '2020-01-01'" in query["$where"][0]
    assert "fecha_de_firma < '2021-01-01'" in query["$where"][0]
    assert instance.state.checkpoint("jbjy-vk9h") == "2026-10-01Z"
    assert checkpoint_bytes(instance) == before and manifest.checkpoint_candidate is None
    again = acquire(instance, resume=manifest.run_id)
    assert again.as_dict() == manifest.as_dict() and len(calls) == 2


def test_exact_full_scope_page_is_partial_without_budget_for_terminal_probe(config, contract):
    calls = []
    instance = runner(
        config, contract, [metadata(contract), [signed("A"), signed("B")]], calls=calls
    )
    manifest = acquire(instance, total_rows=2, grant_rows=2)
    assert manifest.status == "committed" and manifest.truncated and not manifest.range_complete
    assert manifest.population["acquisition"]["state"] == "truncated"
    assert manifest.population["terminal_reason"] == "campaign_rows_exhausted"
    assert len(calls) == 2
    acquire(instance, total_rows=2, grant_rows=2, resume=manifest.run_id)
    assert len(calls) == 2


def test_invocation_pause_and_explicit_resume_preserve_cursor(config, contract):
    calls = []
    instance = runner(
        config, contract, [metadata(contract), [signed("A"), signed("B")], []], calls=calls
    )
    paused = acquire(instance, grant_rows=2, grant_requests=2)
    assert paused.status == "paused_budget"
    assert paused.population["acquisition"]["state"] == "paused_budget"
    assert consumed(paused) == {"rows": 2, "attempts": 2}
    resumed = acquire(instance, grant_rows=10, grant_requests=10, resume=paused.run_id)
    assert resumed.range_complete and len(calls) == 3
    assert "id_contrato > 'B'" in parse_qs(urlsplit(calls[-1]).query)["$where"][0]
    assert len(resumed.population["invocations"]) == 2
    assert resumed.population["invocations"][0]["terminal_reason"] == "invocation_rows_exhausted"
    assert resumed.population["invocations"][0]["state"] == "paused_budget"
    assert resumed.population["invocations"][1]["state"] == "acquired"
    assert consumed(resumed) == {"rows": 2, "attempts": 3}


def test_fixed_full_baseline_batch_needs_no_extra_probe_and_retains_drift(
    config, contract, monkeypatch
):
    binding = {
        "version_id": "baseline",
        "manifest_sha256": "a" * 64,
        "bronze_runs": [{"run_id": "old", "manifest_sha256": "b" * 64}],
        "ids": ["A", "B"],
        "ids_sha256": key_set_digest(["A", "B"]),
        "keys_sha256": key_set_digest(["key-A", "key-B"]),
    }
    monkeypatch.setattr(
        "observatorio_secop.ingestion.reconcile.baseline_binding", lambda *_: binding
    )
    calls = []
    instance = runner(
        config,
        contract,
        [
            metadata(contract),
            [],
            [signed("A", "2021-02-01T00:00:00"), signed("B")],
        ],
        calls=calls,
    )
    manifest = acquire(instance, total_requests=3, grant_requests=3)
    assert manifest.range_complete and len(calls) == 3
    assert "fecha_de_firma" not in parse_qs(urlsplit(calls[-1]).query)["$where"][0]
    assert manifest.pages[-1].row_count == 2
    assert manifest.population["acquisition"]["unconfirmed_ids"] == []


def test_absent_baseline_member_is_unconfirmed_not_deletion(config, contract, monkeypatch):
    binding = {
        "version_id": "baseline",
        "manifest_sha256": "a" * 64,
        "bronze_runs": [{"run_id": "old", "manifest_sha256": "b" * 64}],
        "ids": ["A"],
        "ids_sha256": key_set_digest(["A"]),
        "keys_sha256": key_set_digest(["key-A"]),
    }
    monkeypatch.setattr(
        "observatorio_secop.ingestion.reconcile.baseline_binding", lambda *_: binding
    )
    instance = runner(config, contract, [metadata(contract), [], []])
    manifest = acquire(instance)
    assert manifest.range_complete
    assert manifest.population["acquisition"]["unconfirmed_ids"] == ["A"]


@pytest.mark.parametrize(
    "bad",
    [
        [signed("A"), signed("A")],
        [signed("B"), signed("A")],
        [signed("A", "2021-01-01T00:00:00")],
        [signed("A", "2019-12-31T00:00:00")],
        [{**signed("A"), "fecha_de_firma": None}],
        [{**signed("A"), "departamento": "Caldas"}],
    ],
)
def test_inadmissible_discovery_fails_without_committing(config, contract, bad):
    instance = runner(config, contract, [metadata(contract), bad])
    checkpoint(instance)
    with pytest.raises(PageValidationError):
        acquire(instance)
    report = instance.inspect_campaign("campaign-1")
    assert report["status"] == "failed_recoverable" and report["rows"] == 0
    assert instance.state.checkpoint("jbjy-vk9h") == "2026-10-01Z"


def test_http_retry_counts_and_reserves_rows_before_every_attempt(config, contract):
    calls = []
    instance = runner(
        config,
        contract,
        [
            metadata(contract),
            HTTPError("safe", 503, "down", {}, None),
            [signed("A")],
        ],
        calls=calls,
    )
    manifest = acquire(instance)
    assert len(calls) == 3
    assert consumed(manifest) == {"rows": 3, "attempts": 3}
    assert manifest.population["reservations"][1]["state"] == "uncertain"
    assert manifest.population["reservations"][1]["charged_rows"] == 2


def test_metadata_retries_cannot_exceed_campaign_attempt_ceiling(config, contract):
    calls = []
    instance = runner(config, contract, [HTTPError("safe", 503, "down", {}, None)], calls=calls)
    manifest = acquire(instance, total_requests=1, grant_requests=1)
    assert manifest.truncated and len(calls) == 1 and consumed(manifest)["attempts"] == 1
    assert manifest.population["metadata_verified"] is False


def test_hard_crash_reservation_remains_charged_on_resume(config, contract):
    instance = runner(config, contract, [metadata(contract), KeyboardInterrupt(), []])
    with pytest.raises(KeyboardInterrupt):
        acquire(instance)
    report = instance.inspect_campaign("campaign-1")
    assert report["acquisition"]["consumed"] == {"rows": 2, "attempts": 2}
    manifest = acquire(instance, grant_rows=10, grant_requests=10, resume="campaign-1")
    assert manifest.range_complete and consumed(manifest) == {"rows": 2, "attempts": 3}
    assert manifest.population["invocations"][0]["state"] == "interrupted"


def test_changed_resume_inputs_and_tampered_pages_are_rejected(config, contract):
    instance = runner(config, contract, [metadata(contract), [signed("A"), signed("B")]])
    paused = acquire(instance, grant_rows=2, grant_requests=2)
    with pytest.raises(ResumeError, match="changed"):
        acquire(instance, total_rows=30, resume=paused.run_id)
    path = instance.storage.staging_path(paused.run_id) / paused.pages[0].path
    path.write_bytes(b"[]")
    with pytest.raises(ResumeError, match="hash"):
        acquire(instance, resume=paused.run_id)


def test_prepared_recovery_dispatches_population_without_checkpoint(config, contract, monkeypatch):
    instance = runner(config, contract, [metadata(contract), []])
    checkpoint(instance)
    original = instance.storage.mark_committed
    monkeypatch.setattr(
        instance.storage,
        "mark_committed",
        lambda _: (_ for _ in ()).throw(RuntimeError("lost ack")),
    )
    with pytest.raises(RuntimeError, match="lost ack"):
        acquire(instance)
    monkeypatch.setattr(instance.storage, "mark_committed", original)
    # Generic incremental recovery must preserve the distinct purpose and checkpoint.
    assert BronzeIngestion(config, contract).reconcile_prepared() == ["campaign-1"]
    manifest = instance.storage.load_published(instance.storage.run_path("campaign-1"))
    assert manifest.status == "committed" and manifest.purpose == "population_reconciliation"
    assert instance.state.checkpoint("jbjy-vk9h") == "2026-10-01Z"


def test_ledger_tampering_is_not_accepted_as_completion(config, contract):
    instance = runner(config, contract, [metadata(contract), []])
    manifest = acquire(instance)
    manifest.population["reservations"][1]["charged_rows"] = 1
    durable_write(
        instance.storage.run_path(manifest.run_id) / "manifest.json",
        canonical_json(manifest.as_dict()),
    )
    with pytest.raises(ResumeError, match="receipt"):
        instance.inspect_campaign(manifest.run_id)


def test_durable_terminal_pages_recover_without_refilling_exhausted_attempts(
    config, contract, monkeypatch
):
    calls = []
    instance = runner(config, contract, [metadata(contract), []], calls=calls)
    before = checkpoint(instance)
    prepare = instance.storage.prepare

    def crash(_manifest):
        raise KeyboardInterrupt()

    monkeypatch.setattr(instance.storage, "prepare", crash)
    with pytest.raises(KeyboardInterrupt):
        acquire(instance, total_requests=2, grant_requests=2)
    monkeypatch.setattr(instance.storage, "prepare", prepare)
    manifest = acquire(instance, total_requests=2, grant_requests=2, resume="campaign-1")
    assert manifest.range_complete and not manifest.truncated and len(calls) == 2
    assert len(manifest.population["invocations"]) == 1
    assert checkpoint_bytes(instance) == before
