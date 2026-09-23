from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from conftest import FakeTransport, metadata, row

from observatorio_secop.ingestion.errors import PageValidationError, ResumeError, TransportError
from observatorio_secop.ingestion.logging import EventLogger
from observatorio_secop.ingestion.runner import BronzeIngestion
from observatorio_secop.ingestion.validation import validate_metadata, validate_page

START = "2026-01-01T00:00:00Z"
END = "2026-01-03T00:00:00Z"


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 1, 4, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.value += timedelta(seconds=1)
        return self.value


def run_ingestion(config, contract, transport, **kwargs):
    return BronzeIngestion(
        config,
        contract,
        transport=transport,
        clock=Clock(),
        uuid_factory=lambda: "run-1",
        logger=EventLogger(lambda _: None),
    ).run(START, END, **kwargs)


def test_multiple_pages_ties_two_lanes_and_partial_last_page(config, contract) -> None:
    primary = [row("A"), row("B")]
    last = [row("C", "2026-01-02T00:00:00.000")]
    null_lane = [row("N", None)]
    transport = FakeTransport(metadata(contract), [primary, last, null_lane])

    manifest = run_ingestion(config, contract, transport)

    assert manifest.status == "committed"
    assert manifest.range_complete is True
    assert manifest.truncated is False
    assert manifest.counts["written"] == 4
    assert manifest.lanes["null_watermark"]["captures_updates"] is False
    assert len(transport.queries) == 3
    assert "id_contrato > 'B'" in transport.queries[1]["$where"]
    assert manifest.checkpoint_candidate == "2026-01-03T00:00:00.000Z"
    assert (config.bronze_root / "runs/run-1/manifest.json").exists()
    assert not (config.bronze_root / "staging/run-1").exists()


def test_empty_range_commits_zero_rows_and_advances_completed_time(config, contract) -> None:
    transport = FakeTransport(metadata(contract), [[], []])
    manifest = run_ingestion(config, contract, transport)
    assert manifest.counts["written"] == 0
    assert manifest.range_complete
    assert manifest.checkpoint_candidate == "2026-01-03T00:00:00.000Z"


def test_development_limit_truncates_without_checkpoint(config, contract) -> None:
    limited = replace(config, max_rows=2)
    transport = FakeTransport(metadata(contract), [[row("A"), row("B")]])
    manifest = run_ingestion(limited, contract, transport)
    assert manifest.truncated
    assert not manifest.range_complete
    assert manifest.checkpoint_candidate is None
    engine = BronzeIngestion(limited, contract, transport=transport)
    assert engine.state.checkpoint(contract.dataset_id) is None


def test_non_monotonic_page_fails_and_preserves_checkpoint(config, contract) -> None:
    transport = FakeTransport(metadata(contract), [[row("B"), row("A")]])
    with pytest.raises(PageValidationError, match="did not progress"):
        run_ingestion(config, contract, transport)
    assert not (config.bronze_root / "runs/run-1").exists()


class InterruptingTransport(FakeTransport):
    def query(self, params, **kwargs):
        if not self.pages:
            raise TransportError("simulated interruption")
        return super().query(params, **kwargs)


def test_interruption_resumes_after_last_verified_page(config, contract) -> None:
    interrupted = InterruptingTransport(metadata(contract), [[row("A"), row("B")]])
    with pytest.raises(TransportError):
        run_ingestion(config, contract, interrupted)
    assert (config.bronze_root / "staging/run-1/pages/primary-00001.json").exists()

    resumed_transport = FakeTransport(metadata(contract), [[row("C")], []])
    manifest = run_ingestion(config, contract, resumed_transport, resume="run-1")
    assert manifest.status == "committed"
    assert manifest.counts["written"] == 3
    assert "id_contrato > 'B'" in resumed_transport.queries[0]["$where"]


def test_partial_unreferenced_page_is_ignored_and_requested_again(config, contract) -> None:
    interrupted = InterruptingTransport(metadata(contract), [[row("A"), row("B")]])
    with pytest.raises(TransportError):
        run_ingestion(config, contract, interrupted)
    partial = config.bronze_root / "staging/run-1/pages/.primary-00002.json.tmp"
    partial.write_bytes(b"partial")

    resumed = FakeTransport(metadata(contract), [[row("C")], []])
    manifest = run_ingestion(config, contract, resumed, resume="run-1")
    assert manifest.counts["written"] == 3
    assert len(resumed.queries) == 2


def test_resume_rejects_changed_parameters_and_tampered_page(config, contract) -> None:
    interrupted = InterruptingTransport(metadata(contract), [[row("A"), row("B")]])
    with pytest.raises(TransportError):
        run_ingestion(config, contract, interrupted)

    changed = replace(config, max_rows=config.max_rows + 1)
    with pytest.raises(ResumeError, match="parameters"):
        run_ingestion(
            changed,
            contract,
            FakeTransport(metadata(contract), []),
            resume="run-1",
        )
    page = config.bronze_root / "staging/run-1/pages/primary-00001.json"
    page.write_bytes(b"[]")
    with pytest.raises(ResumeError, match="hash"):
        run_ingestion(config, contract, FakeTransport(metadata(contract), []), resume="run-1")


def test_repeated_run_is_logically_idempotent_and_changed_content_versions(
    config, contract
) -> None:
    first = FakeTransport(metadata(contract), [[row("A")], []])
    first_manifest = run_ingestion(config, contract, first)
    assert first_manifest.counts["new"] == 1

    second_engine = BronzeIngestion(
        config,
        contract,
        transport=FakeTransport(metadata(contract), [[row("A")], []]),
        clock=Clock(),
        uuid_factory=lambda: "run-2",
        logger=EventLogger(lambda _: None),
    )
    second = second_engine.run(START, END)
    assert second.counts["repeated"] == 1

    changed_row = {**row("A"), "estado_contrato": "Modificado"}
    third_engine = BronzeIngestion(
        config,
        contract,
        transport=FakeTransport(metadata(contract), [[changed_row], []]),
        clock=Clock(),
        uuid_factory=lambda: "run-3",
        logger=EventLogger(lambda _: None),
    )
    third = third_engine.run(START, END)
    assert third.counts["new_versions"] == 1


def test_schema_failures_and_additive_drift(config, contract) -> None:
    missing = metadata(contract)
    missing["columns"] = [
        column for column in missing["columns"] if column["fieldName"] != "id_contrato"
    ]
    with pytest.raises(Exception, match="critical"):
        validate_metadata(missing, contract)
    with pytest.raises(PageValidationError, match="expected"):
        validate_page(
            [
                {
                    "id_contrato": "A",
                    "departamento": 42,
                    "ciudad": "Medellín",
                    "ultima_actualizacion": "2026-01-01",
                }
            ],
            contract,
            lane="primary",
        )
    assert validate_metadata(metadata(contract, extra="new_optional"), contract) == ["new_optional"]


def test_logs_redact_token_and_payload_sentinels() -> None:
    lines = []
    logger = EventLogger(lines.append, secrets=("secret-token", "secret-row"))
    logger.emit(
        "safe_event",
        run_id="run",
        token="secret-token",
        message="contains secret-token and secret-row",
        payload={"value": "secret-row"},
    )
    serialized = lines[0]
    assert "secret-token" not in serialized
    assert "secret-row" not in serialized
    assert serialized.count("[REDACTED]") >= 2


def test_prepared_run_is_reconciled_after_state_commit_crash(config, contract, monkeypatch) -> None:
    engine = BronzeIngestion(
        config,
        contract,
        transport=FakeTransport(metadata(contract), [[row("A")], []]),
        clock=Clock(),
        uuid_factory=lambda: "run-1",
        logger=EventLogger(lambda _: None),
    )
    original_commit = engine.state.commit

    def crash(*args, **kwargs):
        raise OSError("simulated crash")

    monkeypatch.setattr(engine.state, "commit", crash)
    with pytest.raises(OSError):
        engine.run(START, END)
    assert (config.bronze_root / "runs/run-1/manifest.json").exists()
    assert engine.state.checkpoint(contract.dataset_id) is None

    recovery = BronzeIngestion(
        config,
        contract,
        transport=FakeTransport(metadata(contract), []),
        logger=EventLogger(lambda _: None),
    )
    monkeypatch.setattr(recovery.state, "commit", original_commit)
    assert recovery.reconcile_prepared() == ["run-1"]
    assert recovery.state.checkpoint(contract.dataset_id) == "2026-01-03T00:00:00.000Z"
    saved = json.loads((config.bronze_root / "runs/run-1/manifest.json").read_text())
    assert saved["status"] == "committed"


def test_reconciliation_after_state_commit_only_marks_manifest(config, contract) -> None:
    engine = BronzeIngestion(
        config,
        contract,
        transport=FakeTransport(metadata(contract), [[row("A")], []]),
        clock=Clock(),
        uuid_factory=lambda: "run-1",
        logger=EventLogger(lambda _: None),
    )
    original_mark = engine.storage.mark_committed
    engine.storage.mark_committed = lambda _manifest: None
    manifest = engine.run(START, END)
    assert manifest.status == "prepared"
    assert engine.state.has_run("run-1")
    engine.storage.mark_committed = original_mark
    assert engine.reconcile_prepared() == ["run-1"]
    saved = json.loads((config.bronze_root / "runs/run-1/manifest.json").read_text())
    assert saved["status"] == "committed"


def test_page_write_failure_preserves_checkpoint_and_does_not_publish(
    config, contract, monkeypatch
) -> None:
    engine = BronzeIngestion(
        config,
        contract,
        transport=FakeTransport(metadata(contract), [[row("A")]]),
        clock=Clock(),
        uuid_factory=lambda: "run-1",
        logger=EventLogger(lambda _: None),
    )

    def fail_write(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(engine.storage, "write_page", fail_write)
    with pytest.raises(OSError, match="disk full"):
        engine.run(START, END)
    assert engine.state.checkpoint(contract.dataset_id) is None
    assert not (config.bronze_root / "runs/run-1").exists()
