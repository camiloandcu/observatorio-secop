"""Cancellation cleanup and diagnostics remain safe across a real process boundary."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from observatorio_secop.orchestration import process, runner
from observatorio_secop.orchestration.models import PipelineError, Runtime
from observatorio_secop.orchestration.receipts import Receipts


def parameters():
    return {
        "mode": "demo",
        "fixture_id": "demo-a",
        "signing_from": "2026-01-01",
        "signing_to": "2026-02-01",
        "baseline_version": None,
        "baseline_sha256": None,
        "total_rows": 100,
        "total_requests": 20,
        "grant_rows": 100,
        "grant_requests": 20,
        "page_size": 50,
        "build_seconds": 180,
        "storage_bytes": 67108864,
    }


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    value = Runtime(
        tmp_path / "project",
        tmp_path / "state",
        tmp_path / "bronze",
        tmp_path / "silver",
        tmp_path / "reports",
        "demo",
    )
    monkeypatch.setattr(Runtime, "environment", classmethod(lambda _: value))
    return value


def _live(pid: int) -> bool:
    path = Path(f"/proc/{pid}/stat")
    return path.exists() and path.read_text().split()[2] != "Z"


@pytest.mark.parametrize("ending", ["failure", "success", "deadline", "cancel"])
def test_descendant_ignoring_sigterm_is_killed_even_after_leader_exit(
    tmp_path, monkeypatch, ending
):
    pid_path = tmp_path / "child.pid"
    child = (
        "import os,signal,time;from pathlib import Path;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        f"Path({str(pid_path)!r}).write_text(str(os.getpid()));time.sleep(30)"
    )
    leader = (
        "import os,signal,subprocess,sys,time;from pathlib import Path;"
        f"subprocess.Popen([sys.executable,'-c',{child!r}]);"
        f"p=Path({str(pid_path)!r})\n"
        "while not p.exists(): time.sleep(0.005)\n"
    )
    if ending == "cancel":
        leader += "time.sleep(0.1);os.kill(os.getppid(),signal.SIGTERM);time.sleep(30)"
    elif ending == "deadline":
        leader += "time.sleep(30)"
    else:
        leader += f"sys.exit({75 if ending == 'failure' else 0})"
    real_popen = subprocess.Popen
    monkeypatch.setattr(
        process.subprocess,
        "Popen",
        lambda _args, **kwargs: real_popen([sys.executable, "-c", leader], **kwargs),
    )
    monkeypatch.setattr(process, "TERMINATION_GRACE_SECONDS", 0.1)
    started = time.monotonic()
    try:
        with (tmp_path / "private.log").open("wb") as output:
            if ending in ("deadline", "cancel"):
                with pytest.raises(PipelineError, match="PIPELINE_TRANSIENT"):
                    process.private_worker(b"{}", output, timeout=0.4, cwd=str(tmp_path))
            else:
                assert process.private_worker(b"{}", output, timeout=5, cwd=str(tmp_path)) == (
                    75 if ending == "failure" else 0
                )
        pid = int(pid_path.read_text())
        deadline = time.monotonic() + 2
        while _live(pid) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not _live(pid)
        assert time.monotonic() - started < 5
    finally:
        if pid_path.exists():
            pid = int(pid_path.read_text())
            if _live(pid):
                os.kill(pid, signal.SIGKILL)


def test_log_directory_escape_is_rejected_before_writes(runtime, tmp_path, monkeypatch):
    outside = tmp_path / "outside"
    outside.mkdir()
    runtime.reports.mkdir()
    (runtime.reports / "private-task-logs").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(process, "private_worker", lambda *_a, **_k: pytest.fail("Worker ran"))
    with pytest.raises(PipelineError, match="PIPELINE_UNSAFE_PATH"):
        runner.run_stage("validate", "fixed", parameters())
    assert not list(outside.iterdir())


def test_mode_mismatch_is_rejected_before_logging(runtime, monkeypatch):
    monkeypatch.setattr(process, "private_worker", lambda *_a, **_k: pytest.fail("Worker ran"))
    with pytest.raises(PipelineError, match="PIPELINE_RUNTIME_MODE_MISMATCH"):
        runner.run_stage("validate", "fixed", parameters() | {"mode": "live", "fixture_id": None})
    assert not runtime.reports.exists()


@pytest.mark.parametrize(
    "returncode,final,expected",
    [
        (2, {"code": "PIPELINE_INPUT_CHANGED"}, "PIPELINE_INPUT_CHANGED"),
        (2, {"code": "PUBLICATION_STALE"}, "PUBLICATION_STALE"),
        (2, {"code": "password=SECRET_CANARY"}, "PIPELINE_STAGE_REJECTED"),
        (75, {"code": "PUBLICATION_STALE"}, "PIPELINE_TRANSIENT"),
    ],
)
def test_worker_safe_error_survives_stale_receipt_without_raw_output(
    runtime, monkeypatch, capsys, returncode, final, expected
):
    receipts = Receipts(runtime.state, "fixed")
    with receipts.locked():
        document = receipts.create(parameters(), {})
        document["stages"] = {"acquire": {"status": "failed", "code": "PIPELINE_OLD_ERROR"}}
        receipts.write(document)

    def failed(_payload, output, **_kwargs):
        output.write(b"private-path/SECRET_CANARY raw producer output\n")
        output.write(json.dumps(final).encode() + b"\n")
        return returncode

    monkeypatch.setattr(process, "private_worker", failed)
    with pytest.raises(PipelineError, match=expected):
        runner.run_stage("acquire", "fixed")
    captured = capsys.readouterr()
    assert "SECRET_CANARY" not in captured.out + captured.err
    logs = list(runtime.reports.rglob("*.log"))
    assert len(logs) == 1 and logs[0].stat().st_mode & 0o777 == 0o600


def test_runner_cli_preserves_safe_code(runtime, monkeypatch, capsys):
    def changed(*_args, **_kwargs):
        raise PipelineError("PIPELINE_INPUT_CHANGED")

    monkeypatch.setattr(runner, "run_stage", changed)
    receipts = Receipts(runtime.state, "fixed")
    with receipts.locked():
        receipts.create(parameters(), {})
    assert runner.main(["stage", "--execution-id", "fixed", "--stage", "acquire"]) == 2
    assert json.loads(capsys.readouterr().err) == {"code": "PIPELINE_INPUT_CHANGED"}
