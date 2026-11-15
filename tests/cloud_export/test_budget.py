from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from observatorio_secop.cloud_export.budget import Budget
from observatorio_secop.cloud_export.build import _rename_no_replace, phase
from observatorio_secop.cloud_export.errors import ExportError


def test_row_and_batch_limits_are_inclusive():
    Budget(row_bytes=4, batch_bytes=8).admit([4, 4])
    for sizes in ([5], [4, 4, 1], [0] * 1001):
        with pytest.raises(ExportError):
            Budget(row_bytes=4, batch_bytes=8).admit(sizes)
    budget = Budget(max_rows=2)
    budget.admit([0, 0])
    with pytest.raises(ExportError):
        budget.admit([0])


def test_inventory_limits_are_inclusive(tmp_path):
    (tmp_path / "one").write_bytes(b"1234")
    Budget(max_files=1, output_bytes=4).files(tmp_path)
    with pytest.raises(ExportError):
        Budget(max_files=1, output_bytes=3).files(tmp_path)
    (tmp_path / "two").write_bytes(b"")
    with pytest.raises(ExportError):
        Budget(max_files=1).files(tmp_path)


def test_observed_memory_and_remaining_deadline():
    with patch("observatorio_secop.cloud_export.budget.rss_bytes", return_value=100):
        budget = Budget(memory_bytes=10, baseline=100)
    with patch("observatorio_secop.cloud_export.budget.rss_bytes", return_value=110):
        budget.check()
    with (
        patch("observatorio_secop.cloud_export.budget.rss_bytes", return_value=111),
        pytest.raises(ExportError),
    ):
        budget.check()
    budget = Budget(seconds=1)
    with patch(
        "observatorio_secop.cloud_export.budget.time.monotonic", return_value=budget.started + 0.999
    ):
        assert budget.remaining_ms() <= 2
    with (
        patch(
            "observatorio_secop.cloud_export.budget.time.monotonic", return_value=budget.started + 1
        ),
        pytest.raises(ExportError),
    ):
        budget.check()


def test_atomic_no_replace_never_overwrites(tmp_path):
    a, b = tmp_path / "source", tmp_path / "target"
    a.mkdir()
    b.mkdir()
    (b / "prior").write_text("preserved")
    with pytest.raises(ExportError):
        _rename_no_replace(a, b)
    assert (b / "prior").read_text() == "preserved" and a.exists()
    b.rmdir() if not list(b.iterdir()) else (b / "prior").unlink()
    b.rmdir()
    _rename_no_replace(a, b)
    assert b.is_dir() and not a.exists()


def test_worker_is_terminated_on_global_deadline(tmp_path, monkeypatch):
    import subprocess

    original = subprocess.Popen
    children = []

    def sleeping(*args, **kwargs):
        process = original(["sleep", "30"], start_new_session=True)
        children.append(process)
        return process

    monkeypatch.setattr("observatorio_secop.cloud_export.build.subprocess.Popen", sleeping)
    with pytest.raises(ExportError, match="deadline"):
        phase("validate", {}, tmp_path, Budget(seconds=0.15))
    assert children[0].poll() is not None


def test_unconfirmed_kill_preserves_scratch_and_safe_limit_error(tmp_path, monkeypatch):
    import subprocess

    from observatorio_secop.cloud_export.build import owned_scratch

    class Unreaped:
        pid = 99999999
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("private worker arguments", timeout)

    signals = []
    monkeypatch.setattr(
        "observatorio_secop.cloud_export.build.subprocess.Popen", lambda *a, **k: Unreaped()
    )
    monkeypatch.setattr(
        "observatorio_secop.cloud_export.build.os.killpg", lambda pid, sig: signals.append(sig)
    )
    retained = None
    try:
        with owned_scratch("secop-export-test-") as scratch:
            retained = scratch
            with pytest.raises(ExportError) as failure:
                phase("verify", {}, scratch, Budget(seconds=0.02))
            assert failure.value.code == "EXPORT_LIMIT"
            assert "private worker arguments" not in str(failure.value)
        assert retained.exists() and (retained / "worker-unreaped.json").exists()
        assert len(signals) == 2
    finally:
        if retained is not None:
            import shutil

            shutil.rmtree(retained)


@pytest.mark.parametrize("failure_mode", ["signal", "journal"])
def test_cleanup_needs_positive_exit_even_when_signal_or_journal_fails(
    tmp_path, monkeypatch, failure_mode
):
    import shutil
    import subprocess

    from observatorio_secop.cloud_export.build import owned_scratch

    class Unreaped:
        pid = 99999999
        returncode = None

        def poll(self):
            return None

        def wait(self, timeout):
            raise subprocess.TimeoutExpired("private worker arguments", timeout)

    monkeypatch.setattr(
        "observatorio_secop.cloud_export.build.subprocess.Popen", lambda *a, **k: Unreaped()
    )

    def signal_failure(*args):
        raise PermissionError("private signal details")

    monkeypatch.setattr(
        "observatorio_secop.cloud_export.build.os.killpg",
        signal_failure if failure_mode == "signal" else lambda *a: None,
    )
    original = Path.write_bytes

    def failed_journal(path, data):
        if path.name == "worker-unreaped.json":
            raise OSError("private disk details")
        return original(path, data)

    if failure_mode == "journal":
        monkeypatch.setattr(Path, "write_bytes", failed_journal)
    retained = None
    try:
        with owned_scratch("secop-export-test-") as scratch:
            retained = scratch
            with pytest.raises(ExportError) as error:
                phase("verify", {}, scratch, Budget(seconds=0.02))
            assert error.value.code == "EXPORT_LIMIT" and "private" not in str(error.value)
        assert retained.exists()
    finally:
        if retained is not None:
            shutil.rmtree(retained)
