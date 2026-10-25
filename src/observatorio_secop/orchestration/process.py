"""Terminate the entire task producer group on deadline or scheduler cancellation."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from collections.abc import Mapping
from contextlib import suppress
from typing import BinaryIO

from .models import PipelineError

TERMINATION_GRACE_SECONDS = 2.0


def _group_exists(group: int) -> bool:
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    return True


def _terminate_group(process: subprocess.Popen, grace: float) -> None:
    """The leader may already be gone while Java/dbt descendants remain alive."""
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    deadline = time.monotonic() + grace
    while _group_exists(process.pid) and time.monotonic() < deadline:
        process.poll()  # reap the leader without confusing it with group completion
        time.sleep(0.025)
    # Always signal the group after grace, even if wait()/poll() reaped its leader.
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=10)


def private_worker(
    payload: bytes,
    output: BinaryIO,
    *,
    timeout: int,
    cwd: str,
    environment: Mapping[str, str] | None = None,
) -> int:
    with subprocess.Popen(
        [sys.executable, "-m", "observatorio_secop.orchestration.worker"],
        stdin=subprocess.PIPE,
        stdout=output,
        stderr=output,
        start_new_session=True,
        cwd=cwd,
        env=environment,
    ) as process:
        terminated = False

        def terminate() -> None:
            nonlocal terminated
            if not terminated:
                terminated = True
                _terminate_group(process, TERMINATION_GRACE_SECONDS)

        def cancelled(_signum, _frame):
            raise PipelineError("PIPELINE_TRANSIENT")

        previous = signal.signal(signal.SIGTERM, cancelled)
        try:
            process.communicate(payload, timeout=timeout)
            return process.returncode
        except subprocess.TimeoutExpired:
            raise PipelineError("PIPELINE_TRANSIENT") from None
        finally:
            # Ignore further cancellation until the bounded group cleanup finishes.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            try:
                terminate()
            finally:
                signal.signal(signal.SIGTERM, previous)
