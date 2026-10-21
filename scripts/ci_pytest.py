"""Opt-in pytest evidence: identities and phase outcomes, never exception payloads."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

_STATE = {"schema": 1, "collected": [], "deselected": [], "results": {}, "collection_errors": 0}


def pytest_collection_finish(session):
    _STATE["collected"] = [item.nodeid for item in session.items]


def pytest_deselected(items):
    _STATE["deselected"].extend(item.nodeid for item in items)


def pytest_collectreport(report):
    if report.failed:
        _STATE["collection_errors"] += 1


def pytest_runtest_logreport(report):
    phase = {"outcome": report.outcome, "duration": report.duration}
    if hasattr(report, "wasxfail"):
        phase["xfail"] = True
    if report.skipped:
        # Reason taxonomy is deliberately bounded; raw skip strings can contain secrets.
        reason = str(report.longrepr).lower()
        if phase.get("xfail"):
            phase["reason"] = "expected-failure"
        elif "warehouse_test_dsn" in reason:
            phase["reason"] = "disposable-dsn-missing"
        elif "java" in reason:
            phase["reason"] = "java-unavailable"
        elif "dbt" in reason:
            phase["reason"] = "dbt-unavailable"
        else:
            phase["reason"] = "test-or-fixture-skip"
    _STATE["results"].setdefault(report.nodeid, {})[report.when] = phase


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session, exitstatus):
    target = os.environ.get("CI_PYTEST_REPORT")
    if target:
        _STATE["exitstatus"] = int(exitstatus)
        path = Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_STATE, sort_keys=True) + "\n")
