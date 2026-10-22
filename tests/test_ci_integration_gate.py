from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("ci_gate", ROOT / "scripts/ci_gate.py")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)
INVENTORY = {
    "required_nodeids": ["tests/warehouse/test_case.py::test_case"],
    "critical_families": ["tests/warehouse/"],
}
NODE = INVENTORY["required_nodeids"][0]


def report():
    return {
        "schema": 1,
        "collected": [NODE],
        "deselected": [],
        "collection_errors": 0,
        "exitstatus": 0,
        "results": {
            NODE: {
                phase: {"outcome": "passed", "duration": 0.1}
                for phase in ("setup", "call", "teardown")
            }
        },
    }


def test_complete_phases_pass():
    assert GATE.reconcile(report(), INVENTORY)["passed"]


@pytest.mark.parametrize("phase", ["setup", "call", "teardown"])
@pytest.mark.parametrize("outcome", ["failed", "skipped", "xfail", "xpass", "missing"])
def test_critical_omission_and_phase_failures_block(phase, outcome):
    payload = report()
    if outcome == "missing":
        del payload["results"][NODE][phase]
    elif outcome in {"xfail", "xpass"}:
        payload["results"][NODE][phase]["xfail"] = True
    else:
        payload["results"][NODE][phase]["outcome"] = outcome
    assert not GATE.reconcile(payload, INVENTORY)["passed"]


@pytest.mark.parametrize("mutation", ["missing", "deselected", "new-skipped", "collection-error"])
def test_collection_reconciliation(mutation):
    payload = report()
    if mutation == "missing":
        payload["collected"] = ["tests/warehouse/other.py::test_other"]
        payload["results"] = {}
    elif mutation == "deselected":
        payload["deselected"] = [NODE]
    elif mutation == "collection-error":
        payload["collection_errors"] = 1
    else:
        new_node = "tests/warehouse/new.py::test_new"
        payload["collected"].append(new_node)
        payload["results"][new_node] = {"setup": {"outcome": "skipped"}}
    assert not GATE.reconcile(payload, INVENTORY)["passed"]


@pytest.mark.parametrize("value", [{}, {"schema": 1}, None, [], {"collected": []}])
def test_malformed_blocks(value):
    with pytest.raises(GATE.GateError):
        GATE.reconcile(value, INVENTORY)


def test_missing_and_malformed_files_block(tmp_path):
    target = tmp_path / "result.json"
    with pytest.raises(GATE.GateError):
        GATE.load_report(target)
    target.write_text("{")
    with pytest.raises(GATE.GateError):
        GATE.load_report(target)


def test_seed_inventory_is_nonempty_and_current():
    inventory = json.loads((ROOT / "config/ci_integration.json").read_text())
    assert len(inventory["required_nodeids"]) > 100
    assert len(set(inventory["required_nodeids"])) == len(inventory["required_nodeids"])
    assert all(
        any(n.startswith(f) for f in inventory["critical_families"])
        for n in inventory["required_nodeids"]
    )


def test_disposable_identity_requires_explicit_matching_target():
    spec = importlib.util.spec_from_file_location(
        "ci_integration", ROOT / "scripts/ci_integration.py"
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    environment = {
        "WAREHOUSE_TEST_DSN": (
            "host=/tmp/secop-test port=5432 dbname=secop_ci_test password=secop_ci_test_only"
        ),
        "CI_DISPOSABLE_DB_HOST": "/tmp/secop-test",
        "CI_DISPOSABLE_DB_PORT": "5432",
        "CI_DISPOSABLE_DB_NAME": "secop_ci_test",
    }
    assert runner.disposable_target(environment)[1]["dbname"] == "secop_ci_test"
    for key in environment:
        wrong = copy.copy(environment)
        wrong[key] = "working_database"
        with pytest.raises(runner.PreflightError):
            runner.disposable_target(wrong)
    with pytest.raises(runner.PreflightError):
        runner.disposable_target({})


def test_plugin_preserves_parametrized_nodeids_and_teardown_without_payloads(tmp_path):
    import os
    import subprocess
    import sys

    (tmp_path / "test_example.py").write_text(
        "import pytest\n"
        '@pytest.fixture\ndef resource():\n    yield\n    raise ValueError("PRIVATE_SENTINEL")\n'
        '@pytest.mark.parametrize("value", [1, 2])\n'
        "def test_example(resource, value):\n    assert value > 0\n"
    )
    output = tmp_path / "outcomes.json"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT)
    environment["CI_PYTEST_REPORT"] = str(output)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "scripts.ci_pytest", "-q", str(tmp_path)],
        env=environment,
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    raw = output.read_text()
    assert "PRIVATE_SENTINEL" not in raw
    evidence = json.loads(raw)
    assert len(evidence["collected"]) == 2
    assert evidence["collected"][0].endswith("test_example[1]")
    for phases in evidence["results"].values():
        assert phases["call"]["outcome"] == "passed"
        assert phases["teardown"]["outcome"] == "failed"


@pytest.mark.parametrize(
    "phases",
    [
        {},
        {"setup": {"outcome": "passed"}},
        {"setup": {"outcome": "passed"}, "call": {"outcome": "passed"}},
    ],
)
def test_noncritical_truncated_evidence_blocks(phases):
    payload = report()
    node = "tests/test_other.py::test_other"
    payload["collected"].append(node)
    payload["results"][node] = phases
    assert not GATE.reconcile(payload, INVENTORY)["passed"]


def test_complete_noncritical_setup_skip_remains_allowed():
    payload = report()
    node = "tests/test_other.py::test_other"
    payload["collected"].append(node)
    payload["results"][node] = {"setup": {"outcome": "skipped"}, "teardown": {"outcome": "passed"}}
    assert GATE.reconcile(payload, INVENTORY)["passed"]
    assert GATE.reconcile(payload, INVENTORY)["noncritical_omissions"]


@pytest.mark.parametrize(
    "key,value",
    [("exitstatus", False), ("collection_errors", False), ("exitstatus", -1), ("exitstatus", 99)],
)
def test_malformed_execution_status_is_rejected(key, value):
    payload = report()
    payload[key] = value
    with pytest.raises(GATE.GateError):
        GATE.reconcile(payload, INVENTORY)


def test_mapped_database_port_uses_client_endpoint(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    import psycopg

    spec = importlib.util.spec_from_file_location(
        "ci_integration", ROOT / "scripts/ci_integration.py"
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    for key, value in {
        "WAREHOUSE_TEST_DSN": (
            "host=127.0.0.1 port=55432 dbname=secop_ci_test password=secop_ci_test_only"
        ),
        "CI_DISPOSABLE_DB_HOST": "127.0.0.1",
        "CI_DISPOSABLE_DB_PORT": "55432",
        "CI_DISPOSABLE_DB_NAME": "secop_ci_test",
    }.items():
        monkeypatch.setenv(key, value)
    connection = MagicMock()
    connection.info = SimpleNamespace(port=55432, server_version=160004)
    connection.execute.return_value.fetchone.return_value = ("secop_ci_test", "5432", "PostgreSQL")
    connection.__enter__.return_value = connection
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: connection)
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stderr='openjdk version "17.0.18"', stdout=""),
    )
    monkeypatch.setattr(runner.shutil, "which", lambda _: "/tools/dbt")
    assert runner.preflight()["postgresql"] == 160004
    connection.info.port = 5432
    with pytest.raises(runner.PreflightError):
        runner.preflight()


@pytest.mark.parametrize("password", ["", " password=''", " password=", " password=' '"])
def test_disposable_target_requires_explicit_nonempty_password(password):
    spec = importlib.util.spec_from_file_location(
        "ci_integration", ROOT / "scripts/ci_integration.py"
    )
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    environment = {
        "WAREHOUSE_TEST_DSN": "host=127.0.0.1 port=55432 dbname=secop_ci_test" + password,
        "CI_DISPOSABLE_DB_HOST": "127.0.0.1",
        "CI_DISPOSABLE_DB_PORT": "55432",
        "CI_DISPOSABLE_DB_NAME": "secop_ci_test",
        # Implicit libpq credentials cannot satisfy the explicit test contract.
        "PGPASSWORD": "implicit_password",
    }
    with pytest.raises(runner.PreflightError, match="DSN/password"):
        runner.disposable_target(environment)
