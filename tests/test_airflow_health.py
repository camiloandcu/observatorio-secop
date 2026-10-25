"""Lightweight checks keep finite connection/query bounds and fail closed."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("airflow_health", ROOT / "scripts/airflow_health.py")
assert SPEC and SPEC.loader
health = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(health)
ENV = {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://probe:CANARY@postgres/airflow"}


class Database:
    def __init__(self, result):
        self.result = result
        self.call = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, query, parameters):
        self.call = (query, parameters)
        return SimpleNamespace(fetchone=lambda: self.result)


@pytest.mark.parametrize("job", health.JOB_TYPES)
@pytest.mark.parametrize("result,expected", [((True,), True), ((False,), False), (None, False)])
def test_job_check_requires_current_matching_running_heartbeat(monkeypatch, job, result, expected):
    database = Database(result)
    options = {}

    def connect(**kwargs):
        options.update(kwargs)
        return database

    monkeypatch.setattr(health.psycopg, "connect", connect)
    assert health.healthy(job, environment=ENV, hostname="this-container") is expected
    query, parameters = database.call
    assert parameters == (job, "this-container", 30)
    rendered = query.as_string()
    assert "state='running'" in rendered
    assert '"public"."job"' in rendered
    assert "latest_heartbeat" in rendered and "clock_timestamp()" in rendered
    assert options["connect_timeout"] == 2
    assert "statement_timeout=2000" in options["options"]
    assert "lock_timeout=1000" in options["options"]


@pytest.mark.parametrize(
    "connection",
    [
        "",
        "sqlite:///state.db",
        "postgresql:///missing-host",
        "postgresql://probe@host/db?options=unbounded",
    ],
)
def test_invalid_metadata_connection_rejected_before_connect(connection, monkeypatch):
    monkeypatch.setattr(health.psycopg, "connect", lambda **_: pytest.fail("Connection reached"))
    with pytest.raises(ValueError):
        health.healthy(
            "SchedulerJob", environment={"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": connection}
        )


@pytest.mark.parametrize(
    "job,age",
    [("UnknownJob", 30), ("SchedulerJob", 0), ("DagProcessorJob", 301), ("SchedulerJob", True)],
)
def test_invalid_job_or_age_rejected(job, age):
    with pytest.raises(ValueError):
        health.healthy(job, max_age=age, environment=ENV)


def test_database_failure_does_not_print_credentials(monkeypatch, capsys):
    monkeypatch.setenv(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", ENV["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    )

    def fail(**_):
        raise psycopg.OperationalError("password=CANARY")

    monkeypatch.setattr(health.psycopg, "connect", fail)
    assert health.main(["--job-type", "SchedulerJob"]) == 1
    captured = capsys.readouterr()
    assert "CANARY" not in captured.out + captured.err
    assert "unavailable or heartbeat stale" in captured.out


def test_compose_healthcheck_executes_one_lightweight_process():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    for service, job in (
        ("airflow-scheduler", "SchedulerJob"),
        ("airflow-dag-processor", "DagProcessorJob"),
    ):
        check = compose["services"][service]["healthcheck"]
        assert check["test"] == [
            "CMD",
            "python",
            "/opt/secop/scripts/airflow_health.py",
            "--job-type",
            job,
        ]
        assert check["timeout"] == "10s"
    source = (ROOT / "scripts/airflow_health.py").read_text()
    assert "import airflow" not in source and "subprocess" not in source
