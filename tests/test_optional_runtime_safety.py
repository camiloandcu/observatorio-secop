"""The compatibility probe must not inherit working-service configuration."""

from __future__ import annotations

import importlib.util
import subprocess
import traceback
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_optional_probe_excludes_working_service_settings() -> None:
    spec = importlib.util.spec_from_file_location(
        "optional_runtimes", ROOT / "scripts/check_optional_runtimes.py"
    )
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    source = {
        "PATH": "/synthetic/bin",
        "AIRFLOW_CONFIG": "/working/airflow.cfg",
        "AIRFLOW_HOME": "/working/airflow",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://working/metadata",
        "AIRFLOW_CONN_HTTP_DEFAULT": "http://working.example",
        "MLFLOW_TRACKING_URI": "https://working.example",
        "MLFLOW_REGISTRY_URI": "https://working.example/registry",
        "MLFLOW_TRACKING_TOKEN": "fixture",
    }
    assert probe.isolated_environment(source) == {"PATH": "/synthetic/bin"}
    assert source["AIRFLOW_CONFIG"] == "/working/airflow.cfg"


@pytest.mark.parametrize("redact", [(), ("private-secret",)])
def test_migration_probe_failure_does_not_print_credentials(monkeypatch, redact) -> None:
    spec = importlib.util.spec_from_file_location(
        "airflow_migration", ROOT / "scripts/check_airflow_migration.py"
    )
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    monkeypatch.setattr(
        probe.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout=b"", stderr=b"postgresql://probe:private-secret@fixture/db"
        ),
    )
    with pytest.raises(RuntimeError) as error:
        probe.docker("exec", "owned-fixture", "airflow", "db", "check", redact=redact)
    assert "private-secret" not in str(error.value)
    assert "exit 1" in str(error.value)


@pytest.mark.parametrize("failure", ["timeout", "oserror"])
def test_migration_process_failure_redacts_command_traceback(monkeypatch, failure) -> None:
    spec = importlib.util.spec_from_file_location(
        "airflow_migration", ROOT / "scripts/check_airflow_migration.py"
    )
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)

    fixture_secret = "private-" + "secret"

    def fail(*args, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(["docker", fixture_secret], 1)
        raise OSError(fixture_secret)

    monkeypatch.setattr(probe.subprocess, "run", fail)
    with pytest.raises(RuntimeError) as error:
        probe.docker("exec", fixture_secret)
    assert fixture_secret not in "".join(traceback.format_exception(error.value))


def test_cleanup_fails_when_owned_container_survives(monkeypatch) -> None:
    spec = importlib.util.spec_from_file_location(
        "airflow_migration", ROOT / "scripts/check_airflow_migration.py"
    )
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    calls = []

    def stubborn(*args, **kwargs):
        calls.append(args)
        if args[:2] == ("ps", "-a"):
            return b"owned-fixture\nunrelated-working-service\n"
        return b""

    monkeypatch.setattr(probe, "docker", stubborn)
    with pytest.raises(RuntimeError, match="cleanup"):
        probe.cleanup(["owned-fixture"], "owned-net")
    assert ("rm", "-f", "-v", "owned-fixture") in calls
    assert all("unrelated-working-service" not in call for call in calls)
