"""The compatibility probe must not inherit working-service configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
