from __future__ import annotations

import re
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/ci.yml"
MAKEFILE = ROOT / "Makefile"
PYPROJECT = ROOT / "pyproject.toml"
LOCKFILE = ROOT / "uv.lock"
PYTHON_VERSION = ROOT / ".python-version"


def workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_ci_has_isolated_real_integration_and_bounded_controls() -> None:
    document = workflow()
    quality = document["jobs"]["quality"]
    commands = [step.get("run", "") for step in quality["steps"]]
    assert 1 <= quality["timeout-minutes"] <= 60
    assert quality["runs-on"] == "ubuntu-24.04"
    assert "make install" in commands
    assert "make ci" in commands
    assert "scripts/ci_integration.py run" in MAKEFILE.read_text()
    service = quality["services"]["postgres"]
    assert "@sha256:" in service["image"]
    assert service["env"]["POSTGRES_DB"].startswith("secop_ci_")
    assert service["env"]["POSTGRES_PASSWORD"]
    assert "POSTGRES_HOST_AUTH_METHOD" not in service["env"]
    assert service["ports"] == ["127.0.0.1:5432:5432"]
    assert "pg_isready" in service["options"]
    environment = quality["env"]
    assert environment["CI_DISPOSABLE_DB_NAME"] == service["env"]["POSTGRES_DB"]
    assert f"dbname={environment['CI_DISPOSABLE_DB_NAME']}" in environment["WAREHOUSE_TEST_DSN"]
    assert environment["CI_DISPOSABLE_DB_HOST"] == "127.0.0.1"
    assert str(environment["CI_DISPOSABLE_DB_PORT"]) == "5432"
    java = next(s for s in quality["steps"] if s.get("uses", "").startswith("actions/setup-java@"))
    assert str(java["with"]["java-version"]) == "17"


def test_ci_runs_both_fail_closed_security_controls() -> None:
    security = workflow()["jobs"]["security"]
    assert 1 <= security["timeout-minutes"] <= 30
    assert set(security["strategy"]["matrix"]["control"]) == {"secrets", "dependencies"}
    assert security["strategy"]["fail-fast"] is False
    commands = "\n".join(s.get("run", "") for s in security["steps"])
    assert "scripts/ci_security.py ${{ matrix.control }}" in commands
    checkout = next(
        s for s in security["steps"] if s.get("uses", "").startswith("actions/checkout@")
    )
    assert checkout["with"]["fetch-depth"] == 0


def test_ci_limits_untrusted_pr_permissions_caches_and_artifacts() -> None:
    document = workflow()
    events = document.get("on", document.get(True))
    assert set(events) == {"pull_request", "push"}
    assert document["permissions"] == {"contents": "read"}
    approved_reports = {
        "reports/ci/summary.json",
        "reports/ci/outcomes.json",
        "reports/ci/resources.json",
        "reports/security/${{ matrix.control }}.json",
        "reports/optional/runtime-smoke.json",
        "reports/optional/airflow-migration.json",
        "reports/optional/compose-profile.json",
    }
    for job in document["jobs"].values():
        assert "permissions" not in job
        for step in job["steps"]:
            assert not step.get("continue-on-error", False)
            action = step.get("uses", "")
            if action:
                assert re.fullmatch(r"[\w/-]+@[a-f0-9]{40}", action)
            if action.startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] is False
            if action.startswith("astral-sh/setup-uv@"):
                assert step["with"]["enable-cache"] is False
                assert step["with"]["version"] == "0.12.0"
            assert not action.startswith("actions/cache@")
            if action.startswith("actions/upload-artifact@"):
                assert 1 <= step["with"]["retention-days"] <= 7
                assert set(step["with"]["path"].splitlines()) <= approved_reports
    assert "secrets." not in WORKFLOW.read_text().casefold()


def test_makefile_uses_uv_as_the_only_python_tool_runner() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")
    assert "$(UV) sync --locked --python 3.12 --extra dev" in makefile
    assert "$(UV) run --locked --extra dev" in makefile
    assert "mypy src/observatorio_secop/warehouse" in makefile
    for forbidden in ("VENV_", ".venv/bin/", "pip install", "python3.12 -m venv"):
        assert forbidden not in makefile


def test_uv_and_python_versions_are_declared_and_locked() -> None:
    pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    lockfile = LOCKFILE.read_text(encoding="utf-8")
    assert pyproject["tool"]["uv"]["required-version"] == ">=0.12,<0.13"
    assert PYTHON_VERSION.read_text(encoding="utf-8").strip() == "3.12"
    assert 'requires-python = "==3.12.*"' in lockfile


def test_optional_runtime_job_exercises_migrations_and_real_profile() -> None:
    job = workflow()["jobs"]["optional-runtimes"]
    assert 1 <= job["timeout-minutes"] <= 30
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "uv sync --locked --all-extras" in commands
    for script in (
        "check_optional_runtimes.py",
        "check_airflow_migration.py",
        "check_local_airflow_profile.py",
    ):
        assert script in commands
