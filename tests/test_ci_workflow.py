from __future__ import annotations

import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
MAKEFILE = ROOT / "Makefile"
PYPROJECT = ROOT / "pyproject.toml"
LOCKFILE = ROOT / "uv.lock"
PYTHON_VERSION = ROOT / ".python-version"


def test_ci_workflow_is_valid_yaml_and_has_minimum_controls() -> None:
    document = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    quality = document["jobs"]["quality"]
    serialized_steps = "\n".join(step.get("run", "") for step in quality["steps"])
    actions = [step.get("uses", "") for step in quality["steps"]]

    assert quality["timeout-minutes"] == 10
    assert any(action.startswith("astral-sh/setup-uv@") for action in actions)
    assert "make install" in serialized_steps
    assert "make quality" in serialized_steps


def test_makefile_uses_uv_as_the_only_python_tool_runner() -> None:
    makefile = MAKEFILE.read_text(encoding="utf-8")

    assert "$(UV) sync --locked --python 3.12 --extra dev" in makefile
    assert "$(UV) run --locked --extra dev" in makefile
    for forbidden in ("VENV_", ".venv/bin/", "pip install", "python3.12 -m venv"):
        assert forbidden not in makefile


def test_uv_and_python_versions_are_declared_and_locked() -> None:
    pyproject = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    lockfile = LOCKFILE.read_text(encoding="utf-8")

    assert pyproject["tool"]["uv"]["required-version"] == ">=0.12,<0.13"
    assert PYTHON_VERSION.read_text(encoding="utf-8").strip() == "3.12"
    assert 'requires-python = "==3.12.*"' in lockfile


def test_ci_does_not_depend_on_external_secrets_or_heavy_services() -> None:
    workflow_text = WORKFLOW.read_text(encoding="utf-8").casefold()

    for forbidden in (
        "secrets.",
        "aws_",
        "socrata",
        "docker compose",
        "airflow",
        "mlflow",
        "pip install",
        "actions/setup-python",
    ):
        assert forbidden not in workflow_text
