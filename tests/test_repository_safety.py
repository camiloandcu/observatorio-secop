from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECKER = ROOT / "scripts" / "check_tracked_files.py"


def run(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, check=check, capture_output=True, text=True)


def init_repository(path: Path) -> None:
    run("git", "init", "-q", cwd=path)
    run("git", "config", "user.email", "test@example.invalid", cwd=path)
    run("git", "config", "user.name", "Test User", cwd=path)


def test_current_repository_has_no_forbidden_tracked_paths() -> None:
    result = run(sys.executable, str(CHECKER), str(ROOT), cwd=ROOT)
    assert "No forbidden paths" in result.stdout


@pytest.mark.parametrize(
    "relative_path",
    ["data/sample.csv", "models/model.bin", "mlruns/run.json", ".env", "report.pbix"],
)
def test_generated_and_secret_paths_are_ignored(relative_path: str) -> None:
    result = run("git", "check-ignore", "--quiet", relative_path, cwd=ROOT, check=False)
    assert result.returncode == 0, relative_path


def test_environment_example_is_allowed() -> None:
    result = run("git", "check-ignore", "--quiet", ".env.example", cwd=ROOT, check=False)
    assert result.returncode == 1


@pytest.mark.parametrize(
    "relative_path",
    [
        "data/sample.csv",
        "models/model.bin",
        "mlruns/run.json",
        ".project/plan.md",
        ".codex/config.toml",
        ".ai/context.md",
        "openspec/specs/private.md",
        ".env",
        ".env.local",
        "nested/.env.production",
        ".dbt/profiles.yml",
        "dbt/profiles.yml",
        "report.PBIX",
    ],
)
def test_checker_rejects_forced_tracked_private_paths(tmp_path: Path, relative_path: str) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    init_repository(repository)

    generated_file = repository / relative_path
    generated_file.parent.mkdir(parents=True, exist_ok=True)
    (repository / ".gitignore").write_text("*\n", encoding="utf-8")
    generated_file.write_text("generated\n", encoding="utf-8")
    run("git", "add", "-f", str(generated_file.relative_to(repository)), cwd=repository)

    result = run(sys.executable, str(CHECKER), str(repository), cwd=ROOT, check=False)

    assert result.returncode == 1
    assert relative_path in result.stderr
    assert generated_file.read_text(encoding="utf-8") == "generated\n"


def test_checker_script_is_portable() -> None:
    assert shutil.which("git")
    assert os.access(CHECKER, os.R_OK)


def test_checker_allows_public_examples_and_fixtures(tmp_path: Path) -> None:
    init_repository(tmp_path)
    paths = [
        ".env.example",
        "nested/.env.example",
        "tests/fixtures/data/sample.csv",
        "tests/fixtures/models/sample.json",
        "config/silver_quality.yaml",
        "dbt/models/facts/fact_contract.sql",
    ]
    for relative_path in paths:
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("public fixture\n", encoding="utf-8")
        run("git", "add", "-f", relative_path, cwd=tmp_path)
    result = run(sys.executable, str(CHECKER), str(tmp_path), cwd=ROOT)
    assert "No forbidden paths" in result.stdout
