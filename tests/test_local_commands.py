from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL_SCRIPT = ROOT / "scripts" / "local.sh"


def run_local(
    command: str, *, env_file: Path | None = None, extra_env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    if env_file is not None:
        environment["ENV_FILE"] = str(env_file)
    if extra_env:
        environment.update(extra_env)
    return subprocess.run(
        ["bash", str(LOCAL_SCRIPT), command],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )


def test_environment_example_is_complete() -> None:
    result = run_local("validate-env", env_file=ROOT / ".env.example")
    assert result.returncode == 0, result.stderr


def test_missing_environment_variable_is_named(tmp_path: Path) -> None:
    environment = tmp_path / ".env"
    lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    environment.write_text(
        "\n".join(line for line in lines if not line.startswith("POSTGRES_PASSWORD=")) + "\n",
        encoding="utf-8",
    )

    result = run_local("validate-env", env_file=environment)

    assert result.returncode == 1
    assert "POSTGRES_PASSWORD" in result.stderr
    assert "local_only_change_me" not in result.stderr


def test_windows_mount_path_is_rejected_without_mutation(tmp_path: Path) -> None:
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("unchanged\n", encoding="utf-8")

    result = run_local(
        "check-path",
        extra_env={"PROJECT_DIR_OVERRIDE": "/mnt/c/projects/observatorio-secop"},
    )

    assert result.returncode == 1
    assert "WSL2 Linux filesystem" in result.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"


def test_clean_requires_explicit_confirmation() -> None:
    result = run_local("clean", env_file=ROOT / ".env.example")
    assert result.returncode == 1
    assert "Destructive cleanup refused" in result.stderr


def test_unavailable_docker_daemon_is_actionable() -> None:
    result = run_local(
        "preflight",
        env_file=ROOT / ".env.example",
        extra_env={"DOCKER_HOST": "unix:///tmp/observatorio-secop-missing-docker.sock"},
    )
    assert result.returncode == 1
    assert any(
        message in result.stderr
        for message in ("Docker CLI is unavailable", "Docker daemon is unavailable")
    )
