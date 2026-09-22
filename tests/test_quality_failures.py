from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def run(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, check=False, capture_output=True, text=True)


def test_format_check_rejects_unformatted_python(tmp_path: Path) -> None:
    source = tmp_path / "unformatted.py"
    source.write_text("values={1,2,3}\n", encoding="utf-8")

    result = run("ruff", "format", "--check", "--no-cache", str(source), cwd=tmp_path)

    assert result.returncode != 0
    assert "Would reformat" in result.stdout


def test_lint_rejects_undefined_name(tmp_path: Path) -> None:
    source = tmp_path / "invalid.py"
    source.write_text("print(undefined_name)\n", encoding="utf-8")

    result = run("ruff", "check", "--no-cache", str(source), cwd=tmp_path)

    assert result.returncode != 0
    assert "F821" in result.stdout


def test_pytest_propagates_smoke_failure(tmp_path: Path) -> None:
    test_file = tmp_path / "test_controlled_failure.py"
    test_file.write_text("def test_failure():\n    assert False\n", encoding="utf-8")

    result = run(
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-p",
        "no:cacheprovider",
        str(test_file),
        cwd=tmp_path,
    )

    assert result.returncode == 1
    assert "1 failed" in result.stdout
