#!/usr/bin/env python3
"""Fail when Git tracks files that must remain local."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path, PurePosixPath

FORBIDDEN_DIRECTORIES = {
    "data",
    "mlruns",
    "models",
}
FORBIDDEN_ROOT_NAMES = {".env"}


def tracked_paths(repository: Path) -> list[PurePosixPath]:
    """Return paths known to the repository index without reading file contents."""
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repository,
        check=True,
        capture_output=True,
    )
    return [PurePosixPath(item.decode()) for item in result.stdout.split(b"\0") if item]


def is_forbidden(path: PurePosixPath) -> bool:
    """Match secrets and generated artifacts."""
    if not path.parts:
        return False
    if path.parts[0] in FORBIDDEN_DIRECTORIES:
        return True
    if len(path.parts) == 1 and path.name in FORBIDDEN_ROOT_NAMES:
        return True
    return path.name == ".env" or path.suffix.casefold() == ".pbix"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "repository",
        nargs="?",
        type=Path,
        default=Path.cwd(),
        help="Git repository to inspect (defaults to the current directory)",
    )
    args = parser.parse_args()

    try:
        forbidden = sorted(
            str(path) for path in tracked_paths(args.repository) if is_forbidden(path)
        )
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"Could not inspect tracked files: {error}", file=sys.stderr)
        return 2

    if forbidden:
        print("Forbidden tracked paths detected:", file=sys.stderr)
        for path in forbidden:
            print(f"- {path}", file=sys.stderr)
        return 1

    print("No forbidden paths are tracked.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
