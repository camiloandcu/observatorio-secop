"""Run dbt only with the reviewed Gold catalog and policy byte identities."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def validate_contracts(root: Path = ROOT) -> None:
    project = yaml.safe_load((root / "dbt/dbt_project.yml").read_text())
    variables = project["vars"]
    for name, relative in (
        ("gold_catalog_sha256", "contracts/gold_metrics.yaml"),
        ("gold_policy_sha256", "config/gold_policy.yaml"),
    ):
        actual = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        if variables.get(name) != actual:
            raise ValueError(f"Reviewed Gold digest differs: {relative}")
    policy = yaml.safe_load((root / "config/gold_policy.yaml").read_text())
    if policy.get("policy_version", policy.get("gold_policy_version")) != policy.get(
        "gold_policy_version"
    ):
        raise ValueError("Reviewed Gold policy version is inconsistent")
    hhi = policy.get("hhi", {})
    for field, variable in (
        ("minimum_count_identity_coverage", "gold_min_count_coverage"),
        ("minimum_value_identity_coverage", "gold_min_value_coverage"),
    ):
        if field in hhi and hhi[field] != policy.get(variable):
            raise ValueError("Reviewed Gold coverage policy is inconsistent")
    # The SQL defaults and version must correspond to the reviewed policy file.
    for key in ("gold_policy_version", "gold_min_count_coverage", "gold_min_value_coverage"):
        if key in policy and variables.get(key) != policy[key]:
            raise ValueError(f"Reviewed Gold policy differs: {key}")


def main(arguments: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if arguments is None else arguments
    try:
        validate_contracts()
        # --vars may adjust supported query inputs, but cannot rewrite reviewed
        # catalog/policy defaults through the supported local build command.
        for index, argument in enumerate(arguments):
            raw = None
            if argument == "--vars" and index + 1 < len(arguments):
                raw = arguments[index + 1]
            elif argument.startswith("--vars="):
                raw = argument.split("=", 1)[1]
            if raw is not None:
                values = yaml.safe_load(raw)
                if not isinstance(values, dict):
                    raise ValueError("dbt vars must be a mapping")
                if any(str(key).startswith(("gold_", "publication_")) for key in values):
                    raise ValueError("Reviewed Gold/publication variables cannot be overridden")
        executable = shutil.which("dbt")
        if executable is None:
            raise ValueError("dbt executable is unavailable")
    except (OSError, ValueError, KeyError, yaml.YAMLError) as error:
        print(json.dumps({"code": "GOLD_CONTRACT_INVALID", "message": str(error)}), file=sys.stderr)
        return 2
    return subprocess.run([executable, *arguments], cwd=ROOT, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
