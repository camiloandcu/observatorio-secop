from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest
import yaml

SPEC = importlib.util.spec_from_file_location(
    "gold_dbt", Path(__file__).resolve().parents[1] / "scripts/gold_dbt.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
validate_contracts = MODULE.validate_contracts


def test_catalog_tamper_and_policy_divergence_block_build(tmp_path: Path) -> None:
    for directory in ("dbt", "contracts", "config"):
        (tmp_path / directory).mkdir()
    catalog = tmp_path / "contracts/gold_metrics.yaml"
    policy = tmp_path / "config/gold_policy.yaml"
    catalog.write_text("version: v1\n")
    policy.write_text("gold_min_count_coverage: 1.0\n")
    variables = {
        "gold_catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest(),
        "gold_policy_sha256": hashlib.sha256(policy.read_bytes()).hexdigest(),
        "gold_min_count_coverage": 1.0,
    }
    project = tmp_path / "dbt/dbt_project.yml"
    project.write_text(yaml.safe_dump({"vars": variables}))
    validate_contracts(tmp_path)
    catalog.write_text("version: changed\n")
    with pytest.raises(ValueError, match="digest differs"):
        validate_contracts(tmp_path)
    variables["gold_catalog_sha256"] = hashlib.sha256(catalog.read_bytes()).hexdigest()
    variables["gold_min_count_coverage"] = 0.5
    project.write_text(yaml.safe_dump({"vars": variables}))
    with pytest.raises(ValueError, match="policy differs"):
        validate_contracts(tmp_path)


@pytest.mark.parametrize(
    "arguments",
    [
        ["build", "--vars", "{gold_min_count_coverage: 0}"],
        ["build", "--vars={gold_policy_sha256: forged}"],
    ],
)
def test_policy_override_is_rejected_before_dbt(monkeypatch, arguments):
    monkeypatch.setattr(MODULE, "validate_contracts", lambda: None)

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid policy override must never launch dbt")

    monkeypatch.setattr(MODULE.subprocess, "run", forbidden)
    assert MODULE.main(arguments) == 2
