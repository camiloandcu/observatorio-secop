"""CLI behavior without network access."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from observatorio_secop.ingestion.cli import main

ROOT = Path(__file__).resolve().parents[2]


def local_config(tmp_path: Path) -> Path:
    document = yaml.safe_load((ROOT / "config/bronze_ingestion.yaml").read_text())
    document["ingestion"]["bronze_root"] = str(tmp_path / "bronze")
    document["ingestion"]["contract_path"] = str(ROOT / "contracts/secop_source.yaml")
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(document))
    return path


def test_inspect_reports_empty_local_state(tmp_path: Path, capsys) -> None:
    config = local_config(tmp_path)
    assert main(["--config", str(config), "inspect"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["checkpoint"] is None
    assert report["committed_runs"] == 0
    assert report["logical_observations"] == 0


def test_clean_staging_only_removes_uncommitted_directories(tmp_path: Path, capsys) -> None:
    config = local_config(tmp_path)
    staging = tmp_path / "bronze/staging/unfinished"
    published = tmp_path / "bronze/runs/complete"
    staging.mkdir(parents=True)
    published.mkdir(parents=True)
    assert main(["--config", str(config), "clean-staging"]) == 0
    assert not staging.exists()
    assert published.exists()
    assert json.loads(capsys.readouterr().out) == {"removed_staging_runs": 1}
