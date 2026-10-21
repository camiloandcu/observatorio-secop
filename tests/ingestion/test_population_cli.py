"""Operator commands exercise durable campaign progress without live services."""

from __future__ import annotations

import json

from ingestion.conftest import metadata
from ingestion.test_ingestion_cli import local_config
from ingestion.test_population_reconciliation import runner, signed
from observatorio_secop.ingestion.cli import main


def test_reconcile_inspect_and_resume_use_the_real_campaign(config, contract, monkeypatch, capsys):
    instance = runner(config, contract, [metadata(contract), [signed("A"), signed("B")], []])
    monkeypatch.setattr(
        "observatorio_secop.ingestion.reconcile.PopulationReconciliation", lambda *_: instance
    )
    config_path = local_config(config.bronze_root.parent)
    arguments = [
        "--config",
        str(config_path),
        "reconcile",
        "--signing-from",
        "2020-01-01",
        "--signing-to",
        "2021-01-01",
        "--total-rows",
        "20",
        "--total-requests",
        "20",
        "--grant-rows",
        "2",
        "--grant-requests",
        "2",
        "--page-size",
        "2",
    ]
    assert main(arguments) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "paused_budget" and report["campaign_id"] == "campaign-1"
    assert main(["--config", str(config_path), "inspect-campaign", "campaign-1"]) == 0
    inspected = json.loads(capsys.readouterr().out)
    assert inspected["rows"] == 2 and len(inspected["reservations"]) == 2
    assert main([*arguments, "--resume", "campaign-1"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "committed" and report["range_complete"]
    assert report["acquisition"]["consumed"] == {"rows": 2, "attempts": 3}


def test_inspection_rejects_path_escape_without_source_access(tmp_path, capsys):
    config_path = local_config(tmp_path)
    assert main(["--config", str(config_path), "inspect-campaign", "../other"]) == 2
    assert "single safe" in capsys.readouterr().err
