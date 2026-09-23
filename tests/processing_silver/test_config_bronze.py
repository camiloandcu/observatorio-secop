from __future__ import annotations

import json

import pytest
from conftest import ROOT, row, source_contract_hash, write_bronze_run

from observatorio_secop.processing.silver.bronze import (
    read_bronze_dataframe,
    resolve_bronze_runs,
)
from observatorio_secop.processing.silver.config import (
    load_silver_config,
    load_territory_catalog,
    territory_key,
)
from observatorio_secop.processing.silver.errors import BronzeEvidenceError


def test_config_and_catalog_are_explicit() -> None:
    config = load_silver_config(ROOT / "config/silver_quality.yaml")
    catalog = load_territory_catalog(ROOT / "config/antioquia_municipalities.yaml")
    assert config.max_rejection_rate == 0.02
    assert config.allow_empty_output is False
    assert config.parquet_codec == "snappy"
    assert len(catalog.municipalities) == 125
    assert catalog.canonical_by_key["ITAGUI"] == "Itagüí"
    assert territory_key("  medellín ") == "MEDELLIN"
    assert len(catalog.valle_de_aburra) == 10


def test_resolves_only_committed_runs_and_projects_extra_fields(silver_config, spark) -> None:
    write_bronze_run(
        silver_config,
        "run-1",
        [{**row("A"), "unapproved_addition": "ignored"}],
    )
    write_bronze_run(silver_config, "run-2", [row("B")], status="prepared")
    runs = resolve_bronze_runs(
        silver_config.bronze_root,
        dataset_id=silver_config.dataset_id,
        contract_sha256=source_contract_hash(),
    )
    assert [item.run_id for item in runs] == ["run-1"]
    frame = read_bronze_dataframe(spark, runs)
    assert frame.count() == 1
    assert "unapproved_addition" not in frame.columns
    assert frame.first().bronze_row_ordinal == 0


def test_requested_non_committed_run_is_rejected(silver_config) -> None:
    write_bronze_run(silver_config, "run-1", [row("A")], status="prepared")
    with pytest.raises(BronzeEvidenceError, match="not committed"):
        resolve_bronze_runs(
            silver_config.bronze_root,
            dataset_id=silver_config.dataset_id,
            contract_sha256=source_contract_hash(),
            run_ids=("run-1",),
        )


@pytest.mark.parametrize("mutation", ["hash", "count", "missing"])
def test_tampered_bronze_evidence_fails(silver_config, mutation: str) -> None:
    run_path = write_bronze_run(silver_config, "run-1", [row("A")])
    manifest_path = run_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if mutation == "hash":
        (run_path / "pages/primary-00001.json").write_text("[]")
    elif mutation == "count":
        manifest["pages"][0]["row_count"] = 2
        manifest_path.write_text(json.dumps(manifest))
    else:
        (run_path / "pages/primary-00001.json").unlink()
    with pytest.raises(BronzeEvidenceError):
        resolve_bronze_runs(
            silver_config.bronze_root,
            dataset_id=silver_config.dataset_id,
            contract_sha256=source_contract_hash(),
            run_ids=("run-1",),
        )
