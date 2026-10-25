"""Adapters to the supported producers; no metric or selection logic lives here."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.reconcile import PopulationReconciliation
from observatorio_secop.orchestration.models import PipelineError, Runtime, contained
from observatorio_secop.population import PopulationScope
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.inventory import sha256_path


def warehouse_config(runtime: Runtime):
    return replace(
        load_warehouse_config(runtime.project / "config/warehouse.yaml"), silver_root=runtime.silver
    )


def baseline(runtime: Runtime, parameters: dict[str, Any]) -> Path | None:
    version = parameters["baseline_version"]
    if version is None:
        return None
    path = contained(runtime.silver, "versions", version, "manifest.json")
    verify_manifest_bundle(path, expected_hash=parameters["baseline_sha256"])
    return path


def acquisition(runtime: Runtime, document: dict[str, Any]) -> dict[str, Any]:
    p = document["parameters"]
    config = replace(
        load_ingestion_config(runtime.project / "config/bronze_ingestion.yaml"),
        bronze_root=runtime.bronze,
        contract_path=runtime.project / "contracts/secop_source.yaml",
        page_size=p["page_size"],
        max_rows=p["total_rows"],
    )
    contract = load_bronze_contract(config.contract_path, config.projection)
    transport = None
    if p["mode"] == "demo":
        from .fixtures import fixture_transport

        transport = fixture_transport(config, contract, p["fixture_id"])
    runner = PopulationReconciliation(config, contract, transport=transport)
    grant = document["grant"]
    manifest = runner.reconcile(
        p["signing_from"],
        p["signing_to"],
        total_rows=p["total_rows"],
        total_requests=p["total_requests"],
        grant_rows=grant["rows"],
        grant_requests=grant["requests"],
        baseline_path=baseline(runtime, p),
        campaign_id=document["campaign_id"],
        invocation_key=grant["id"],
    )
    if not manifest.range_complete or manifest.truncated or manifest.status != "committed":
        raise PipelineError("PIPELINE_COVERAGE_INCOMPLETE")
    report = runner.inspect_campaign(manifest.run_id)  # verified, no source calls
    path = contained(runtime.bronze, "runs", manifest.run_id, "manifest.json")
    previous = document["stages"].get("acquire", {}).get("manifest_sha256")
    if previous is not None and previous != sha256_path(path):
        raise PipelineError("PIPELINE_ARTIFACT_CHANGED")
    return {
        "run_id": manifest.run_id,
        "manifest_sha256": sha256_path(path),
        "rows": manifest.counts["written"],
        "requests": report["acquisition"]["consumed"]["attempts"],
    }


def bundle_for(runtime: Runtime, document: dict[str, Any]):
    result = document["stages"]["silver"]
    return verify_manifest_bundle(
        contained(runtime.silver, "versions", result["silver_version_id"], "manifest.json"),
        expected_hash=result["manifest_sha256"],
    )


def silver(runtime: Runtime, document: dict[str, Any]) -> dict[str, Any]:
    from observatorio_secop.processing.silver.config import load_silver_config
    from observatorio_secop.processing.silver.runner import SilverPipeline

    prior = document["stages"].get("silver", {})
    if prior.get("silver_version_id"):
        bundle = bundle_for(runtime, document)
        return {
            "silver_version_id": bundle.version_id,
            "manifest_sha256": bundle.manifest_sha256,
            "rows": bundle.row_count,
        }
    acquired = document["stages"]["acquire"]
    path = contained(runtime.bronze, "runs", acquired["run_id"], "manifest.json")
    if sha256_path(path) != acquired["manifest_sha256"]:
        raise PipelineError("PIPELINE_ARTIFACT_CHANGED")
    config_path = runtime.project / "config/silver_quality.yaml"
    config = replace(
        load_silver_config(config_path),
        bronze_root=runtime.bronze,
        silver_root=runtime.silver,
        contract_path=runtime.project / "contracts/secop_source.yaml",
        territory_catalog_path=runtime.project / "config/antioquia_municipalities.yaml",
        local_threads=1,
    )
    p = document["parameters"]
    manifest = SilverPipeline(config, config_path=config_path).run_population(
        PopulationScope(p["signing_from"], p["signing_to"]),
        (document["campaign_id"],),
        baseline_version_id=p["baseline_version"],
        baseline_manifest_sha256=p["baseline_sha256"],
        mode="authoritative_candidate",
    )
    if not manifest.critical_passed:
        raise PipelineError("PIPELINE_QUALITY_FAILED")
    bundle = verify_manifest_bundle(
        contained(runtime.silver, "versions", manifest.silver_version_id, "manifest.json")
    )
    if bundle.coverage.get("state") != "complete":
        raise PipelineError("PIPELINE_COVERAGE_INCOMPLETE")
    return {
        "silver_version_id": bundle.version_id,
        "manifest_sha256": bundle.manifest_sha256,
        "rows": bundle.row_count,
    }


def warehouse(runtime: Runtime, document: dict[str, Any]) -> dict[str, Any]:
    from observatorio_secop.warehouse.loader import WarehouseLoader

    bundle = bundle_for(runtime, document)
    WarehouseLoader(warehouse_config(runtime)).load(bundle)
    return {
        "silver_version_id": bundle.version_id,
        "manifest_sha256": bundle.manifest_sha256,
        "rows": bundle.row_count,
    }
