"""Scoped Silver assembly with independently verifiable acquisition evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from observatorio_secop.identity import IDENTITY_RULE_VERSION, identity_coverage, key_set_digest
from observatorio_secop.population import (
    PopulationScope,
    digest_json,
    links_arrow_schema,
    summary_arrow_schema,
    validate_condition_set,
    validate_coverage,
    validate_reconciliation,
)
from observatorio_secop.processing.silver.bronze import read_bronze_dataframe, resolve_bronze_runs
from observatorio_secop.processing.silver.conditions import observation_conditions
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.models import (
    QualityResult,
    SilverManifest,
    VerifiedBronzeRun,
)
from observatorio_secop.processing.silver.quality import validate_quality
from observatorio_secop.processing.silver.selection import adoption_record, binding
from observatorio_secop.processing.silver.storage import canonical_json, durable_write
from observatorio_secop.processing.silver.transform import transform_bronze
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from observatorio_secop.warehouse.inventory import sha256_path

COVERAGE_PATH = "evidence/population_coverage.json"
RECONCILIATION_PATH = "evidence/population_reconciliation.json"
SUMMARY_PATH = "evidence/observation_conditions/summary.parquet"
LINKS_PATH = "evidence/observation_conditions/links.parquet"


def run_population(
    pipeline: Any,
    scope: PopulationScope,
    campaign_run_ids: tuple[str, ...],
    *,
    baseline_version_id: str | None = None,
    baseline_manifest_sha256: str | None = None,
    mode: str = "preview",
) -> SilverManifest:
    """Assemble full baseline plus explicit complete or terminal-partial campaigns."""
    if mode not in ("preview", "authoritative_candidate") or not campaign_run_ids:
        raise PublicationError("Population build requires explicit mode and campaign runs")
    if len(campaign_run_ids) != len(set(campaign_run_ids)):
        raise PublicationError("Population campaign identifiers must be unique")
    baseline, dependencies, nodes = _baseline(
        pipeline, baseline_version_id, baseline_manifest_sha256, scope
    )
    identifiers = tuple(sorted(set(campaign_run_ids) | {item["run_id"] for item in dependencies}))
    runs = resolve_bronze_runs(
        pipeline.config.bronze_root,
        dataset_id=pipeline.config.dataset_id,
        contract_sha256=pipeline.contract.contract_hash,
        run_ids=identifiers,
        registry=pipeline.registry,
    )
    by_id = {run.run_id: run for run in runs}
    if any(
        by_id[item["run_id"]].manifest_sha256 != item["manifest_sha256"] for item in dependencies
    ):
        raise PublicationError("Population baseline Bronze closure has changed")
    _verify_refresh_nodes(runs, nodes, pipeline.config.silver_root)
    campaigns = [by_id[identifier] for identifier in campaign_run_ids]
    baseline_binding = (
        None
        if baseline is None
        else {
            "silver_version_id": baseline.version_id,
            "manifest_sha256": baseline.manifest_sha256,
            "keys_sha256": key_set_digest(baseline.table["contract_key"].to_pylist()),
        }
    )
    acquisitions = []
    for run in campaigns:
        if run.purpose != "population_reconciliation" or not run.population:
            raise PublicationError("Population build requires governed reconciliation campaigns")
        acquisition = run.population.get("acquisition")
        if (
            not isinstance(acquisition, dict)
            or acquisition.get("scope_id") != scope.scope_id
            or acquisition.get("baseline") != baseline_binding
        ):
            raise PublicationError("Campaign scope/baseline does not match Silver assembly")
        if run.population.get("scope") != scope.as_dict():
            raise PublicationError("Campaign semantic scope descriptor differs")
        acquisitions.append(acquisition)
    inputs = [{"run_id": run.run_id, "manifest_sha256": run.manifest_sha256} for run in runs]
    expected_parent = (
        None
        if baseline_binding is None
        else {key: baseline_binding[key] for key in ("silver_version_id", "manifest_sha256")}
    )
    version_id = digest_json(
        {
            "writer_format_version": "silver-population-v3",
            "row_schema_version": 2,
            "mode": mode,
            "scope": scope.as_dict(),
            "scope_id": scope.scope_id,
            "campaigns": acquisitions,
            "bronze_runs": inputs,
            "baseline": baseline_binding,
            "config_sha256": pipeline.config_sha256,
            "contract_sha256": pipeline.contract.contract_hash,
            "territory_catalog_sha256": pipeline.catalog.digest,
            "compatibility_sha256": pipeline.registry.digest,
            "transformation_version": pipeline.config.transformation_version,
            "key_rule_version": pipeline.config.key_rule_version,
        }
    )
    existing = pipeline.storage.existing_manifest(version_id)
    if existing is not None:
        pipeline.storage.verify_existing(version_id, existing)
        verify_manifest_bundle(pipeline.storage.version_path(version_id) / "manifest.json")
        if mode == "authoritative_candidate":
            pipeline.storage.rollback(version_id)
        return SilverManifest.from_dict(existing)
    started = pipeline._now()
    spark, owns_spark = pipeline._spark()
    transformed = None
    try:
        bronze = read_bronze_dataframe(spark, runs)
        transformed = transform_bronze(
            bronze, pipeline.config, pipeline.catalog, version_id, preserve_observations=True
        )
        winners = utc_rows(transformed.valid)
        assert transformed.observations is not None
        observations = utc_rows(transformed.observations)
        summaries, links = observation_conditions(
            observations,
            winners,
            runs,
            scope_id=scope.scope_id,
            version_id=version_id,
            validation_version=pipeline.config.transformation_version,
        )
        validate_condition_set(
            summaries,
            links,
            scope_id=scope.scope_id,
            silver_version_id=version_id,
            contract_keys={row["contract_key"] for row in winners},
        )
        old_keys = [] if baseline is None else sorted(baseline.table["contract_key"].to_pylist())
        candidate_keys = sorted(row["contract_key"] for row in winners)
        baseline_ids = (
            set() if baseline is None else set(baseline.table["source_contract_id"].to_pylist())
        )
        drift = [
            _drift(row)
            for row in observations
            if row["bronze_run_id"] in campaign_run_ids
            and row.get("source_contract_id") in baseline_ids
            and (row["signing_date"] is not None or not row.get("signing_date_raw"))
            and not scope.contains(row["signing_date"], row.get("department_name"))
        ]
        unconfirmed = sorted(
            {
                value
                for acquisition in acquisitions
                for value in acquisition.get("unconfirmed_ids", [])
            }
        )
        acquisition_complete = bool(acquisitions) and all(
            item.get("state") == "acquired"
            and item.get("scope_scan_complete") is True
            and item.get("baseline_check_complete") is True
            and item.get("truncated") is False
            for item in acquisitions
        )
        receipt: dict[str, Any] = {
            "schema_version": "population-coverage-v1",
            "silver_version_id": version_id,
            "scope": scope.as_dict(),
            "scope_id": scope.scope_id,
            "mode": mode,
            "state": "complete" if acquisition_complete else "partial",
            "basis": "bounded_scan_observed" if acquisition_complete else "observed_inputs",
            "baseline": baseline_binding,
            "expected_parent": expected_parent,
            "bronze_runs": inputs,
            "baseline_keys_sha256": None if baseline is None else key_set_digest(old_keys),
            "candidate_keys_sha256": key_set_digest(candidate_keys),
            "acquisitions": acquisitions,
            "limitations": [
                "Missing signing dates are excluded from this population.",
                "Mutable source queries do not provide point-in-time snapshot isolation.",
                "Missing baseline responses are unconfirmed, not source deletions.",
            ],
            "condition_schema_version": "observation-condition-v1",
        }
        validate_coverage(receipt, silver_version_id=version_id)
        reconciliation = {
            "schema_version": "population-reconciliation-v1",
            "scope_id": scope.scope_id,
            "silver_version_id": version_id,
            "baseline_bronze_runs": dependencies,
            "baseline_keys": old_keys,
            "candidate_keys": candidate_keys,
            "admitted_keys": sorted(set(candidate_keys) - set(old_keys)),
            "scope_drift": drift,
            "unconfirmed_ids": unconfirmed,
            "historical_refresh_nodes": nodes,
            "counts": {
                "input_rows": transformed.counts["input_rows"],
                "output_rows": len(winners),
                "rejected_rows": transformed.counts["rejected_rows"],
                "unattributable_rejected_rows": int(transformed.counts["rejected_rows"])
                - len(
                    {
                        (
                            item["rejected_run_id"],
                            item["rejected_page_path"],
                            item["rejected_row_ordinal"],
                        )
                        for item in links
                    }
                ),
            },
        }
        validate_reconciliation(
            reconciliation, scope_id=scope.scope_id, silver_version_id=version_id
        )
        results = validate_quality(
            spark, transformed.valid, transformed.counts, pipeline.config, pipeline.catalog
        )
        preserved = set(old_keys) <= set(candidate_keys)
        in_scope = all(
            scope.contains(row["signing_date"], row["department_name"]) for row in winners
        )
        if mode == "authoritative_candidate":
            for check, passed in (
                ("POPULATION_COMPLETE", acquisition_complete),
                ("POPULATION_PRESERVATION", preserved),
                ("POPULATION_SCOPE", in_scope and not drift),
            ):
                results.append(QualityResult(check, "critical", passed, passed, True))
        metadata = {
            "mode": mode,
            "scope_id": scope.scope_id,
            "coverage_path": COVERAGE_PATH,
            "reconciliation_path": RECONCILIATION_PATH,
            "condition_summary_path": SUMMARY_PATH,
            "condition_links_path": LINKS_PATH,
            "condition_schema_version": "observation-condition-v1",
            "summary_rows": len(summaries),
            "link_rows": len(links),
            "scope_drift": bool(drift),
        }
        manifest = SilverManifest(
            silver_version_id=version_id,
            dataset_id=pipeline.config.dataset_id,
            transformation_version=pipeline.config.transformation_version,
            key_rule_version=pipeline.config.key_rule_version,
            contract_sha256=pipeline.contract.contract_hash,
            config_sha256=pipeline.config_sha256,
            territory_catalog_sha256=pipeline.catalog.digest,
            bronze_runs=inputs,
            counts=transformed.counts,
            quality_results=sorted(results, key=lambda result: result.check_id),
            status="staging",
            started_at_utc=started,
            schema_version=3,
            row_schema_version=2,
            writer_format_version="silver-population-v3",
            identity_rule_version=IDENTITY_RULE_VERSION,
            compatibility_sha256=pipeline.registry.digest,
            identity_coverage=identity_coverage(winners),
            input_purposes={run.run_id: run.purpose for run in runs},
            population=metadata,
            limitations=receipt["limitations"],
        )
        staging = pipeline.storage.create_staging(version_id)
        pipeline.storage.write_data(
            transformed.valid,
            transformed.rejected,
            version_id,
            codec=pipeline.config.parquet_codec,
            partitions=pipeline.config.output_partitions,
        )
        _write_evidence(staging, metadata, receipt, reconciliation, summaries, links)
        pipeline.storage.write_quality(version_id, manifest)
        manifest.files = pipeline.storage.inventory(version_id)
        manifest.status = "valid" if manifest.critical_passed else "invalid"
        manifest.finished_at_utc = pipeline._now()
        pipeline.storage.write_manifest(version_id, manifest)
        pipeline.storage.verify_staging(spark, manifest, codec=pipeline.config.parquet_codec)
        if manifest.critical_passed:
            pipeline.storage.promote(manifest)
        else:
            pipeline.storage.invalidate(manifest)
        return manifest
    finally:
        if transformed is not None:
            transformed.valid.unpersist()
            transformed.rejected.unpersist()
            if transformed.observations is not None:
                transformed.observations.unpersist()
        if owns_spark:
            spark.stop()


def utc_rows(frame: DataFrame) -> list[dict[str, Any]]:
    for name in ("bronze_fetched_at_utc",):
        frame = frame.withColumn(name, F.date_format(name, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"))
    return [row.asDict(recursive=True) for row in frame.collect()]


def _drift(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_contract_id": row["source_contract_id"],
        "contract_key": row["contract_key"],
        "bronze_run_id": row["bronze_run_id"],
        "bronze_page_path": row["bronze_page_path"],
        "bronze_row_ordinal": row["bronze_row_ordinal"],
        "signing_date": row["signing_date"].isoformat() if row["signing_date"] else None,
        "department": row.get("department_name"),
    }


def _write_evidence(
    staging: Path,
    metadata: dict[str, Any],
    receipt: dict[str, Any],
    reconciliation: dict[str, Any],
    summaries: list[dict[str, Any]],
    links: list[dict[str, Any]],
) -> None:
    for path, document, hash_name in (
        (COVERAGE_PATH, receipt, "coverage_sha256"),
        (RECONCILIATION_PATH, reconciliation, "reconciliation_sha256"),
    ):
        durable_write(staging / path, canonical_json(document) + b"\n")
        metadata[hash_name] = sha256_path(staging / path)
    for path, rows, schema, hash_name in (
        (SUMMARY_PATH, summaries, summary_arrow_schema(), "condition_summary_sha256"),
        (LINKS_PATH, links, links_arrow_schema(), "condition_links_sha256"),
    ):
        (staging / path).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pylist(rows, schema=schema), staging / path, compression="snappy"
        )
        metadata[hash_name] = sha256_path(staging / path)


def _baseline(
    pipeline: Any, version_id: str | None, expected_hash: str | None, scope: PopulationScope
) -> tuple[Any, list[dict[str, str]], list[dict[str, Any]]]:
    if (version_id is None) != (expected_hash is None):
        raise PublicationError("Population baseline requires version and manifest hash together")
    if version_id is None:
        return None, [], []
    assert expected_hash is not None
    if Path(version_id).name != version_id or version_id in (".", ".."):
        raise PublicationError("Invalid population baseline version")
    path = pipeline.storage.version_path(version_id) / "manifest.json"
    baseline = verify_manifest_bundle(path, expected_hash=expected_hash)
    manifest = json.loads(path.read_bytes())
    population = manifest.get("population")
    if population:
        if (
            population.get("scope_id") != scope.scope_id
            or population.get("mode") != "authoritative_candidate"
        ):
            raise PublicationError("Population baseline has incompatible scope or preview mode")
    else:
        adoption = adoption_record(pipeline.config.silver_root, version_id, expected_hash)
        if not adoption or adoption.get("scope_id") != scope.scope_id:
            raise PublicationError("Legacy population baseline requires explicit verified adoption")
    bound = binding(pipeline.config.silver_root)
    if bound and bound.get("scope_id") != scope.scope_id:
        raise PublicationError("Workspace is bound to a different population")
    dependencies = manifest.get("bronze_runs")
    if not isinstance(dependencies, list) or not dependencies:
        raise PublicationError("Population baseline lacks complete Bronze closure")
    nodes, seen = [], set()

    def visit(node_path: Path) -> None:
        if node_path in seen:
            raise PublicationError("Population baseline dependency cycle")
        seen.add(node_path)
        node = json.loads(node_path.read_bytes())
        verify_manifest_bundle(node_path)
        if node.get("refresh"):
            nodes.append(
                {
                    "silver_version_id": node["silver_version_id"],
                    "manifest_sha256": sha256_path(node_path),
                    "refresh_run_ids": sorted(
                        item["run_id"] for item in node["refresh"]["refresh_runs"]
                    ),
                }
            )
            ancestor = node["refresh"]["baseline_version_id"]
            visit(pipeline.storage.version_path(ancestor) / "manifest.json")
        if node.get("population"):
            receipt = json.loads(
                (node_path.parent / node["population"]["coverage_path"]).read_bytes()
            )
            parent = receipt.get("baseline")
            if parent:
                ancestor_path = (
                    pipeline.storage.version_path(parent["silver_version_id"]) / "manifest.json"
                )
                verify_manifest_bundle(ancestor_path, expected_hash=parent["manifest_sha256"])
                visit(ancestor_path)

    visit(path)
    return baseline, dependencies, nodes


def _verify_refresh_nodes(
    runs: tuple[VerifiedBronzeRun, ...], nodes: list[dict[str, Any]], root: Path
) -> None:
    proved = set()
    for node in nodes:
        path = root / "versions" / node["silver_version_id"] / "manifest.json"
        verify_manifest_bundle(path, expected_hash=node["manifest_sha256"])
        proved.update(node["refresh_run_ids"])
    refreshes = {run.run_id for run in runs if run.purpose == "identity_refresh"}
    if refreshes - proved:
        raise PublicationError(
            "Historical refresh run lacks its original exact immutable node proof"
        )
    if any(
        run.purpose not in ("incremental", "identity_refresh", "population_reconciliation")
        for run in runs
    ):
        raise PublicationError("Unsupported acquisition purpose in population closure")
