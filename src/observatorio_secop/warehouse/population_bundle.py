"""Verify envelope-3 auxiliary evidence without granting publication authority."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from observatorio_secop.identity import key_set_digest
from observatorio_secop.ingestion.compatibility import CompatibilityRegistry
from observatorio_secop.population import (
    PopulationScope,
    links_arrow_schema,
    summary_arrow_schema,
    validate_condition_set,
    validate_coverage,
    validate_reconciliation,
)
from observatorio_secop.population.core import boolean, digest, integer, object_fields, utc_text
from observatorio_secop.warehouse.errors import SilverBundleError
from observatorio_secop.warehouse.inventory import _inventory_path, sha256_path

_ARTIFACTS = {
    "coverage": "evidence/population_coverage.json",
    "reconciliation": "evidence/population_reconciliation.json",
    "condition_summary": "evidence/observation_conditions/summary.parquet",
    "condition_links": "evidence/observation_conditions/links.parquet",
}
_METADATA_FIELDS = {
    "mode",
    "scope_id",
    "condition_schema_version",
    "summary_rows",
    "link_rows",
    "scope_drift",
    *(f"{role}_{suffix}" for role in _ARTIFACTS for suffix in ("path", "sha256")),
}
RelatedVerifier = Callable[[Path, str], Any]


def verify_population_bundle(
    table: pa.Table,
    manifest: dict[str, Any],
    manifest_path: Path,
    verify_related: RelatedVerifier,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate independently hashed sidecars, their joins, and pinned ancestry."""
    try:
        return _verify(table, manifest, manifest_path, verify_related)
    except SilverBundleError:
        raise
    except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as error:
        raise SilverBundleError(f"Invalid population bundle evidence: {error}") from error


def _verify(
    table: pa.Table,
    manifest: dict[str, Any],
    path: Path,
    verify_related: RelatedVerifier,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    metadata = object_fields(manifest.get("population"), _METADATA_FIELDS, "population metadata")
    version = manifest["silver_version_id"]
    files = {role: _artifact(path.parent, manifest, metadata, role) for role in _ARTIFACTS}
    coverage = validate_coverage(_json(files["coverage"]), silver_version_id=version)
    scope = PopulationScope.from_dict(coverage["scope"])
    reconciliation = validate_reconciliation(
        _json(files["reconciliation"]),
        scope_id=scope.scope_id,
        silver_version_id=version,
    )
    if (
        metadata["mode"] != coverage["mode"]
        or metadata["scope_id"] != scope.scope_id
        or metadata["condition_schema_version"] != coverage["condition_schema_version"]
        or coverage["bronze_runs"] != manifest.get("bronze_runs")
        or boolean(metadata["scope_drift"], "scope_drift") != bool(reconciliation["scope_drift"])
    ):
        raise SilverBundleError("Population manifest, receipt and reconciliation disagree")
    rows = table.to_pylist()
    keys = set(table["contract_key"].to_pylist())
    if (
        set(reconciliation["candidate_keys"]) != keys
        or coverage["candidate_keys_sha256"] != key_set_digest(list(keys))
        or reconciliation["counts"]["output_rows"] != table.num_rows
        or any(
            reconciliation["counts"][key] != manifest["counts"].get(key)
            for key in ("input_rows", "output_rows", "rejected_rows")
        )
    ):
        raise SilverBundleError("Population keys, digest or counts differ from contract rows")
    unconfirmed = {
        identity for item in coverage["acquisitions"] for identity in item["unconfirmed_ids"]
    }
    if unconfirmed != set(reconciliation["unconfirmed_ids"]):
        raise SilverBundleError("Unconfirmed members differ from acquisition receipts")
    inputs = {(run["run_id"], run["manifest_sha256"]) for run in coverage["bronze_runs"]}
    purposes = manifest.get("input_purposes")
    if not isinstance(purposes, dict) or set(purposes) != {run for run, _ in inputs}:
        raise SilverBundleError("Population input-purpose identities differ")
    if any(
        purposes.get(item["campaign_id"]) != "population_reconciliation"
        for item in coverage["acquisitions"]
    ):
        raise SilverBundleError("Coverage campaign lacks reconciliation purpose")
    summaries = _parquet(
        files["condition_summary"], summary_arrow_schema(), metadata["summary_rows"]
    )
    links = _parquet(files["condition_links"], links_arrow_schema(), metadata["link_rows"])
    summaries, links = validate_condition_set(
        summaries,
        links,
        scope_id=scope.scope_id,
        silver_version_id=version,
        contract_keys=keys,
    )
    if reconciliation["counts"]["unattributable_rejected_rows"] != (
        reconciliation["counts"]["rejected_rows"] - len(links)
    ):
        raise SilverBundleError("Attributable rejection links do not reconcile with reject counts")
    campaign_ids = {item["campaign_id"] for item in coverage["acquisitions"]}
    if any(
        row["bronze_run_id"] not in campaign_ids
        or scope.contains(row["signing_date"], row["department"])
        for row in reconciliation["scope_drift"]
    ):
        raise SilverBundleError("Drift evidence lacks an incompatible campaign observation")
    _winners(rows, summaries, links, purposes)
    _baseline(path, coverage, reconciliation, inputs, scope, verify_related)
    _refresh_nodes(path, reconciliation, inputs, purposes, verify_related)
    if coverage["mode"] == "authoritative_candidate" and (
        coverage["state"] != "complete"
        or reconciliation["scope_drift"]
        or not set(reconciliation["baseline_keys"]) <= keys
        or any(not scope.contains(row["signing_date"], row["department_name"]) for row in rows)
    ):
        raise SilverBundleError("Authoritative population has drift or loses baseline membership")
    return coverage, reconciliation, summaries, links


def _artifact(
    directory: Path,
    manifest: dict[str, Any],
    metadata: dict[str, Any],
    role: str,
) -> Path:
    relative = _ARTIFACTS[role]
    if metadata[f"{role}_path"] != relative:
        raise SilverBundleError(f"Unsupported population evidence role path: {role}")
    expected = digest(metadata[f"{role}_sha256"], f"{role}_sha256")
    path = _inventory_path(directory.resolve(), relative)
    entries = [item for item in manifest["files"] if item.get("path") == relative]
    if len(entries) != 1:
        raise SilverBundleError(f"Population evidence must be inventoried exactly once: {role}")
    entry = entries[0]
    if (
        entry.get("sha256") != expected
        or integer(entry.get("size_bytes"), "size_bytes") != path.stat().st_size
        or sha256_path(path) != expected
    ):
        raise SilverBundleError(f"Population evidence hash/size differs: {role}")
    return path


def _json(path: Path) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise SilverBundleError("Duplicate JSON evidence field")
            result[key] = value
        return result

    value = json.loads(path.read_bytes(), object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise SilverBundleError("Population evidence must be an object")
    return value


def _parquet(path: Path, schema: pa.Schema, count: object) -> list[dict[str, Any]]:
    table = pq.ParquetFile(path).read()
    if not table.schema.equals(schema, check_metadata=False):
        raise SilverBundleError("Population condition Parquet schema differs")
    if table.num_rows != integer(count, "condition row count"):
        raise SilverBundleError("Population condition Parquet count differs")
    if any(table[field.name].null_count for field in schema):
        raise SilverBundleError("Population condition required fields contain nulls")
    return list(table.to_pylist())


def _winners(
    rows: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
    links: list[dict[str, Any]],
    purposes: dict[str, str],
) -> None:
    by_key = {row["contract_key"]: row for row in rows}
    columns = {
        "winner_payload_sha256": "payload_sha256",
        "winner_run_id": "bronze_run_id",
        "winner_page_path": "bronze_page_path",
        "winner_row_ordinal": "bronze_row_ordinal",
        "winner_profile_sha256": "source_contract_sha256",
        "winner_projection_version": "source_projection_version",
    }
    for summary in summaries:
        row = by_key[summary["contract_key"]]
        observed = row["bronze_fetched_at_utc"]
        # Canonical contract TimestampType represents acquisition UTC without a
        # zone marker; never apply this conversion to a source-calendar field.
        if isinstance(observed, datetime) and observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        if any(summary[field] != row[column] for field, column in columns.items()) or summary[
            "winner_observed_at_utc"
        ] != utc_text(observed):
            raise SilverBundleError("Condition summary differs from actual selected winner")
    profiles = {
        (item.contract_sha256, item.projection_version) for item in CompatibilityRegistry().profiles
    }
    for link in links:
        if (
            link["rejected_run_id"] not in purposes
            or (link["rejected_profile_sha256"], link["rejected_projection_version"])
            not in profiles
        ):
            raise SilverBundleError("Rejected-observation profile/input is unsupported")


def _related_path(path: Path, identifier: str) -> Path:
    if (
        not isinstance(identifier, str)
        or Path(identifier).name != identifier
        or identifier in (".", "..")
    ):
        raise SilverBundleError("Invalid related population version")
    target = path.parent.parent / identifier / "manifest.json"
    if (
        target.resolve() == path.resolve()
        or target.resolve().parent.parent != path.resolve().parent.parent
    ):
        raise SilverBundleError("Population ancestry escapes versions or references itself")
    return target


def _baseline(
    path: Path,
    coverage: dict[str, Any],
    reconciliation: dict[str, Any],
    inputs: set[tuple[str, str]],
    scope: PopulationScope,
    verify_related: RelatedVerifier,
) -> None:
    binding = coverage["baseline"]
    if binding is None:
        if (
            reconciliation["baseline_keys"]
            or reconciliation["baseline_bronze_runs"]
            or reconciliation["scope_drift"]
        ):
            raise SilverBundleError("Bootstrap contains contradictory baseline evidence")
        return
    target = _related_path(path, binding["silver_version_id"])
    baseline = verify_related(target, binding["manifest_sha256"])
    document = _json(target)
    keys = baseline.table["contract_key"].to_pylist()
    baseline_ids = {
        row["contract_key"]: row["source_contract_id"] for row in baseline.table.to_pylist()
    }
    if any(
        baseline_ids.get(row["contract_key"]) != row["source_contract_id"]
        for row in reconciliation["scope_drift"]
    ):
        raise SilverBundleError("Observed scope drift is not attributable to a baseline identity")
    if (
        set(reconciliation["baseline_keys"]) != set(keys)
        or key_set_digest(keys) != binding["keys_sha256"]
        or reconciliation["baseline_bronze_runs"] != document.get("bronze_runs")
        or not reconciliation["baseline_bronze_runs"]
        or not {
            (row["run_id"], row["manifest_sha256"])
            for row in reconciliation["baseline_bronze_runs"]
        }
        <= inputs
    ):
        raise SilverBundleError("Population baseline key digest or complete closure differs")
    if document.get("schema_version", 1) == 3:
        population = document.get("population", {})
        if (
            population.get("scope_id") != scope.scope_id
            or population.get("mode") != "authoritative_candidate"
        ):
            raise SilverBundleError("Population baseline scope or authority differs")
    else:
        # Delayed import avoids coupling standalone acquisition contracts to Spark.
        from observatorio_secop.processing.silver.errors import PublicationError
        from observatorio_secop.processing.silver.selection import (
            adoption_record,
            validate_adoption,
        )

        try:
            adoption = adoption_record(
                path.parents[2], baseline.version_id, baseline.manifest_sha256
            )
            if not adoption or adoption.get("scope_id") != scope.scope_id:
                raise SilverBundleError("Legacy population baseline lacks explicit scope adoption")
            validate_adoption(adoption, baseline)
        except PublicationError as error:
            raise SilverBundleError(f"Invalid legacy population adoption: {error}") from error


def _refresh_nodes(
    path: Path,
    reconciliation: dict[str, Any],
    inputs: set[tuple[str, str]],
    purposes: dict[str, str],
    verify_related: RelatedVerifier,
) -> None:
    proved = set()
    for node in reconciliation["historical_refresh_nodes"]:
        target = _related_path(path, node["silver_version_id"])
        verify_related(target, node["manifest_sha256"])
        document = _json(target)
        refresh = document.get("refresh")
        if not isinstance(refresh, dict) or refresh.get("complete") is not True:
            raise SilverBundleError("Historical refresh proof lacks original exact refresh node")
        original = {item["run_id"] for item in refresh["refresh_runs"]}
        node_inputs = {
            (item["run_id"], item["manifest_sha256"]) for item in document["bronze_runs"]
        }
        if original != set(node["refresh_run_ids"]) or not node_inputs <= inputs:
            raise SilverBundleError("Historical refresh run/dependency proof differs")
        proved.update(original)
    if proved != {run for run, purpose in purposes.items() if purpose == "identity_refresh"}:
        raise SilverBundleError("Historical identity-refresh proofs do not cover exact inputs")
