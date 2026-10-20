"""Baseline-pinned reconciliation for candidates that consume identity refreshes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from observatorio_secop.identity import key_set_digest
from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.models import VerifiedBronzeRun
from observatorio_secop.warehouse.bundle import VerifiedSilverBundle, verify_manifest_bundle
from observatorio_secop.warehouse.errors import SilverBundleError


def refresh_baseline(
    runs: tuple[VerifiedBronzeRun, ...], root: Path
) -> tuple[VerifiedSilverBundle, dict[str, Any]] | None:
    refreshes = [run for run in runs if run.purpose == "identity_refresh"]
    if any(run.purpose not in ("incremental", "identity_refresh") for run in runs):
        raise PublicationError("Unknown Bronze acquisition purpose")
    if not refreshes:
        return None
    contexts = _baseline_contexts(refreshes, root)
    choices = []
    for identity, context in contexts.items():
        dependency_keys = {(item["run_id"], item["manifest_sha256"]) for item in context[1]}
        additions = [
            run for run in refreshes if (run.run_id, run.manifest_sha256) not in dependency_keys
        ]
        if additions and all(
            (run.parameters["baseline_version_id"], run.parameters["baseline_manifest_sha256"])
            == identity
            for run in additions
        ):
            choices.append(context)
    if len(choices) != 1:
        raise PublicationError("Identity refresh inputs do not extend one pinned baseline")
    baseline, dependencies = choices[0]
    selected = {(run.run_id, run.manifest_sha256) for run in runs}
    if not all((run["run_id"], run["manifest_sha256"]) in selected for run in dependencies):
        raise PublicationError("Identity refresh selection omits baseline Bronze dependencies")
    summaries = []
    for run in refreshes:
        own_baseline, own_dependencies = contexts[
            (run.parameters["baseline_version_id"], run.parameters["baseline_manifest_sha256"])
        ]
        if (
            not run.range_complete
            or run.truncated
            or not run.refresh
            or run.refresh.get("complete") is not True
            or run.refresh.get("baseline_bronze_runs") != own_dependencies
        ):
            raise PublicationError(
                "Identity refresh evidence is incomplete or uses another baseline"
            )
        ids = run.parameters.get("requested_ids")
        if (
            not isinstance(ids, list)
            or not ids
            or len(ids) != len(set(ids))
            or key_set_digest(ids) != run.parameters.get("ids_sha256")
            or not set(ids) <= set(own_baseline.table["source_contract_id"].to_pylist())
        ):
            raise PublicationError("Identity refresh ID set is inconsistent with baseline")
        summaries.append(
            {
                "run_id": run.run_id,
                "manifest_sha256": run.manifest_sha256,
                "complete": True,
                "truncated": False,
                "ids_sha256": run.parameters["ids_sha256"],
            }
        )
    return baseline, {
        "complete": True,
        "baseline_version_id": baseline.version_id,
        "baseline_manifest_sha256": baseline.manifest_sha256,
        "baseline_bronze_runs": dependencies,
        "baseline_keys_sha256": key_set_digest(baseline.table["contract_key"].to_pylist()),
        "refresh_runs": summaries,
    }


def _baseline_contexts(
    refreshes: list[VerifiedBronzeRun], root: Path
) -> dict[tuple[str, str], tuple[VerifiedSilverBundle, list[dict[str, str]]]]:
    contexts = {}
    for run in refreshes:
        baseline_id = run.parameters.get("baseline_version_id")
        expected_hash = run.parameters.get("baseline_manifest_sha256")
        if (
            not isinstance(baseline_id, str)
            or baseline_id in (".", "..")
            or Path(baseline_id).name != baseline_id
            or not isinstance(expected_hash, str)
            or not expected_hash
        ):
            raise PublicationError("Refresh requires a pinned baseline version/hash")
        identity = (baseline_id, expected_hash)
        if identity in contexts:
            continue
        path = root / "versions" / baseline_id / "manifest.json"
        try:
            baseline = verify_manifest_bundle(path, expected_hash=expected_hash)
            document = json.loads(path.read_bytes())
        except (SilverBundleError, OSError, ValueError) as error:
            raise PublicationError(f"Invalid identity refresh baseline: {error}") from error
        dependencies = document.get("bronze_runs")
        if not isinstance(dependencies, list) or not dependencies:
            raise PublicationError("Identity baseline has no pinned Bronze dependencies")
        contexts[identity] = baseline, dependencies
    return contexts


def reconcile_refresh(
    valid: DataFrame, context: tuple[VerifiedSilverBundle, dict[str, Any]] | None
) -> dict[str, Any] | None:
    if context is None:
        return None
    baseline, evidence = context
    # TimestampType.collect() converts through Python's host timezone. Compare the
    # source clock in the configured UTC calculation context without inferring a zone.
    rows = [
        row.asDict()
        for row in valid.withColumn(
            "source_updated_at", F.date_format("source_updated_at", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        ).collect()
    ]
    evidence["candidate_keys_sha256"] = key_set_digest([row["contract_key"] for row in rows])
    if evidence["candidate_keys_sha256"] != evidence["baseline_keys_sha256"]:
        raise PublicationError("Identity refresh changes the baseline contractual key set")
    old = {row["contract_key"]: row for row in baseline.table.to_pylist()}
    for before in old.values():
        if before["source_updated_at"] is not None:
            before["source_updated_at"] = before["source_updated_at"].isoformat(
                timespec="microseconds"
            )
    business_fields = (
        "contract_reference",
        "procurement_process_id",
        "contract_value_cop",
        "contract_status",
        "signing_date",
        "start_date",
        "end_date",
        "source_updated_at",
        "source_url",
        "municipality_key",
        "procurement_method",
        "contract_type",
        "main_category_code",
        "process_description",
    )
    evidence["business_changed_contracts"] = sum(
        any(row[field] != old[row["contract_key"]][field] for field in business_fields)
        for row in rows
    )
    changes = []
    identity_fields = tuple(
        f"{role}_{suffix}"
        for role in ("entity", "supplier")
        for suffix in ("code", "name", "identity_status", "identity_reason")
    )
    for row in sorted(rows, key=lambda item: item["contract_key"]):
        before = old[row["contract_key"]]
        business_changes = [field for field in business_fields if row[field] != before[field]]
        identity_changes = [field for field in identity_fields if row[field] != before[field]]
        if business_changes or identity_changes:
            changes.append(
                {
                    "contract_key": row["contract_key"],
                    "source_contract_id": row["source_contract_id"],
                    "business_changed_fields": business_changes,
                    "identity_changed_fields": identity_changes,
                    "before_value_cop": str(before["contract_value_cop"]),
                    "after_value_cop": str(row["contract_value_cop"]),
                }
            )
    evidence["changed_contracts"] = changes
    for role in ("entity", "supplier"):
        evidence[f"{role}_identity_completed"] = sum(
            old[row["contract_key"]][f"{role}_identity_status"] == "UNKNOWN"
            and row[f"{role}_identity_status"] == "KNOWN"
            for row in rows
        )
        evidence[f"{role}_still_unknown"] = sum(
            row[f"{role}_identity_status"] == "UNKNOWN" for row in rows
        )
    return evidence
