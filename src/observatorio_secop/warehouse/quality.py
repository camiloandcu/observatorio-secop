"""Verified, sanitized observation quality evidence and transactional persistence."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from observatorio_secop.population import digest_json
from observatorio_secop.warehouse.config import IDENTIFIER, WarehouseConfig
from observatorio_secop.warehouse.errors import (
    SilverBundleError,
    WarehouseConfigurationError,
    WarehouseConflictError,
)
from observatorio_secop.warehouse.inventory import sha256_path

COUNTERS = {
    "input_observations": "input_rows",
    "output_contracts": "output_rows",
    "rejected_observations": "rejected_rows",
    "exact_duplicates": "exact_duplicates",
    "superseded_valid_observations": "superseded_versions",
}
# Only row-level controls have a selected-contract denominator. CRITICAL_NULLS
# counts cells; schema/population controls evaluate a bundle, not contracts.
CONTRACT_CONTROLS = {
    "IDENTITY_CONSISTENCY",
    "UNIQUE_CONTRACT_KEY",
    "AMOUNT_VALID",
    "DATE_COHERENCE",
    "TERRITORY_CONSISTENCY",
    "UNKNOWN_MUNICIPALITY",
}


def _integer(value: Any) -> int:
    if type(value) is not int or value < 0:
        raise SilverBundleError("Quality counts must be non-negative integers")
    return int(value)


def verify_quality_summary(manifest: dict[str, Any], directory: Path) -> dict[str, Any]:
    """Call after full inventory verification; never parse an unreferenced file."""
    declared = [item for item in manifest["files"] if item["path"] == "quality/results.json"]
    counts = manifest.get("counts", {})
    writer = manifest.get("writer_format_version")
    if writer is not None and writer not in ("silver-parquet-v2", "silver-population-v3"):
        raise SilverBundleError("Unsupported Silver quality writer format")
    current = writer in ("silver-parquet-v2", "silver-population-v3")
    available = bool(declared) and all(key in counts for key in COUNTERS.values())
    if (current or declared) and not available:
        raise SilverBundleError("Current Silver quality evidence is missing")
    summary: dict[str, Any] = {
        "schema_version": "silver-quality-summary-v1",
        "state": "AVAILABLE" if available else "UNAVAILABLE_LEGACY",
        "reason": None if available else "LEGACY_EVIDENCE_ABSENT",
        "counts": None,
        "controls": [],
        "quality_results_sha256": None,
        "bronze_runs": [],
        "pipeline_completed_at_utc": manifest["finished_at_utc"],
    }
    if not available:
        return summary
    mapped = {target: _integer(counts[source]) for target, source in COUNTERS.items()}
    if mapped["input_observations"] != sum(
        mapped[key]
        for key in (
            "output_contracts",
            "rejected_observations",
            "exact_duplicates",
            "superseded_valid_observations",
        )
    ):
        raise SilverBundleError("Quality observation accounting differs")
    mapped["accepted_valid_observations"] = (
        mapped["input_observations"] - mapped["rejected_observations"]
    )
    path = directory / "quality/results.json"
    try:
        results = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise SilverBundleError("Could not parse verified quality results") from error
    if not isinstance(results, list) or results != manifest.get("quality_results"):
        raise SilverBundleError("Quality results differ from manifest")
    controls, seen = [], set()
    for result in results:
        if not isinstance(result, dict):
            raise SilverBundleError("Invalid quality control")
        check = result.get("check_id")
        if (
            not isinstance(check, str)
            or not re.fullmatch(r"[A-Z][A-Z0-9_]*", check)
            or check in seen
            or result.get("severity") not in ("critical", "warning")
            or type(result.get("passed")) is not bool
        ):
            raise SilverBundleError("Invalid quality control metadata")
        seen.add(check)
        affected = _integer(result.get("affected_rows", 0))
        denominator = mapped["output_contracts"] if check in CONTRACT_CONTROLS else None
        if check == "REJECTION_RATE":
            denominator = mapped["input_observations"]
            observed = result.get("observed")
            if (
                not isinstance(observed, dict)
                or any(
                    observed.get(source) != counts[source]
                    for source in ("input_rows", "rejected_rows")
                )
                or affected != mapped["rejected_observations"]
            ):
                raise SilverBundleError("Quality rejection denominator differs")
        if denominator is not None and affected > denominator:
            raise SilverBundleError("Quality affected count exceeds denominator")
        if check == "COUNT_RECONCILIATION" and (
            result.get("observed") != mapped["input_observations"]
            or result.get("expected") != mapped["input_observations"]
        ):
            raise SilverBundleError("Quality reconciliation control differs")
        controls.append(
            {
                "check_id": check,
                "severity": result["severity"],
                "passed": result["passed"],
                "affected_rows": affected,
                "denominator": denominator,
                "denominator_grain": (
                    "observations"
                    if check == "REJECTION_RATE"
                    else "selected_contracts"
                    if denominator is not None
                    else "bundle"
                ),
            }
        )
    runs = manifest.get("bronze_runs")
    if not isinstance(runs, list) or not runs:
        raise SilverBundleError("Quality Bronze inventory is missing")
    sanitized = []
    for run in runs:
        if (
            not isinstance(run, dict)
            or not isinstance(run.get("run_id"), str)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", run["run_id"])
            or not re.fullmatch(r"[a-f0-9]{64}", str(run.get("manifest_sha256", "")))
        ):
            raise SilverBundleError("Invalid quality Bronze inventory")
        sanitized.append({"run_id": run["run_id"], "manifest_sha256": run["manifest_sha256"]})
    summary.update(
        counts=mapped,
        controls=sorted(controls, key=lambda row: row["check_id"]),
        bronze_runs=sorted(sanitized, key=lambda row: row["run_id"]),
        quality_results_sha256=sha256_path(path),
    )
    return summary


class QualityStore:
    """Immutable snapshot evidence under the loader's transaction/advisory lock."""

    def __init__(self, config: WarehouseConfig) -> None:
        name = config.contract_table + "_quality"
        if not IDENTIFIER.fullmatch(name) or len(name.encode()) > 63 or name == config.audit_table:
            raise WarehouseConfigurationError("Quality relation name collides or exceeds limits")
        self.relation = sql.Identifier(config.schema, name)

    def persist(self, cursor: psycopg.Cursor[Any], bundle: Any) -> None:
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (silver_version_id text NOT NULL, "
                "manifest_sha256 text NOT NULL, scope_id text, summary jsonb NOT NULL, "
                "evidence_sha256 text NOT NULL, PRIMARY KEY(silver_version_id,manifest_sha256))"
            ).format(self.relation)
        )
        summary = getattr(bundle, "quality_summary", None)
        if summary is None:
            summary = {
                "schema_version": "silver-quality-summary-v1",
                "state": "UNAVAILABLE_LEGACY",
                "reason": "LEGACY_EVIDENCE_ABSENT",
                "counts": None,
                "controls": [],
                "quality_results_sha256": None,
                "bronze_runs": [],
                "pipeline_completed_at_utc": bundle.manifest_finished_at.isoformat(),
            }
        identity = (bundle.version_id, bundle.manifest_sha256)
        coverage = getattr(bundle, "coverage", None)
        scope_id = coverage["scope_id"] if coverage else None
        checksum = digest_json(summary)
        cursor.execute(
            sql.SQL(
                "SELECT scope_id,summary,evidence_sha256 FROM {} "
                "WHERE silver_version_id=%s AND manifest_sha256=%s"
            ).format(self.relation),
            identity,
        )
        existing = cursor.fetchone()
        if existing is not None:
            if existing != (scope_id, summary, checksum):
                raise WarehouseConflictError("Immutable quality evidence conflict")
            return
        cursor.execute(
            sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s)").format(self.relation),
            (*identity, scope_id, Jsonb(summary), checksum),
        )
