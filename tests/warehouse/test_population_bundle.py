"""Arrow-only trust-boundary checks for population sidecars and pinned ancestry."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from observatorio_secop.identity import key_set_digest
from observatorio_secop.population import (
    PopulationScope,
    canonical_json,
    condition_link_id,
    digest_json,
    links_arrow_schema,
    summary_arrow_schema,
)
from observatorio_secop.warehouse.bundle import IDENTITY_SCHEMA
from observatorio_secop.warehouse.errors import SilverBundleError
from observatorio_secop.warehouse.inventory import sha256_path
from observatorio_secop.warehouse.population_bundle import verify_population_bundle

from .conftest import identity_row, write_identity_bundle
from .inventory_helpers import inventory_entry

KEY, HASH = "b" * 64, "a" * 64
SCOPE = PopulationScope("2026-01-01", "2027-01-01")
PATHS = {
    "coverage": "evidence/population_coverage.json",
    "reconciliation": "evidence/population_reconciliation.json",
    "condition_summary": "evidence/observation_conditions/summary.parquet",
    "condition_links": "evidence/observation_conditions/links.parquet",
}


def fixture(root: Path, version: str = "A", run_id: str = "run-1") -> dict[str, Any]:
    row = identity_row(version, KEY)
    row["bronze_run_id"] = run_id
    path = write_identity_bundle(root, version, [row])
    manifest = json.loads(path.read_bytes())
    manifest.update(schema_version=3, row_schema_version=2)
    manifest["bronze_runs"] = [{"run_id": run_id, "manifest_sha256": HASH}]
    manifest["input_purposes"] = {run_id: "population_reconciliation"}
    manifest["counts"] = {"input_rows": 1, "output_rows": 1, "rejected_rows": 0}
    table = pa.Table.from_pylist([row], schema=IDENTITY_SCHEMA)
    acquisition = {
        "schema_version": "population-acquisition-v1",
        "campaign_id": run_id,
        "plan_id": HASH,
        "scope_id": SCOPE.scope_id,
        "baseline": None,
        "state": "acquired",
        "limits": {"rows": 10, "attempts": 5},
        "consumed": {"rows": 1, "attempts": 2},
        "scope_scan_complete": True,
        "baseline_check_complete": True,
        "unconfirmed_ids": [],
        "observed_from_utc": "2026-01-03T00:00:00Z",
        "observed_to_utc": "2026-01-03T00:00:01Z",
        "truncated": False,
        "limitations": ["Mutable source."],
    }
    coverage = {
        "schema_version": "population-coverage-v1",
        "silver_version_id": version,
        "scope": SCOPE.as_dict(),
        "scope_id": SCOPE.scope_id,
        "mode": "authoritative_candidate",
        "state": "complete",
        "basis": "bounded_scan_observed",
        "baseline": None,
        "expected_parent": None,
        "bronze_runs": deepcopy(manifest["bronze_runs"]),
        "baseline_keys_sha256": None,
        "candidate_keys_sha256": key_set_digest([KEY]),
        "acquisitions": [acquisition],
        "limitations": ["Missing signing dates excluded."],
        "condition_schema_version": "observation-condition-v1",
    }
    reconciliation = {
        "schema_version": "population-reconciliation-v1",
        "scope_id": SCOPE.scope_id,
        "silver_version_id": version,
        "baseline_bronze_runs": [],
        "baseline_keys": [],
        "candidate_keys": [KEY],
        "admitted_keys": [KEY],
        "scope_drift": [],
        "unconfirmed_ids": [],
        "historical_refresh_nodes": [],
        "counts": {**manifest["counts"], "unattributable_rejected_rows": 0},
    }
    summary = {
        "schema_version": "observation-condition-v1",
        "scope_id": SCOPE.scope_id,
        "silver_version_id": version,
        "contract_key": KEY,
        "state": "NO_LATER_INVALID_OBSERVED",
        "winner_payload_sha256": row["payload_sha256"],
        "winner_run_id": run_id,
        "winner_page_path": row["bronze_page_path"],
        "winner_page_sha256": HASH,
        "winner_row_ordinal": row["bronze_row_ordinal"],
        "winner_observed_at_utc": row["bronze_fetched_at_utc"].replace(tzinfo=UTC),
        "winner_profile_sha256": row["source_contract_sha256"],
        "winner_projection_version": row["source_projection_version"],
        "validation_version": "v3",
        "later_invalid_count": 0,
        "equal_invalid_count": 0,
    }
    manifest["population"] = {
        "mode": coverage["mode"],
        "scope_id": SCOPE.scope_id,
        "condition_schema_version": "observation-condition-v1",
        "summary_rows": 1,
        "link_rows": 0,
        "scope_drift": False,
        **{f"{role}_path": relative for role, relative in PATHS.items()},
    }
    result = {
        "path": path,
        "manifest": manifest,
        "table": table,
        "coverage": coverage,
        "reconciliation": reconciliation,
        "condition_summary": [summary],
        "condition_links": [],
    }
    persist(result)
    return result


def persist(value: dict[str, Any]) -> None:
    manifest = value["manifest"]
    metadata = manifest["population"]
    for role, relative in PATHS.items():
        path = value["path"].parent / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if role in ("coverage", "reconciliation"):
            path.write_bytes(canonical_json(value[role]) + b"\n")
        else:
            schema = summary_arrow_schema() if role == "condition_summary" else links_arrow_schema()
            pq.write_table(pa.Table.from_pylist(value[role], schema=schema), path)
        metadata[f"{role}_sha256"] = sha256_path(path)
    manifest["files"] = [
        entry for entry in manifest["files"] if entry["path"] not in PATHS.values()
    ]
    manifest["files"].extend(
        inventory_entry(value["path"].parent / relative, value["path"].parent)
        for relative in PATHS.values()
    )
    value["path"].write_bytes(canonical_json(manifest))


def no_related(path: Path, expected_hash: str) -> Any:
    raise AssertionError(f"Unexpected ancestry lookup: {path} {expected_hash}")


def verify(value: dict[str, Any], related=no_related):
    return verify_population_bundle(value["table"], value["manifest"], value["path"], related)


def baseline_candidate(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    base = fixture(root)
    value = fixture(root, "B", "run-2")
    binding = {
        "silver_version_id": "A",
        "manifest_sha256": sha256_path(base["path"]),
        "keys_sha256": key_set_digest([KEY]),
    }
    value["coverage"].update(
        baseline=binding,
        baseline_keys_sha256=binding["keys_sha256"],
        expected_parent={key: binding[key] for key in ("silver_version_id", "manifest_sha256")},
    )
    value["coverage"]["acquisitions"][0]["baseline"] = binding
    value["manifest"]["bronze_runs"] = (
        base["manifest"]["bronze_runs"] + value["manifest"]["bronze_runs"]
    )
    value["coverage"]["bronze_runs"] = deepcopy(value["manifest"]["bronze_runs"])
    value["manifest"]["input_purposes"].update(base["manifest"]["input_purposes"])
    value["reconciliation"].update(
        baseline_bronze_runs=base["manifest"]["bronze_runs"], baseline_keys=[KEY], admitted_keys=[]
    )
    persist(value)
    return base, value


def related(base: dict[str, Any]):
    def callback(path: Path, expected_hash: str):
        assert path == base["path"] and expected_hash == sha256_path(path)
        return SimpleNamespace(
            table=base["table"],
            version_id="A",
            manifest_path=path,
            manifest_sha256=sha256_path(path),
        )

    return callback


def test_bootstrap_sidecars_join_exact_winners_and_are_json_serializable(tmp_path):
    value = fixture(tmp_path)
    coverage, reconciliation, summaries, links = verify(value)
    assert coverage["state"] == "complete" and reconciliation["candidate_keys"] == [KEY]
    assert summaries[0]["winner_observed_at_utc"].endswith("Z") and links == []
    json.dumps(summaries)


@pytest.mark.parametrize("role", PATHS)
def test_each_auxiliary_file_is_bound_by_inventory_and_metadata(tmp_path, role):
    value = fixture(tmp_path)
    path = value["path"].parent / PATHS[role]
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(SilverBundleError, match="hash/size"):
        verify(value)


@pytest.mark.parametrize("case", ["undeclared", "duplicate", "symlink", "path", "count", "schema"])
def test_sidecar_policies_do_not_fall_back_to_contract_rows(tmp_path, case):
    value = fixture(tmp_path)
    metadata, manifest = value["manifest"]["population"], value["manifest"]
    path = value["path"].parent / PATHS["condition_summary"]
    if case == "undeclared":
        manifest["files"] = [
            entry for entry in manifest["files"] if entry["path"] != PATHS["condition_summary"]
        ]
    elif case == "duplicate":
        manifest["files"].append(
            next(
                entry for entry in manifest["files"] if entry["path"] == PATHS["condition_summary"]
            )
        )
    elif case == "symlink":
        other = path.with_name("other.parquet")
        path.rename(other)
        path.symlink_to(other.name)
    elif case == "path":
        metadata["condition_summary_path"] = "../outside.parquet"
    elif case == "count":
        metadata["summary_rows"] = True
    else:
        pq.write_table(pa.table({"fake": [1]}), path)
        metadata["condition_summary_sha256"] = sha256_path(path)
        manifest["files"] = [
            inventory_entry(path, value["path"].parent)
            if entry["path"] == PATHS["condition_summary"]
            else entry
            for entry in manifest["files"]
        ]
    with pytest.raises(SilverBundleError):
        verify(value)


@pytest.mark.parametrize("case", ["key", "winner", "input", "counts", "unconfirmed", "metadata"])
def test_consistent_hashes_cannot_authorize_inconsistent_semantics(tmp_path, case):
    value = fixture(tmp_path)
    if case == "key":
        value["coverage"]["candidate_keys_sha256"] = HASH
    elif case == "winner":
        value["condition_summary"][0]["winner_payload_sha256"] = HASH
    elif case == "input":
        value["manifest"]["bronze_runs"][0]["manifest_sha256"] = KEY
    elif case == "counts":
        value["manifest"]["counts"]["input_rows"] = 2
    elif case == "unconfirmed":
        value["reconciliation"]["unconfirmed_ids"] = ["imaginary"]
    else:
        value["manifest"]["population"]["scope_drift"] = True
    persist(value)
    with pytest.raises(SilverBundleError):
        verify(value)


def test_baseline_digest_closure_and_ancestry_callback_are_mandatory(tmp_path):
    base, value = baseline_candidate(tmp_path)
    assert verify(value, related(base))[0]["baseline"]["silver_version_id"] == "A"
    value["reconciliation"]["baseline_bronze_runs"] = []
    persist(value)
    with pytest.raises(SilverBundleError, match="closure"):
        verify(value, related(base))


def test_legacy_baseline_cannot_get_scoped_authority_without_adoption(tmp_path):
    base, value = baseline_candidate(tmp_path)
    base["manifest"]["schema_version"] = 2
    persist(base)
    binding = value["coverage"]["baseline"]
    binding["manifest_sha256"] = sha256_path(base["path"])
    value["coverage"]["expected_parent"]["manifest_sha256"] = binding["manifest_sha256"]
    persist(value)
    with pytest.raises(SilverBundleError, match="adoption"):
        verify(value, related(base))


def test_self_referential_baseline_fails_before_recursive_lookup(tmp_path):
    _, value = baseline_candidate(tmp_path)
    value["coverage"]["baseline"]["silver_version_id"] = "B"
    value["coverage"]["expected_parent"]["silver_version_id"] = "B"
    persist(value)
    with pytest.raises(SilverBundleError, match="references itself"):
        verify(value)


def test_preview_preserves_scope_drift_diagnostics_but_authoritative_mode_blocks(tmp_path):
    base, value = baseline_candidate(tmp_path)
    value["coverage"]["mode"] = value["manifest"]["population"]["mode"] = "preview"
    value["reconciliation"]["scope_drift"] = [
        {
            "source_contract_id": "CO1.PCCNTR.1",
            "contract_key": KEY,
            "bronze_run_id": "run-2",
            "bronze_page_path": "pages/primary-00001.json",
            "bronze_row_ordinal": 0,
            "signing_date": "2025-01-01",
            "department": "Antioquia",
        }
    ]
    value["manifest"]["population"]["scope_drift"] = True
    persist(value)
    assert verify(value, related(base))[1]["scope_drift"]
    value["coverage"]["mode"] = value["manifest"]["population"]["mode"] = "authoritative_candidate"
    persist(value)
    with pytest.raises(SilverBundleError, match="drift"):
        verify(value, related(base))


def test_rejected_links_must_reference_registered_inputs_and_actual_summary(tmp_path):
    value = fixture(tmp_path)
    summary = value["condition_summary"][0]
    link = {
        key: item
        for key, item in summary.items()
        if key
        not in {
            "state",
            "later_invalid_count",
            "equal_invalid_count",
        }
    }
    link.update(
        {
            key.replace("winner_", "rejected_", 1): item
            for key, item in list(link.items())
            if key.startswith("winner_")
        }
    )
    link.update(
        temporal_relation="EQUAL", rejection_codes=["AMOUNT_NULL"], rejected_run_id="foreign"
    )
    link["link_id"] = condition_link_id(link)
    value["condition_links"] = [link]
    value["manifest"]["counts"].update(input_rows=2, rejected_rows=1)
    value["reconciliation"]["counts"].update(input_rows=2, rejected_rows=1)
    summary.update(state="TEMPORAL_ORDER_INDETERMINATE", equal_invalid_count=1)
    value["manifest"]["population"]["link_rows"] = 1
    persist(value)
    with pytest.raises(SilverBundleError, match="profile/input"):
        verify(value)


def test_verified_legacy_adoption_retains_original_hash_and_full_closure(tmp_path):
    base, value = baseline_candidate(tmp_path)
    original = base["manifest"]
    original.update(schema_version=2, input_purposes={"run-1": "incremental"})
    original.pop("population")
    original.pop("row_schema_version")
    base["path"].write_bytes(canonical_json(original))
    original_hash = sha256_path(base["path"])
    adoption = {
        "schema_version": "population-adoption-v1",
        "silver_version_id": "A",
        "manifest_sha256": original_hash,
        "scope": SCOPE.as_dict(),
        "scope_id": SCOPE.scope_id,
        "keys_sha256": key_set_digest([KEY]),
        "bronze_runs": original["bronze_runs"],
        "coverage_known": False,
        "reason": "LEGACY_UNASSESSED",
    }
    adoption["adoption_id"] = digest_json(adoption)
    record = tmp_path / "population_adoptions" / "A.json"
    record.parent.mkdir()
    record.write_bytes(canonical_json(adoption))
    value["coverage"]["baseline"]["manifest_sha256"] = original_hash
    value["coverage"]["expected_parent"]["manifest_sha256"] = original_hash
    value["manifest"]["input_purposes"]["run-1"] = "incremental"
    persist(value)
    assert verify(value, related(base))[0]["baseline"]["manifest_sha256"] == original_hash
    assert sha256_path(base["path"]) == original_hash
    adoption["bronze_runs"] = []
    adoption["adoption_id"] = digest_json(
        {key: item for key, item in adoption.items() if key != "adoption_id"}
    )
    record.write_bytes(canonical_json(adoption))
    with pytest.raises(SilverBundleError, match="adoption"):
        verify(value, related(base))


def test_original_refresh_nodes_require_recursive_verification_and_exact_run_coverage(tmp_path):
    value = fixture(tmp_path)
    entry = {"run_id": "old-refresh", "manifest_sha256": KEY}
    value["manifest"]["bronze_runs"].append(entry)
    value["coverage"]["bronze_runs"].append(deepcopy(entry))
    value["manifest"]["input_purposes"]["old-refresh"] = "identity_refresh"
    node_path = tmp_path / "versions" / "old-node" / "manifest.json"
    node_path.parent.mkdir()
    node_path.write_bytes(
        canonical_json(
            {
                "bronze_runs": [entry],
                "refresh": {"complete": True, "refresh_runs": [{"run_id": "old-refresh"}]},
            }
        )
    )
    node = {
        "silver_version_id": "old-node",
        "manifest_sha256": sha256_path(node_path),
        "refresh_run_ids": ["old-refresh"],
    }
    value["reconciliation"]["historical_refresh_nodes"] = [node]
    persist(value)
    calls = []

    def original_verifier(path, expected_hash):
        # The supplied verifier owns the complete standalone EN-01 proof.
        assert path == node_path and expected_hash == sha256_path(path)
        calls.append(path)
        return SimpleNamespace()

    assert verify(value, original_verifier)[1]["historical_refresh_nodes"] == [node]
    assert calls == [node_path]
    node["refresh_run_ids"] = []
    persist(value)
    with pytest.raises(SilverBundleError, match="run/dependency"):
        verify(value, original_verifier)
