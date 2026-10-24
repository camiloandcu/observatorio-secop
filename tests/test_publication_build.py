"""Publications require complete fresh dbt evidence, not a process exit code."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from observatorio_secop.publication.build import validate_artifacts

PUBLICATION = "a" * 32


def _evidence(target: Path) -> dict:
    nodes = {
        "model.project.fact_contract": {
            "resource_type": "model",
            "name": "fact_contract",
            "schema": f"candidate_{PUBLICATION}_analytics",
            "checksum": {"checksum": "model-bytes"},
        },
        "test.project.unique_fact_contract": {
            "resource_type": "test",
            "name": "unique_fact_contract",
            "schema": f"candidate_{PUBLICATION}_dbt_test__audit",
            "checksum": {"checksum": "test-bytes"},
        },
    }
    manifest = {
        "metadata": {"invocation_id": "same-invocation"},
        "nodes": nodes,
        "sources": {str(index): {"schema": f"candidate_{PUBLICATION}_input"} for index in range(6)},
    }
    results = {
        "metadata": {"invocation_id": "same-invocation"},
        "results": [
            {"unique_id": key, "status": "pass" if key.startswith("test.") else "success"}
            for key in nodes
        ],
    }
    (target / "manifest.json").write_text(json.dumps(manifest))
    (target / "run_results.json").write_text(json.dumps(results))
    return nodes


def test_required_inventory_and_fresh_invocation_form_sealable_receipt(tmp_path: Path) -> None:
    expected = _evidence(tmp_path)
    receipt = validate_artifacts(tmp_path, PUBLICATION, expected)
    assert receipt["required_nodes"] == sorted(expected)
    assert len(receipt["manifest_sha256"]) == len(receipt["run_results_sha256"]) == 64
    assert receipt["dbt_invocation_id"] == "same-invocation"


@pytest.mark.parametrize("status", ["skipped", "fail", "warn", "error"])
def test_any_unexecuted_or_unsuccessful_required_test_blocks_sealing(
    tmp_path: Path, status: str
) -> None:
    expected = _evidence(tmp_path)
    path = tmp_path / "run_results.json"
    result = json.loads(path.read_text())
    result["results"][1]["status"] = status
    path.write_text(json.dumps(result))
    with pytest.raises(ValueError, match="VALIDATION_FAILED"):
        validate_artifacts(tmp_path, PUBLICATION, expected)


@pytest.mark.parametrize("change", ["missing", "duplicate", "stale"])
def test_missing_duplicate_or_stale_success_cannot_authorize_candidate(
    tmp_path: Path, change: str
) -> None:
    expected = _evidence(tmp_path)
    path = tmp_path / "run_results.json"
    result = json.loads(path.read_text())
    if change == "missing":
        result["results"].pop()
    elif change == "duplicate":
        result["results"].append(result["results"][0])
    else:
        result["metadata"]["invocation_id"] = "another-build"
    path.write_text(json.dumps(result))
    with pytest.raises(ValueError, match="INCOMPLETE_RESULTS|DUPLICATE_RESULT|STALE_RESULTS"):
        validate_artifacts(tmp_path, PUBLICATION, expected)


@pytest.mark.parametrize("escape", ["model", "source"])
def test_complete_success_cannot_reference_global_mutable_relations(
    tmp_path: Path, escape: str
) -> None:
    expected = _evidence(tmp_path)
    path = tmp_path / "manifest.json"
    manifest = json.loads(path.read_text())
    if escape == "model":
        manifest["nodes"]["model.project.fact_contract"]["schema"] = "analytics"
    else:
        manifest["sources"]["0"]["schema"] = "silver"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="NAMESPACE_ESCAPE|SOURCE_ESCAPE"):
        validate_artifacts(tmp_path, PUBLICATION, expected)
