"""Quality observation accounting and privacy at the verified input boundary."""

import json

import pytest

from observatorio_secop.warehouse.errors import SilverBundleError
from observatorio_secop.warehouse.quality import verify_quality_summary


def quality_input(tmp_path):
    results = [
        {
            "check_id": "COUNT_RECONCILIATION",
            "severity": "critical",
            "passed": True,
            "observed": 6,
            "expected": 6,
            "affected_rows": 0,
        },
        {
            "check_id": "REJECTION_RATE",
            "severity": "critical",
            "passed": True,
            "observed": {"input_rows": 6, "rejected_rows": 1, "private": "rejected payload"},
            "expected": {"maximum": 1.0},
            "affected_rows": 1,
        },
    ]
    path = tmp_path / "quality/results.json"
    path.parent.mkdir()
    path.write_text(json.dumps(results))
    return {
        "files": [{"path": "quality/results.json"}],
        "writer_format_version": "silver-population-v3",
        "counts": {
            "input_rows": 6,
            "output_rows": 2,
            "rejected_rows": 1,
            "exact_duplicates": 1,
            "superseded_versions": 2,
        },
        "quality_results": results,
        "bronze_runs": [
            {"run_id": "run-1", "manifest_sha256": "a" * 64, "path": "/private/bronze"}
        ],
        "finished_at_utc": "2026-10-22T00:00:00+00:00",
    }


def test_observations_are_not_unique_contracts_and_payload_is_sanitized(tmp_path):
    summary = verify_quality_summary(quality_input(tmp_path), tmp_path)
    assert summary["counts"] == {
        "input_observations": 6,
        "output_contracts": 2,
        "rejected_observations": 1,
        "exact_duplicates": 1,
        "superseded_valid_observations": 2,
        "accepted_valid_observations": 5,
    }
    rejection = summary["controls"][1]
    assert rejection["denominator"] == 6
    assert rejection["denominator_grain"] == "observations"
    assert "private" not in json.dumps(summary)
    assert "rejected payload" not in json.dumps(summary)
    assert len(summary["quality_results_sha256"]) == 64


@pytest.mark.parametrize(
    "case",
    [
        "missing_file",
        "missing_counts",
        "arithmetic",
        "bool",
        "different_results",
        "denominator",
        "corrupt",
        "unknown_writer",
        "partial_legacy",
    ],
)
def test_current_quality_fails_closed(tmp_path, case):
    manifest = quality_input(tmp_path)
    if case == "missing_file":
        manifest["files"] = []
    elif case == "missing_counts":
        del manifest["counts"]["superseded_versions"]
    elif case == "arithmetic":
        manifest["counts"]["input_rows"] = 7
    elif case == "bool":
        manifest["counts"]["exact_duplicates"] = True
    elif case == "different_results":
        manifest["quality_results"] = []
    elif case == "denominator":
        manifest["quality_results"][1]["observed"]["input_rows"] = 7
        (tmp_path / "quality/results.json").write_text(json.dumps(manifest["quality_results"]))
    elif case == "unknown_writer":
        manifest["writer_format_version"] = "silver-unknown-v99"
    elif case == "partial_legacy":
        manifest.pop("writer_format_version")
        del manifest["counts"]["superseded_versions"]
    elif case == "corrupt":
        (tmp_path / "quality/results.json").write_text("{")
    with pytest.raises(SilverBundleError):
        verify_quality_summary(manifest, tmp_path)


def test_legacy_absent_evidence_is_not_zero(tmp_path):
    summary = verify_quality_summary(
        {"files": [], "counts": {"output_rows": 2}, "finished_at_utc": "2026-10-22T00:00:00+00:00"},
        tmp_path,
    )
    assert summary["state"] == "UNAVAILABLE_LEGACY"
    assert summary["counts"] is None
    assert summary["quality_results_sha256"] is None
