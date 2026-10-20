from __future__ import annotations

import json

import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.errors import SilverBundleError

from .conftest import (
    identity_row,
    rewrite_manifest,
    silver_row,
    write_bundle,
    write_identity_bundle,
)
from .inventory_helpers import fingerprint


def test_old_and_new_schema_adapters_preserve_immutable_bytes(tmp_path) -> None:
    for int96 in (False, True):
        root = tmp_path / str(int96)
        write_bundle(
            root,
            "legacy",
            [silver_row("legacy")],
            parquet_options={"use_deprecated_int96_timestamps": int96},
        )
        before = fingerprint(root)
        old = verify_current_bundle(root)
        row = old.table.to_pylist()[0]
        assert (
            row["entity_identity_reason"] == row["supplier_identity_reason"] == "LEGACY_PROJECTION"
        )
        assert row["entity_code"] is row["supplier_code"] is None
        assert fingerprint(root) == before
        write_identity_bundle(root, "current", [identity_row("current", entity_name=None)])
        before = fingerprint(root)
        new = verify_current_bundle(root).table.to_pylist()[0]
        assert new["entity_code"] == "00100"
        assert new["entity_identity_status"] == "KNOWN" and new["entity_name"] is None
        assert fingerprint(root) == before


@pytest.mark.parametrize(
    "mutation",
    [
        {"schema_version": 3},
        {"schema_version": True},
        {"schema_version": 1},
        {"identity_rule_version": "made-up"},
        {"compatibility_sha256": "0" * 64},
        {"input_purposes": {"run-1": "identity_refresh"}},
    ],
)
def test_schema_and_profile_mismatch_is_rejected(tmp_path, mutation) -> None:
    manifest = write_identity_bundle(tmp_path, "current", [identity_row("current")])
    rewrite_manifest(manifest, lambda value: value.update(mutation))
    with pytest.raises(SilverBundleError):
        verify_current_bundle(tmp_path)


@pytest.mark.parametrize(
    "updates",
    [
        {"entity_code": "100"},
        {"supplier_identity_status": "UNKNOWN"},
        {"source_contract_sha256": "0" * 64},
        {"entity_identity_reason": "MISSING_CODE"},
        {"source_dataset_id": "other-dataset"},
        {"bronze_run_id": "unregistered-run"},
    ],
)
def test_identity_state_or_source_provenance_cannot_be_silently_coerced(tmp_path, updates) -> None:
    row = identity_row("current")
    row.update(updates)
    write_identity_bundle(tmp_path, "current", [row])
    with pytest.raises(SilverBundleError):
        verify_current_bundle(tmp_path)


def test_same_count_different_baseline_keys_is_not_a_valid_refresh(tmp_path) -> None:
    old_manifest = write_identity_bundle(tmp_path, "base", [identity_row("base")])
    old_pointer = json.loads((tmp_path / "current.json").read_text())
    candidate = write_identity_bundle(tmp_path, "candidate", [identity_row("candidate", "other")])
    baseline = json.loads(old_manifest.read_text())
    rewrite_manifest(
        candidate,
        lambda value: value.update(
            refresh={
                "complete": True,
                "baseline_version_id": "base",
                "baseline_manifest_sha256": old_pointer["manifest_sha256"],
                "baseline_bronze_runs": baseline["bronze_runs"],
                "baseline_keys_sha256": "fabricated",
                "candidate_keys_sha256": "fabricated",
            },
        ),
    )
    with pytest.raises(SilverBundleError, match="key-set"):
        verify_current_bundle(tmp_path)
