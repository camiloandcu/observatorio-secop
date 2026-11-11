"""Historical active retry and real identity-refresh across calendar policies.

Synthetic temporary fixtures only; no PostgreSQL or live SECOP calls. Coordinate
the Spark slot before executing these tests alongside another pipeline.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from conftest import ROOT, row, write_bronze_run

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.refresh import IdentityRefresh
from observatorio_secop.ingestion.transport import SocrataTransport
from observatorio_secop.processing.silver.bronze import resolve_bronze_runs
from observatorio_secop.processing.silver.errors import PublicationError
from processing_silver.test_calendar_policy_integration import (
    bundle_hashes,
    configured_pipeline,
    forbid_spark,
)
from processing_silver.test_identity_pipeline import Response, historical_snapshot, observed


def resolved_runs(pipeline, identifiers):
    return resolve_bronze_runs(
        pipeline.config.bronze_root,
        dataset_id=pipeline.config.dataset_id,
        contract_sha256=pipeline.contract.contract_hash,
        run_ids=identifiers,
        registry=pipeline.registry,
    )


def test_active_legacy_v1_retry_is_exact_and_not_reused_by_v2(
    silver_config, spark, tmp_path, monkeypatch
):
    write_bronze_run(silver_config, "legacy-policy-input", [row("A")], legacy=True)
    old = configured_pipeline(silver_config, "silver-v1", tmp_path, spark)
    runs = resolved_runs(old, ("legacy-policy-input",))
    legacy_id = old._version_id(runs)
    preview_id = old._version_id(runs, selection_mode="preview")
    assert legacy_id != preview_id
    # Reconstruct a historical pre-preview fixture with real Spark/writer bytes.
    # Only fixture assembly chooses the historical ID; subsequent retries run the
    # unmodified identity computation and must reach the actual legacy branch.
    with monkeypatch.context() as historical_writer:
        historical_writer.setattr(old, "_version_id", lambda *_args, **_kwargs: legacy_id)
        manifest = old.run(("legacy-policy-input",))
    assert manifest.status == "valid" and manifest.transformation_version == "silver-v1"
    baseline = historical_snapshot(old, manifest)
    assert baseline.version_id == old._version_id(runs) == legacy_id
    pointer = old.storage.current_path.read_bytes()
    retained = bundle_hashes(baseline.manifest_path.parent)
    bronze = bundle_hashes(silver_config.bronze_root)
    with monkeypatch.context() as retry:
        retry.setattr(old, "_spark", forbid_spark)
        result = old.run(("legacy-policy-input",))
    assert result.silver_version_id == legacy_id
    assert result.transformation_version == "silver-v1"
    assert result.selection_mode is None
    assert old.storage.current_path.read_bytes() == pointer
    assert bundle_hashes(baseline.manifest_path.parent) == retained

    current = configured_pipeline(silver_config, "silver-v2-calendar-date", tmp_path, spark)
    assert current._version_id(runs) != legacy_id

    class NewPolicyBuildRequired(Exception):
        pass

    def stop_before_new_spark():
        raise NewPolicyBuildRequired

    # Deliberately stop at the new build boundary: v2 must not return the active
    # v1 bundle through either the legacy retry or preview cache shortcut.
    with monkeypatch.context() as migration:
        migration.setattr(current, "_spark", stop_before_new_spark)
        with pytest.raises(NewPolicyBuildRequired):
            current.run(("legacy-policy-input",))
    assert old.storage.current_path.read_bytes() == pointer
    assert bundle_hashes(baseline.manifest_path.parent) == retained
    assert bundle_hashes(silver_config.bronze_root) == bronze


@pytest.mark.parametrize("expanded_eligibility", [False, True])
def test_identity_refresh_across_policies_preserves_exact_key_gate(
    silver_config, spark, tmp_path, monkeypatch, expanded_eligibility
):
    # One previously rejected row among 51 is below the unchanged 2% threshold.
    # No quality exception is introduced to manufacture a valid v1 baseline.
    originals = [row("A")]
    if expanded_eligibility:
        originals += [row(f"MIDNIGHT-{index:02d}") for index in range(49)]
        originals.append(row("NEWLY-VALID", signing="2026-01-01T16:53:42.594"))
    write_bronze_run(silver_config, "retained-input", originals, legacy=True)
    old = configured_pipeline(silver_config, "silver-v1", tmp_path, spark)
    manifest = old.run(("retained-input",))
    assert manifest.status == "valid" and manifest.critical_passed
    assert manifest.counts["rejected_rows"] == int(expanded_eligibility)
    assert old.config.max_rejection_rate == 0.02
    baseline = historical_snapshot(old, manifest)
    pointer = old.storage.current_path.read_bytes()
    retained = bundle_hashes(baseline.manifest_path.parent)
    original_bronze = bundle_hashes(silver_config.bronze_root / "runs" / "retained-input")

    ingestion = replace(
        load_ingestion_config(ROOT / "config/bronze_ingestion.yaml"),
        bronze_root=silver_config.bronze_root,
        page_size=2,
        max_rows=10,
        contract_path=ROOT / "contracts/secop_source.yaml",
    )
    contract = load_bronze_contract(ingestion.contract_path, ingestion.projection)
    metadata = {
        "id": contract.dataset_id,
        "columns": [
            {"fieldName": name, "dataTypeName": value["declared_type"]}
            for name, value in contract.columns.items()
        ],
    }
    replies = iter([metadata, [observed("A", "001", "100")]])
    transport = SocrataTransport(
        ingestion,
        contract,
        opener=lambda *_args, **_kwargs: Response(next(replies)),
        sleeper=lambda _: None,
    )
    refresher = IdentityRefresh(
        ingestion, contract, transport=transport, uuid_factory=lambda: "policy-identity-refresh"
    )
    with refresher.state.connect() as database:
        database.execute(
            "INSERT INTO checkpoints VALUES (?, 'primary', ?, 'unchanged')",
            (contract.dataset_id, "2026-01-01T00:00:00Z"),
        )
    checkpoint = refresher.state.checkpoint(contract.dataset_id)
    refresh = refresher.refresh(baseline.manifest_path, ["A"])
    assert refresh.range_complete and not refresh.truncated
    assert refresher.state.checkpoint(contract.dataset_id) == checkpoint
    refresh_bronze = bundle_hashes(silver_config.bronze_root / "runs" / refresh.run_id)
    current = configured_pipeline(silver_config, "silver-v2-calendar-date", tmp_path, spark)
    selected = ("retained-input", refresh.run_id)
    with pytest.raises(PublicationError, match="baseline Bronze dependencies"):
        current.run((refresh.run_id,))
    if expanded_eligibility:
        with pytest.raises(PublicationError, match="changes the baseline contractual key set"):
            current.run(selected)
        identifier = current._version_id(resolved_runs(current, selected), selection_mode="preview")
        assert not current.storage.version_path(identifier).exists()
        assert not current.storage.staging_path(identifier).exists()
    else:
        candidate = current.run(selected)
        assert candidate.status == "valid" and candidate.critical_passed
        assert candidate.transformation_version == "silver-v2-calendar-date"
        assert candidate.silver_version_id != baseline.version_id
        assert candidate.selection_mode == "preview"
        assert candidate.refresh["business_changed_contracts"] == 0
        assert candidate.refresh["entity_identity_completed"] == 1
        assert candidate.refresh["supplier_identity_completed"] == 1
        assert (
            candidate.refresh["baseline_keys_sha256"] == candidate.refresh["candidate_keys_sha256"]
        )
        with monkeypatch.context() as retry:
            retry.setattr(current, "_spark", forbid_spark)
            assert current.run(selected).as_dict() == candidate.as_dict()
    assert old.storage.current_path.read_bytes() == pointer
    assert bundle_hashes(baseline.manifest_path.parent) == retained
    assert bundle_hashes(silver_config.bronze_root / "runs" / "retained-input") == original_bronze
    assert bundle_hashes(silver_config.bronze_root / "runs" / refresh.run_id) == refresh_bronze
    assert refresher.state.checkpoint(contract.dataset_id) == checkpoint
