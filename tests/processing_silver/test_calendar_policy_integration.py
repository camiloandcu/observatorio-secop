"""Policy-specific identities and verified cache paths across Silver entry points.

Spark-backed cases must run after the coordinator releases the heavy-suite slot.
They use synthetic temporary Bronze and require no database or external service.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from conftest import row, write_bronze_run

import observatorio_secop.processing.silver.runner as runner
from observatorio_secop.processing.silver.config import load_silver_config
from observatorio_secop.processing.silver.errors import SilverConfigurationError
from observatorio_secop.processing.silver.runner import SilverPipeline
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from processing_silver.test_population_pipeline import SCOPE, acquire

POLICIES = ("silver-v1", "silver-v2-calendar-date")


def configured_pipeline(config, policy, directory, spark):
    """Persist the exact configuration being hashed, rather than a different default."""
    configured = replace(config, transformation_version=policy)
    document = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in asdict(configured).items()
    }
    path = directory / f"{policy}.yaml"
    path.write_text(yaml.safe_dump({"silver": document}), encoding="utf-8")
    loaded = load_silver_config(path, environment={})
    return SilverPipeline(loaded, config_path=path, spark=spark)


def bundle_hashes(path):
    return {
        str(file.relative_to(path)): hashlib.sha256(file.read_bytes()).hexdigest()
        for file in sorted(path.rglob("*"))
        if file.is_file()
    }


def forbid_spark(*_args, **_kwargs):
    pytest.fail("A verified policy-specific retry unexpectedly requested Spark")


def forbid_pointer(*_args, **_kwargs):
    pytest.fail("A preview retry attempted authoritative pointer mutation")


def test_standalone_policy_versions_reuse_only_their_verified_preview(
    silver_config, spark, tmp_path, monkeypatch
):
    # Midnight inputs are valid under both policies: different IDs must therefore
    # come from the declared semantics/configuration, not differing input bytes.
    bronze = write_bronze_run(silver_config, "policy-input", [row("A"), row("B")])
    original_bronze = bundle_hashes(bronze)
    pipelines, manifests, saved = [], [], {}
    for policy in POLICIES:
        pipeline = configured_pipeline(silver_config, policy, tmp_path, spark)
        with monkeypatch.context() as guard:
            guard.setattr(pipeline.storage, "_write_current", forbid_pointer)
            manifest = pipeline.run(("policy-input",))
        assert manifest.status == "valid" and manifest.critical_passed
        assert manifest.transformation_version == policy
        assert manifest.selection_mode == "preview"
        assert manifest.counts["output_rows"] == 2
        path = pipeline.storage.version_path(manifest.silver_version_id)
        verified = verify_manifest_bundle(path / "manifest.json")
        assert verified.version_id == manifest.silver_version_id
        saved[manifest.silver_version_id] = bundle_hashes(path)
        pipelines.append(pipeline)
        manifests.append(manifest)
    assert manifests[0].silver_version_id != manifests[1].silver_version_id
    assert manifests[0].config_sha256 != manifests[1].config_sha256
    for pipeline, manifest in zip(pipelines, manifests, strict=True):
        with monkeypatch.context() as guard:
            guard.setattr(pipeline, "_spark", forbid_spark)
            guard.setattr(pipeline.storage, "_write_current", forbid_pointer)
            retried = pipeline.run(("policy-input",))
        assert retried.as_dict() == manifest.as_dict()
        assert (
            bundle_hashes(pipeline.storage.version_path(manifest.silver_version_id))
            == saved[manifest.silver_version_id]
        )
    assert not (silver_config.silver_root / "current.json").exists()
    assert bundle_hashes(bronze) == original_bronze


def test_scoped_policies_keep_separate_preview_caches_and_authoritative_retry(
    silver_config, spark, tmp_path, monkeypatch
):
    # Reuse the governed acquisition fixture, including its unchanged checkpoint
    # assertion. Its mocked source is local; no PostgreSQL/dbt helpers are called.
    campaign = acquire(silver_config, "calendar-policy-campaign", [[row("A", updated=None)]])
    bronze_before = bundle_hashes(silver_config.bronze_root)
    pipelines, previews, saved = [], [], {}
    for policy in POLICIES:
        pipeline = configured_pipeline(silver_config, policy, tmp_path, spark)
        with monkeypatch.context() as guard:
            guard.setattr(pipeline.storage, "_write_current", forbid_pointer)
            preview = pipeline.run_population(SCOPE, (campaign.run_id,), mode="preview")
        path = pipeline.storage.version_path(preview.silver_version_id)
        bundle = verify_manifest_bundle(path / "manifest.json")
        assert preview.status == "valid" and preview.transformation_version == policy
        assert bundle.coverage["mode"] == "preview"
        assert bundle.row_count == 1
        saved[preview.silver_version_id] = bundle_hashes(path)
        pipelines.append(pipeline)
        previews.append(preview)
    assert previews[0].silver_version_id != previews[1].silver_version_id
    assert not (silver_config.silver_root / "current.json").exists()
    current = pipelines[1]
    authoritative = current.run_population(
        SCOPE, (campaign.run_id,), mode="authoritative_candidate"
    )
    assert authoritative.transformation_version == POLICIES[1]
    assert authoritative.silver_version_id not in {p.silver_version_id for p in previews}
    pointer = current.storage.current_path.read_bytes()
    authoritative_path = current.storage.version_path(authoritative.silver_version_id)
    authoritative_hashes = bundle_hashes(authoritative_path)
    # Preview retries remain non-authoritative even with another policy selected.
    for pipeline, preview in zip(pipelines, previews, strict=True):
        with monkeypatch.context() as guard:
            guard.setattr(pipeline, "_spark", forbid_spark)
            guard.setattr(pipeline.storage, "rollback", forbid_pointer)
            guard.setattr(pipeline.storage, "_write_current", forbid_pointer)
            retried = pipeline.run_population(SCOPE, (campaign.run_id,), mode="preview")
        assert retried.as_dict() == preview.as_dict()
        assert (
            bundle_hashes(pipeline.storage.version_path(preview.silver_version_id))
            == saved[preview.silver_version_id]
        )
        assert current.storage.current_path.read_bytes() == pointer
    with monkeypatch.context() as guard:
        guard.setattr(current, "_spark", forbid_spark)
        retried = current.run_population(SCOPE, (campaign.run_id,), mode="authoritative_candidate")
    assert retried.as_dict() == authoritative.as_dict()
    assert current.storage.current_path.read_bytes() == pointer
    assert bundle_hashes(authoritative_path) == authoritative_hashes
    assert bundle_hashes(silver_config.bronze_root) == bronze_before


def test_unknown_policy_fails_before_pipeline_sources_storage_or_cache(
    silver_config, tmp_path, monkeypatch
):
    # Populate a cache-shaped directory so early rejection cannot accidentally
    # pass only because a workspace has no existing selection to discover.
    root = tmp_path / "retained"
    root.mkdir()
    (root / "current.json").write_text('{"silver_version_id":"retained"}\n')
    before = bundle_hashes(root)
    unknown = SimpleNamespace(
        transformation_version="silver-unsupported",
        silver_root=root,
        territory_catalog_path=silver_config.territory_catalog_path,
    )

    def forbidden(*_args, **_kwargs):
        pytest.fail("Unknown policy reached source/catalog/storage/cache access")

    monkeypatch.setattr(runner, "load_territory_catalog", forbidden)
    monkeypatch.setattr(runner, "load_bronze_contract", forbidden)
    monkeypatch.setattr(runner, "SilverStorage", forbidden)
    with pytest.raises(SilverConfigurationError, match="Unsupported Silver transformation_version"):
        SilverPipeline(unknown, config_path=tmp_path / "nonexistent.yaml")
    assert bundle_hashes(root) == before
