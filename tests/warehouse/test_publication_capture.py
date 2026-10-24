"""The publication capture boundary rejects changed source or verified evidence."""

from __future__ import annotations

import os
from argparse import Namespace
from pathlib import Path

import pytest
from psycopg import sql

from observatorio_secop.publication import capture
from observatorio_secop.publication import cli as publication_cli
from observatorio_secop.publication.build import run_candidate_build
from observatorio_secop.publication.models import candidate_lock_id
from observatorio_secop.publication.readers import read_publication, resolve
from observatorio_secop.publication.store import PublicationStore
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import SilverBundleError
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import identity_row, population_row, write_identity_bundle, write_population_bundle
from .snapshot_helpers import seed_registered
from .test_publication_acceptance import BUDGET, _activate
from .test_publication_acceptance import publication_database as _publication_database


@pytest.fixture
def capture_publication_database(tmp_path):
    yield from _publication_database.__wrapped__(tmp_path)


@pytest.mark.skipif(
    not os.environ.get("WAREHOUSE_TEST_DSN"), reason="Disposable PostgreSQL required"
)
def test_captured_graph_survives_warehouse_a_b_a_and_builds_only_a(capture_publication_database):
    config, store, root = capture_publication_database
    write_population_bundle(
        config.silver_root, "capture-a", [population_row("capture-a", "one", amount="100.00")]
    )
    a = verify_current_bundle(config.silver_root)
    loader = WarehouseLoader(config)
    loader.load(a)
    value = store.reserve({})["publication_id"]
    metadata = capture.capture_inputs(config, a, value, storage_budget_bytes=BUDGET)

    def copied_rows():
        suffixes = (
            "",
            "_population",
            "_population_binding",
            "_observation_condition",
            "_observation_link",
            "_quality",
        )
        with capture._connect(config) as database:
            return {
                suffix: database.execute(
                    sql.SQL("SELECT to_jsonb(r) FROM {} r ORDER BY to_jsonb(r)::text").format(
                        sql.Identifier(f"candidate_{value}_input", "contract_current" + suffix)
                    )
                ).fetchall()
                for suffix in suffixes
            }

    original = copied_rows()
    write_population_bundle(
        config.silver_root,
        "capture-b",
        [
            population_row("capture-b", "one", amount="200.00"),
            population_row("capture-b", "two", contract_id="CO1.PCCNTR.2", amount="50.00"),
        ],
    )
    b = verify_current_bundle(config.silver_root)
    loader.load(b)
    with capture._connect(config) as database:
        assert database.execute(
            "SELECT count(*),sum(contract_value_cop),min(silver_version_id) "
            "FROM silver.contract_current"
        ).fetchone() == (2, 250, b.version_id)
    assert copied_rows() == original
    assert store.inspect(value)["metadata"] == metadata
    receipt = run_candidate_build(
        config,
        value,
        artifact_root=root / "builds",
        timeout_seconds=180,
        storage_budget_bytes=BUDGET,
        metadata=metadata,
    )
    before_restore = store.inspect(value)["metadata"]
    loader.load(a, restore=True, expected_current=(b.version_id, b.manifest_sha256))
    assert copied_rows() == original
    assert store.inspect(value)["metadata"] == before_restore
    store.seal(value, receipt)
    _activate(store, value)
    detail = read_publication(config, resolve(config), "mart_contract_detail")
    assert len(detail) == 1
    assert (detail[0]["contract_value_cop"], detail[0]["silver_version_id"]) == (100, a.version_id)


def test_manifest_changed_after_verification_fails_before_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "silver"
    write_identity_bundle(root, "capture-verified", [identity_row("capture-verified")])
    bundle = verify_current_bundle(root)
    bundle.manifest_path.write_bytes(bundle.manifest_path.read_bytes() + b" ")

    def forbidden(*args, **kwargs):
        pytest.fail("Modified verified evidence must not open PostgreSQL")

    monkeypatch.setattr(capture, "_connect", forbidden)
    config = WarehouseConfig(
        root,
        "silver",
        "contract_current",
        "load_audit",
        7404001,
        "127.0.0.1",
        1,
        "unused",
        "unused",
        "unused",
    )
    with pytest.raises(SilverBundleError, match="differs"):
        capture.capture_inputs(config, bundle, "b" * 32, storage_budget_bytes=100_000_000)


@pytest.mark.skipif(
    not os.environ.get("WAREHOUSE_TEST_DSN"), reason="Disposable PostgreSQL required"
)
def test_row_drift_is_not_hidden_by_same_version_and_manifest(postgres_config) -> None:
    config = postgres_config
    write_identity_bundle(config.silver_root, "capture-drift", [identity_row("capture-drift")])
    bundle = verify_current_bundle(config.silver_root)
    seed_registered(config, bundle)
    WarehouseLoader(config).load(bundle)
    store = PublicationStore(config)
    store.bootstrap()
    row = store.reserve(
        {"silver_version_id": bundle.version_id, "silver_manifest_sha256": bundle.manifest_sha256}
    )
    value = row["publication_id"]
    try:
        with capture._connect(config) as database:
            database.execute(
                sql.SQL("UPDATE {} SET contract_value_cop=contract_value_cop+1").format(
                    sql.Identifier(config.schema, config.contract_table)
                )
            )
        with pytest.raises(ValueError, match="SOURCE_CONTENT_CHANGED"):
            capture.capture_inputs(config, bundle, value, storage_budget_bytes=100_000_000)
        with capture._connect(config) as database:
            assert (
                database.execute(
                    "SELECT 1 FROM pg_namespace WHERE nspname=%s", (f"candidate_{value}_input",)
                ).fetchone()
                is None
            )
    finally:
        store.discard(value)


@pytest.mark.skipif(
    not os.environ.get("WAREHOUSE_TEST_DSN"), reason="Disposable PostgreSQL required"
)
def test_live_candidate_guard_blocks_capture_before_mutation(postgres_config) -> None:
    config = postgres_config
    write_identity_bundle(config.silver_root, "capture-lock", [identity_row("capture-lock")])
    bundle = verify_current_bundle(config.silver_root)
    store = PublicationStore(config)
    store.bootstrap()
    value = store.reserve({})["publication_id"]
    try:
        with capture._connect(config, autocommit=True) as guard:
            assert guard.execute(
                "SELECT pg_try_advisory_lock(%s)", (candidate_lock_id(value),)
            ).fetchone() == (True,)
            with pytest.raises(ValueError, match="PUBLICATION_BUSY"):
                capture.capture_inputs(config, bundle, value, storage_budget_bytes=100_000_000)
    finally:
        store.discard(value)


@pytest.mark.skipif(
    not os.environ.get("WAREHOUSE_TEST_DSN"), reason="Disposable PostgreSQL required"
)
def test_capture_commit_preserves_resume_metadata_without_a_caller_update(
    postgres_config, monkeypatch, tmp_path
) -> None:
    config = postgres_config
    write_identity_bundle(config.silver_root, "capture-crash", [identity_row("capture-crash")])
    bundle = verify_current_bundle(config.silver_root)
    seed_registered(config, bundle)
    WarehouseLoader(config).load(bundle)
    store = PublicationStore(config)
    store.bootstrap()
    value = store.reserve(
        {"silver_version_id": bundle.version_id, "silver_manifest_sha256": bundle.manifest_sha256}
    )["publication_id"]
    try:
        # Simulate losing the process immediately after capture commits: the CLI's
        # separate update_metadata call never runs.
        captured = capture.capture_inputs(config, bundle, value, storage_budget_bytes=100_000_000)
        durable = store.inspect(value)["metadata"]
        assert all(durable[key] == expected for key, expected in captured.items())
        with capture._connect(config) as database:
            assert database.execute(
                sql.SQL("SELECT count(*) FROM {}.contract_current").format(
                    sql.Identifier(f"candidate_{value}_input")
                )
            ).fetchone() == (bundle.row_count,)

        def forbidden_recapture(*args, **kwargs):
            pytest.fail("Pinned retry must resume committed inputs instead of recapturing")

        reached_build = []

        def stop_at_build(*args, **kwargs):
            reached_build.append(kwargs["metadata"])
            raise ValueError("TEST_STOP_BEFORE_DBT")

        monkeypatch.setattr(publication_cli, "capture_inputs", forbidden_recapture)
        monkeypatch.setattr(publication_cli, "run_candidate_build", stop_at_build)
        arguments = Namespace(
            command="retry",
            publication_id=value,
            timeout_seconds=60,
            storage_budget_bytes=100_000_000,
            artifact_root=tmp_path / "artifacts",
        )
        with pytest.raises(ValueError, match="TEST_STOP_BEFORE_DBT"):
            publication_cli._build(arguments, store)
        assert reached_build == [durable]
        assert store.inspect(value)["metadata"] == durable
    finally:
        store.discard(value)
