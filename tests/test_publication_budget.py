"""A build deadline includes post-dbt SQL and streamed validation work."""

from __future__ import annotations

import json
import subprocess
from contextlib import contextmanager
from pathlib import Path

import psycopg
import pytest

from observatorio_secop.publication import build
from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.store import PublicationStore
from observatorio_secop.warehouse.config import WarehouseConfig


class Cursor:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def execute(self, query, parameters=None):
        self.calls.append((query, parameters))

    def fetchone(self):
        return (0,)

    def fetchmany(self, size):
        return []


def test_each_validation_query_receives_only_the_remaining_budget(monkeypatch):
    clock = [1.0]
    monkeypatch.setattr(build.time, "monotonic", lambda: clock[0])
    raw = Cursor()
    cursor = build._BudgetCursor(raw, 10)
    cursor.execute("first validation")
    clock[0] = 8
    cursor.execute("second validation")
    assert [parameters for _, parameters in raw.calls if parameters] == [("9000ms",), ("2000ms",)]
    before = list(raw.calls)
    clock[0] = 10
    with pytest.raises(PublicationError, match="budget expired"):
        cursor.execute("must not run after expiry")
    assert raw.calls == before
    with pytest.raises(PublicationError, match="budget expired"):
        cursor.fetchmany(1000)


def test_server_validation_cancellation_has_an_actionable_timeout_code(monkeypatch):
    monkeypatch.setattr(build.time, "monotonic", lambda: 1)
    raw = Cursor()
    original = raw.execute

    def execute(query, parameters=None):
        if parameters is None:
            raise psycopg.errors.QueryCanceled("statement timeout")
        original(query, parameters)

    monkeypatch.setattr(raw, "execute", execute)
    with pytest.raises(PublicationError) as failure:
        build._BudgetCursor(raw, 10).execute("slow validation")
    assert failure.value.code == "PUBLICATION_BUILD_TIMEOUT"


def test_successful_dbt_cannot_return_a_receipt_after_postbuild_budget_expires(
    tmp_path: Path, monkeypatch
):
    clock = [0.0]
    monkeypatch.setattr(build.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(build, "reviewed_metadata", lambda root: {})
    monkeypatch.setattr(build, "_prepare_schemas", lambda *args, **kwargs: None)
    monkeypatch.setattr(build, "_write_profile", lambda *args: None)
    monkeypatch.setattr(build.shutil, "which", lambda name: "dbt")
    monkeypatch.setattr(PublicationStore, "register_expected_nodes", lambda *args: None)
    monkeypatch.setattr(
        PublicationStore, "update_metadata", lambda self, value, metadata: {"metadata": metadata}
    )
    models = tmp_path / "dbt/models"
    models.mkdir(parents=True)
    (models / "fact_contract.sql").write_text("select 1")
    nodes = {"model.test.fact_contract": {"name": "fact_contract", "resource_type": "model"}}
    postgres_options = []

    def dbt(arguments, **kwargs):
        target = Path(arguments[arguments.index("--target-path") + 1])
        target.mkdir(parents=True, exist_ok=True)
        (target / "manifest.json").write_text(json.dumps({"nodes": nodes}))
        postgres_options.append(kwargs["env"]["PGOPTIONS"])
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(build.subprocess, "run", dbt)
    monkeypatch.setattr(build, "validate_artifacts", lambda *args: {"version": 1})
    monkeypatch.setattr(build, "_reconcile", lambda *args: None)

    class Database:
        def cursor(self):
            return Cursor()

    @contextmanager
    def connect(*args):
        yield Database()

    monkeypatch.setattr(build, "_connect", connect)
    import observatorio_secop.publication.inventory as inventory

    def slow_inventory(*args):
        clock[0] = 11
        return {}

    monkeypatch.setattr(inventory, "relation_inventory", slow_inventory)
    config = WarehouseConfig(
        tmp_path, "silver", "contract_current", "audit", 1, "host", 1, "db", "operator", "password"
    )
    with pytest.raises(PublicationError) as failure:
        build._run_candidate_build(
            config,
            "a" * 32,
            artifact_root=tmp_path / "artifacts",
            timeout_seconds=10,
            storage_budget_bytes=1_000_000,
            project_root=tmp_path,
        )
    assert failure.value.code == "PUBLICATION_BUILD_TIMEOUT"
    assert (
        postgres_options
        == ["-c role=secop_publication_builder -c statement_timeout=10000 -c lock_timeout=5000"] * 2
    )
