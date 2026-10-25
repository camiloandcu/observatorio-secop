"""Denied root/database combinations and explicit bounded live adoption."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from observatorio_secop.orchestration import binding
from observatorio_secop.orchestration.models import PipelineError, Runtime
from observatorio_secop.population import PopulationScope
from observatorio_secop.warehouse.config import WarehouseConfig


@pytest.fixture
def runtime(tmp_path):
    return Runtime(
        tmp_path / "project",
        tmp_path / "state",
        tmp_path / "bronze",
        tmp_path / "silver",
        tmp_path / "reports",
        "demo",
    )


def test_unconfigured_mode_fails_before_creating_roots(runtime):
    with pytest.raises(PipelineError, match="INVALID_MODE"):
        binding.root_binding(replace(runtime, mode="unconfigured"))
    assert not runtime.state.exists()


def test_root_mode_is_durable_and_mode_change_cannot_write(runtime):
    first = binding.root_binding(runtime)
    assert binding.root_binding(runtime) == first
    with pytest.raises(PipelineError, match="MODE_MISMATCH"):
        binding.root_binding(replace(runtime, mode="live"))
    assert binding.root_binding(runtime) == first


def test_reused_root_with_different_other_roots_is_denied(runtime, tmp_path):
    binding.root_binding(runtime)
    with pytest.raises(PipelineError, match="BINDING_CONFLICT"):
        binding.root_binding(replace(runtime, silver=tmp_path / "other-silver"))


def test_partial_marker_crash_reuses_reserved_identity(runtime, monkeypatch):
    original = binding.durable_write
    written = []

    def interrupted(path, content):
        if written:
            raise OSError("synthetic interruption")
        original(path, content)
        written.append(json.loads(content))

    monkeypatch.setattr(binding, "durable_write", interrupted)
    with pytest.raises(OSError):
        binding.root_binding(runtime)
    monkeypatch.setattr(binding, "durable_write", original)
    assert binding.root_binding(runtime) == written[0]
    assert len(list(runtime.state.parent.rglob(".pipeline-binding.json"))) == 4


def test_shared_root_locks_prevent_concurrent_binding(runtime):
    with binding._locked_roots(runtime), pytest.raises(PipelineError, match="TRANSIENT"):
        binding.root_binding(runtime)


@pytest.mark.parametrize("kind", ["bronze", "silver", "current"])
def test_demo_cannot_bless_existing_unbound_local_data(runtime, kind):
    path = {
        "bronze": runtime.bronze / "runs" / "old" / "manifest.json",
        "silver": runtime.silver / "versions" / "old" / "manifest.json",
        "current": runtime.silver / "current.json",
    }[kind]
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    with pytest.raises(PipelineError, match="UNBOUND_AUTHORITATIVE"):
        binding.root_binding(runtime)
    assert not list(runtime.state.parent.rglob(".pipeline-binding.json"))


def test_conflicting_or_malformed_markers_fail_closed(runtime):
    first = binding.root_binding(runtime)
    marker = runtime.silver / ".pipeline-binding.json"
    marker.write_text(json.dumps(first | {"mode": "live"}))
    with pytest.raises(PipelineError, match="BINDING_CONFLICT"):
        binding.root_binding(runtime)
    marker.write_text("[]")
    with pytest.raises(PipelineError, match="BINDING_CONFLICT"):
        binding.root_binding(runtime)


def test_binding_marker_symlink_is_denied(runtime, tmp_path):
    runtime.state.mkdir()
    external = tmp_path / "external.json"
    external.write_text("{}")
    (runtime.state / ".pipeline-binding.json").symlink_to(external)
    with pytest.raises(PipelineError, match="UNSAFE_PATH"):
        binding.root_binding(runtime)
    assert external.read_text() == "{}"


class Cursor:
    def __init__(self):
        self.binding = None
        self.publications = 0
        self.raw = False
        self.active = None
        self.scope = None
        self.publication_scopes = []
        self.result = None

    def execute(self, query, values=None):
        text = query if isinstance(query, str) else query.as_string()
        if text.startswith("SELECT binding"):
            self.result = (self.binding,) if self.binding else None
        elif text.startswith("SELECT count"):
            self.result = (self.publications,)
        elif text.startswith("SELECT to_regclass"):
            self.result = ("silver.contract_current" if self.raw else None,)
        elif text.startswith("SELECT silver_version_id"):
            self.result = self.active
        elif text.startswith("SELECT scope_id"):
            self.result = self.scope
        elif text.startswith("SELECT metadata"):
            self.result = self.publication_scopes
        elif text.startswith("INSERT INTO publication_control.pipeline_runtime"):
            self.binding = values[0].obj

    def fetchone(self):
        return self.result

    def fetchall(self):
        return self.result


@pytest.fixture
def database(runtime, monkeypatch):
    cursor = Cursor()
    config = WarehouseConfig(
        runtime.silver,
        "silver",
        "contract_current",
        "load_audit",
        7404001,
        "test",
        5432,
        "disposable",
        "operator",
        "unused",
    )

    class Store:
        def __init__(self, _config):
            pass

        @contextmanager
        def transaction(self):
            yield cursor

        @staticmethod
        def _lock(_cursor):
            pass

    monkeypatch.setattr(binding, "PublicationStore", Store)
    monkeypatch.setattr(binding, "warehouse_config", lambda _: config)
    return cursor


def test_new_root_workspace_cannot_reuse_bound_database(runtime, database, tmp_path):
    binding.bind(runtime, {"parameters": {}})
    other = replace(
        runtime,
        state=tmp_path / "other-state",
        bronze=tmp_path / "other-bronze",
        silver=tmp_path / "other-silver",
        reports=tmp_path / "other-reports",
    )
    with pytest.raises(PipelineError, match="BINDING_CONFLICT"):
        binding.bind(other, {"parameters": {}})
    binding.bind(runtime, {"parameters": {}})


@pytest.mark.parametrize("kind", ["raw", "publication"])
def test_unbound_demo_database_with_authority_is_denied(runtime, database, kind):
    database.raw = kind == "raw"
    database.publications = int(kind == "publication")
    with pytest.raises(PipelineError, match="UNBOUND_AUTHORITATIVE"):
        binding.bind(runtime, {"parameters": {}})
    assert database.binding is None


def live_baseline(runtime, database, monkeypatch):
    from observatorio_secop.processing.silver.storage import SilverStorage
    from observatorio_secop.warehouse import bundle

    scope = PopulationScope("2026-01-01", "2026-02-01")
    p = {
        "baseline_version": "a" * 64,
        "baseline_sha256": "b" * 64,
        "signing_from": scope.signing_from,
        "signing_to": scope.signing_to,
    }
    monkeypatch.setattr(
        SilverStorage,
        "current",
        lambda _: {
            "silver_version_id": p["baseline_version"],
            "manifest_sha256": p["baseline_sha256"],
        },
    )
    coverage = {
        "mode": "authoritative_candidate",
        "state": "complete",
        "scope_id": scope.scope_id,
        "scope": scope.as_dict(),
    }
    monkeypatch.setattr(
        bundle,
        "verify_manifest_bundle",
        lambda *_args, **_kwargs: SimpleNamespace(coverage=coverage),
    )
    database.raw = True
    database.publications = 1
    database.active = (p["baseline_version"], p["baseline_sha256"])
    database.scope = (scope.scope_id, scope.as_dict())
    database.publication_scopes = [(scope.scope_id,)]
    return replace(runtime, mode="live"), {"parameters": p}, coverage


def test_live_adoption_requires_exact_silver_raw_and_scope(runtime, database, monkeypatch):
    live, document, _ = live_baseline(runtime, database, monkeypatch)
    binding.bind(live, document)
    assert database.binding["mode"] == "live"
    assert document["live_adoption"]["scope_id"] == database.scope[0]


@pytest.mark.parametrize("mismatch", ["raw", "scope", "publication_scope", "coverage"])
def test_live_adoption_conflicts_are_denied(runtime, database, monkeypatch, mismatch):
    live, document, coverage = live_baseline(runtime, database, monkeypatch)
    if mismatch == "raw":
        database.active = ("c" * 64, "d" * 64)
    elif mismatch == "scope":
        database.scope = ("other", {})
    elif mismatch == "publication_scope":
        database.publication_scopes = [("other",)]
    else:
        coverage["state"] = "partial"
    with pytest.raises(PipelineError, match="UNBOUND_AUTHORITATIVE"):
        binding.bind(live, document)
    assert database.binding is None
