"""Durable, serialized demo/live binding before any synthetic acquisition."""

from __future__ import annotations

import fcntl
import json
import os
import re
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

from psycopg import sql
from psycopg.types.json import Jsonb

from observatorio_secop.ingestion.storage import canonical_json, durable_write
from observatorio_secop.orchestration.models import PipelineError, Runtime, contained
from observatorio_secop.orchestration.operations import warehouse_config
from observatorio_secop.population import PopulationScope
from observatorio_secop.publication.models import OWNER_ROLE
from observatorio_secop.publication.store import PublicationStore


def _roots(runtime: Runtime):
    return (runtime.state, runtime.bronze, runtime.silver, runtime.reports)


@contextmanager
def _locked_roots(runtime: Runtime) -> Iterator[None]:
    """Different DagRuns sharing even one root cannot race first-time adoption."""
    if runtime.mode not in ("demo", "live"):
        raise PipelineError("PIPELINE_INVALID_MODE")
    runtime.check(runtime.mode)
    with ExitStack() as stack:
        for root in sorted(_roots(runtime), key=lambda p: str(p.resolve())):
            root.mkdir(parents=True, exist_ok=True, mode=0o700)
            path = contained(root, ".pipeline-binding.lock")
            descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            stream = stack.enter_context(os.fdopen(descriptor, "a+"))
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise PipelineError("PIPELINE_TRANSIENT") from error
        yield


def _root_binding(runtime: Runtime) -> dict[str, Any]:
    paths = [contained(root, ".pipeline-binding.json") for root in _roots(runtime)]
    present = []
    for path in paths:
        if path.is_symlink():
            raise PipelineError("PIPELINE_UNSAFE_PATH")
        if path.exists():
            try:
                present.append(json.loads(path.read_bytes()))
            except (ValueError, OSError) as error:
                raise PipelineError("PIPELINE_RUNTIME_BINDING_CONFLICT") from error
    if present and any(item != present[0] for item in present):
        raise PipelineError("PIPELINE_RUNTIME_BINDING_CONFLICT")
    roots = [str(root.resolve()) for root in _roots(runtime)]
    binding = (
        present[0]
        if present
        else {
            "mode": runtime.mode,
            "workspace_id": uuid.uuid4().hex,
            "roots": roots,
        }
    )
    if (
        not isinstance(binding, dict)
        or set(binding) != {"mode", "workspace_id", "roots"}
        or not isinstance(binding["workspace_id"], str)
        or re.fullmatch(r"[0-9a-f]{32}", binding["workspace_id"]) is None
        or binding["roots"] != roots
    ):
        raise PipelineError("PIPELINE_RUNTIME_BINDING_CONFLICT")
    if binding["mode"] != runtime.mode:
        raise PipelineError("PIPELINE_RUNTIME_MODE_MISMATCH")
    if (
        not present
        and runtime.mode == "demo"
        and any(
            path.exists() and (not path.is_dir() or any(path.iterdir()))
            for path in (
                runtime.bronze / "runs",
                runtime.silver / "versions",
                runtime.silver / "current.json",
            )
        )
    ):
        raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
    # Persist first-time intent before DB commit. A crash after any subset reuses
    # the surviving identity, never reserves a replacement workspace on retry.
    for path in paths:
        if not path.exists():
            durable_write(path, canonical_json(binding) + b"\n")
    return binding


def root_binding(runtime: Runtime) -> dict[str, Any]:
    with _locked_roots(runtime):
        return _root_binding(runtime)


def bind(runtime: Runtime, document: dict) -> None:
    with _locked_roots(runtime):
        value = _root_binding(runtime)
        config = warehouse_config(runtime)
        store = PublicationStore(config)
        with store.transaction() as cursor:
            store._lock(cursor)
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (config.advisory_lock_id,))
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS publication_control.pipeline_runtime (
                    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                    binding jsonb NOT NULL
                )
            """)
            cursor.execute(
                "SELECT binding FROM publication_control.pipeline_runtime WHERE singleton"
            )
            row = cursor.fetchone()
            if row is not None:
                if row[0] != value:
                    raise PipelineError("PIPELINE_RUNTIME_BINDING_CONFLICT")
                return
            cursor.execute("SELECT count(*) FROM publication_control.publications")
            has_publications = cursor.fetchone()[0] > 0
            relation = sql.Identifier(config.schema, config.contract_table).as_string()
            cursor.execute("SELECT to_regclass(%s)", (relation,))
            has_raw = cursor.fetchone()[0] is not None
            if has_raw or has_publications:
                if runtime.mode != "live":
                    raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
                _check_live_adoption(runtime, document, cursor, config)
            cursor.execute(
                "INSERT INTO publication_control.pipeline_runtime(binding) VALUES(%s)",
                (Jsonb(value),),
            )
            cursor.execute(
                "ALTER TABLE publication_control.pipeline_runtime OWNER TO " + OWNER_ROLE
            )
            cursor.execute("REVOKE ALL ON publication_control.pipeline_runtime FROM PUBLIC")


def _check_live_adoption(runtime: Runtime, document: dict, cursor, config) -> None:
    from observatorio_secop.processing.silver.selection import identity
    from observatorio_secop.processing.silver.storage import SilverStorage
    from observatorio_secop.warehouse.activation import ActivationStore
    from observatorio_secop.warehouse.bundle import verify_manifest_bundle
    from observatorio_secop.warehouse.population import PopulationStore

    p = document["parameters"]
    scope = PopulationScope(p["signing_from"], p["signing_to"])
    expected = {"silver_version_id": p["baseline_version"], "manifest_sha256": p["baseline_sha256"]}
    if not p["baseline_version"] or identity(SilverStorage(runtime.silver).current()) != expected:
        raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
    bundle = verify_manifest_bundle(
        contained(runtime.silver, "versions", p["baseline_version"], "manifest.json"),
        expected_hash=p["baseline_sha256"],
    )
    if (
        not bundle.coverage
        or bundle.coverage.get("mode") != "authoritative_candidate"
        or bundle.coverage.get("state") != "complete"
        or bundle.coverage.get("scope_id") != scope.scope_id
        or bundle.coverage.get("scope") != scope.as_dict()
    ):
        raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
    active = ActivationStore(config)
    cursor.execute(
        sql.SQL("SELECT silver_version_id,manifest_sha256 FROM {} WHERE singleton").format(
            active.active
        )
    )
    if cursor.fetchone() != (p["baseline_version"], p["baseline_sha256"]):
        raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
    population = PopulationStore(config)
    cursor.execute(
        sql.SQL("SELECT scope_id,scope FROM {} WHERE singleton").format(population.binding)
    )
    if cursor.fetchone() != (scope.scope_id, scope.as_dict()):
        raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
    cursor.execute(
        "SELECT metadata->>'scope_id' FROM publication_control.publications WHERE ever_activated"
    )
    if any(row[0] != scope.scope_id for row in cursor.fetchall()):
        raise PipelineError("PIPELINE_UNBOUND_AUTHORITATIVE_STATE")
    document["live_adoption"] = expected | {"scope_id": scope.scope_id}
