"""Run the entire reviewed dbt graph against captured, private publication inputs."""

from __future__ import annotations

import hashlib
import json
import os
import runpy
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, cast

import psycopg
import yaml
from psycopg import sql

from observatorio_secop.publication.capture import _connect, _identity, _one, _positive
from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.warehouse.config import WarehouseConfig

ROOT = Path(__file__).resolve().parents[3]


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise PublicationError("PUBLICATION_BUILD_TIMEOUT", "The finite build budget expired")
    return remaining


class _BudgetCursor:
    """Apply the remaining whole-build budget to each validation query and fetch."""

    def __init__(self, cursor: psycopg.Cursor[Any], deadline: float):
        self.cursor = cursor
        self.deadline = deadline

    def execute(self, query: Any, parameters: Any = None) -> None:
        milliseconds = max(1, int(_remaining(self.deadline) * 1000))
        self.cursor.execute(
            "SELECT set_config('statement_timeout',%s,true)", (f"{milliseconds}ms",)
        )
        try:
            self.cursor.execute(query, parameters)
        except psycopg.errors.QueryCanceled as error:
            raise PublicationError(
                "PUBLICATION_BUILD_TIMEOUT", "Publication validation exceeded its build budget"
            ) from error
        _remaining(self.deadline)

    def fetchone(self) -> Any:
        _remaining(self.deadline)
        result = self.cursor.fetchone()
        _remaining(self.deadline)
        return result

    def fetchall(self) -> Any:
        _remaining(self.deadline)
        result = self.cursor.fetchall()
        _remaining(self.deadline)
        return result

    def fetchmany(self, size: int) -> Any:
        _remaining(self.deadline)
        result = self.cursor.fetchmany(size)
        _remaining(self.deadline)
        return result


def _budget_cursor(cursor: psycopg.Cursor[Any], deadline: float) -> psycopg.Cursor[Any]:
    # Internal SQL helpers use only execute/fetch methods; preserve their existing
    # cursor contract while checking time even between streamed content batches.
    return cast("psycopg.Cursor[Any]", _BudgetCursor(cursor, deadline))


def reviewed_metadata(project_root: Path = ROOT) -> dict[str, str]:
    """Reuse the development build's contract validator, then pin its exact bytes."""
    namespace = runpy.run_path(str(project_root / "scripts/gold_dbt.py"))
    namespace["validate_contracts"](project_root)
    project = yaml.safe_load((project_root / "dbt/dbt_project.yml").read_text())
    variables = project["vars"]
    return {
        "catalog_sha256": variables["gold_catalog_sha256"],
        "policy_sha256": variables["gold_policy_sha256"],
        "policy_version": variables["gold_policy_version"],
        "dbt_graph_sha256": _graph_digest(project_root),
        "code_revision": "sha256:" + _implementation_digest(project_root),
    }


def _implementation_digest(project_root: Path) -> str:
    """Identify exact executing source bytes, including an uncommitted worktree."""
    checksum = hashlib.sha256()
    for path in sorted((ROOT / "src/observatorio_secop").rglob("*.py")):
        checksum.update(path.relative_to(ROOT).as_posix().encode() + b"\0" + path.read_bytes())
    for path in (ROOT / "uv.lock", ROOT / "pyproject.toml", project_root / "scripts/gold_dbt.py"):
        checksum.update(path.name.encode() + b"\0" + path.read_bytes())
    return checksum.hexdigest()


def _graph_digest(root: Path) -> str:
    digest = hashlib.sha256()
    paths = [
        path
        for path in (root / "dbt").rglob("*")
        if path.is_file()
        and path.suffix in (".sql", ".yml", ".yaml")
        and not set(path.relative_to(root / "dbt").parts) & {"target", "logs", "dbt_packages"}
    ]
    paths += [root / "contracts/gold_metrics.yaml", root / "config/gold_policy.yaml"]
    for path in sorted(paths):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


def validate_artifacts(
    target: Path,
    publication_id: str,
    expected_nodes: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """A zero process exit cannot replace a complete successful test inventory."""
    publication_id = _identity(publication_id)
    manifest_path, results_path = target / "manifest.json", target / "run_results.json"
    manifest = json.loads(manifest_path.read_bytes())
    results = json.loads(results_path.read_bytes())
    actual_nodes = _required_nodes(manifest)
    if set(actual_nodes) != set(expected_nodes):
        raise ValueError("PUBLICATION_DBT_INVENTORY_CHANGED")
    if not manifest.get("metadata", {}).get("invocation_id") or manifest["metadata"][
        "invocation_id"
    ] != results.get("metadata", {}).get("invocation_id"):
        raise ValueError("PUBLICATION_DBT_STALE_RESULTS")
    observed: dict[str, str] = {}
    for item in results.get("results", []):
        unique_id, status = item.get("unique_id"), item.get("status")
        if unique_id not in actual_nodes:
            continue  # on-run hooks are operations, not selectable graph resources
        if unique_id in observed:
            raise ValueError("PUBLICATION_DBT_DUPLICATE_RESULT")
        observed[unique_id] = status
    if set(observed) != set(expected_nodes):
        raise ValueError("PUBLICATION_DBT_INCOMPLETE_RESULTS")
    models: list[str] = []
    tests: list[str] = []
    expected_schemas = {
        f"candidate_{publication_id}_{part}"
        for part in ("staging", "intermediate", "analytics", "dbt_test__audit")
    }
    for unique_id, node in actual_nodes.items():
        kind = node["resource_type"]
        if observed[unique_id] != ("pass" if kind == "test" else "success"):
            raise ValueError("PUBLICATION_DBT_VALIDATION_FAILED")
        if node.get("schema") not in expected_schemas:
            raise ValueError("PUBLICATION_DBT_NAMESPACE_ESCAPE")
        if node.get("checksum") != expected_nodes[unique_id].get("checksum"):
            raise ValueError("PUBLICATION_DBT_CODE_CHANGED")
        (tests if kind == "test" else models).append(unique_id)
    if not tests or not models:
        raise ValueError("PUBLICATION_DBT_EMPTY_INVENTORY")
    sources = manifest.get("sources", {})
    if len(sources) != 6 or any(
        item.get("schema") != f"candidate_{publication_id}_input" for item in sources.values()
    ):
        raise ValueError("PUBLICATION_DBT_SOURCE_ESCAPE")
    return {
        "version": 1,
        "publication_id": publication_id,
        "required_nodes": sorted(actual_nodes),
        "model_nodes": sorted(models),
        "test_nodes": sorted(tests),
        "results": [{"unique_id": key, "status": observed[key]} for key in sorted(observed)],
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "run_results_sha256": hashlib.sha256(results_path.read_bytes()).hexdigest(),
        "dbt_invocation_id": manifest["metadata"]["invocation_id"],
    }


def _required_nodes(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        key: node
        for key, node in manifest.get("nodes", {}).items()
        if node.get("resource_type") in ("model", "test")
        and node.get("config", {}).get("enabled", True)
    }


def run_candidate_build(
    config: WarehouseConfig,
    publication_id: str,
    *,
    artifact_root: Path,
    timeout_seconds: int,
    storage_budget_bytes: int,
    metadata: dict[str, Any] | None = None,
    project_root: Path = ROOT,
) -> dict[str, Any]:
    """Hold the distinct candidate lock so discard cannot destroy a live build."""
    from observatorio_secop.publication.models import candidate_lock_id

    publication_id = _identity(publication_id)
    _positive(timeout_seconds, "BUILD_TIMEOUT")
    _positive(storage_budget_bytes, "STORAGE_BUDGET")
    with _connect(config, autocommit=True) as guard:
        locked = guard.execute(
            "SELECT pg_try_advisory_lock(%s)", (candidate_lock_id(publication_id),)
        ).fetchone()
        if locked != (True,):
            raise ValueError("PUBLICATION_BUSY")
        return _run_candidate_build(
            config,
            publication_id,
            artifact_root=artifact_root,
            timeout_seconds=timeout_seconds,
            storage_budget_bytes=storage_budget_bytes,
            metadata=metadata,
            project_root=project_root,
        )


def _run_candidate_build(
    config: WarehouseConfig,
    publication_id: str,
    *,
    artifact_root: Path,
    timeout_seconds: int,
    storage_budget_bytes: int,
    metadata: dict[str, Any] | None = None,
    project_root: Path = ROOT,
) -> dict[str, Any]:
    """Build as the restricted builder and return a sealable validation receipt.

    Artifact/profile/log paths remain private. PostgreSQL credentials exist only in
    subprocess environment values; the generated profile refers to those variables.
    """
    publication_id = _identity(publication_id)
    _positive(timeout_seconds, "BUILD_TIMEOUT")
    _positive(storage_budget_bytes, "STORAGE_BUDGET")
    reviewed = reviewed_metadata(project_root)
    metadata = dict(metadata or {})
    for key, value in reviewed.items():
        if key in metadata and metadata[key] != value:
            raise ValueError("PUBLICATION_CONTRACT_CHANGED")
        metadata[key] = value
    # Pin the recipe before any dbt process; a failed candidate cannot retry
    # different code/catalog/policy under its original immutable identity.
    from observatorio_secop.publication.store import PublicationStore

    metadata = PublicationStore(config).update_metadata(publication_id, metadata)["metadata"]
    started = time.monotonic()
    deadline = started + timeout_seconds
    _prepare_schemas(config, publication_id, storage_budget_bytes, deadline=deadline)
    directory = artifact_root / publication_id / uuid.uuid4().hex
    target, logs, profiles = directory / "target", directory / "logs", directory / "profiles"
    profiles.mkdir(parents=True)
    _write_profile(profiles / "profiles.yml", publication_id)
    environment = {key: value for key, value in os.environ.items() if not key.startswith("DBT_")}
    environment.update(
        POSTGRES_HOST=config.host,
        POSTGRES_PORT=str(config.port),
        POSTGRES_DB=config.dbname,
        POSTGRES_USER=config.user,
        POSTGRES_PASSWORD=config.password,
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    executable = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    options = [
        "--no-partial-parse",
        "--project-dir",
        str(project_root / "dbt"),
        "--profiles-dir",
        str(profiles),
        "--target-path",
        str(target),
        "--log-path",
        str(logs),
        "--vars",
        json.dumps({"publication_id": publication_id}),
    ]

    def execute(command: str, *arguments: str) -> None:
        remaining = _remaining(deadline)
        # libpq applies this bound to every dbt connection, including worker
        # threads; killing the client must not leave an unbounded server query.
        environment["PGOPTIONS"] = (
            "-c role=secop_publication_builder "
            f"-c statement_timeout={max(1, int(remaining * 1000))} -c lock_timeout=5000"
        )
        try:
            process = subprocess.run(
                [executable, command, *arguments, *options],
                cwd=project_root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=remaining,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise PublicationError(
                "PUBLICATION_BUILD_TIMEOUT", "dbt exceeded the finite build budget"
            ) from error
        # Keep detailed diagnostics only in the private invocation log directory.
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{command}.stdout.log").write_text(process.stdout)
        (directory / f"{command}.stderr.log").write_text(process.stderr)
        _remaining(deadline)
        if process.returncode:
            raise ValueError(f"PUBLICATION_DBT_{command.upper()}_FAILED")

    execute("parse")
    expected_manifest = json.loads((target / "manifest.json").read_bytes())
    expected_nodes = _required_nodes(expected_manifest)
    expected_model_names = {path.stem for path in (project_root / "dbt/models").rglob("*.sql")}
    if {
        node["name"] for node in expected_nodes.values() if node["resource_type"] == "model"
    } != expected_model_names:
        raise ValueError("PUBLICATION_DBT_INCOMPLETE_GRAPH")
    PublicationStore(config).register_expected_nodes(publication_id, sorted(expected_nodes))
    execute("build", "--full-refresh")
    if reviewed_metadata(project_root) != reviewed:
        raise ValueError("PUBLICATION_CONTRACT_CHANGED")
    receipt = validate_artifacts(target, publication_id, expected_nodes)
    from observatorio_secop.publication.inventory import relation_inventory

    schema = f"candidate_{publication_id}_analytics"
    with _connect(config) as database, database.cursor() as raw_cursor:
        cursor = _budget_cursor(raw_cursor, deadline)
        cursor.execute(
            sql.SQL("SELECT count(*) FROM {}._gold_build_binding").format(sql.Identifier(schema))
        )
        if cursor.fetchone() != (0,):
            raise ValueError("PUBLICATION_DBT_BINDING_INCOMPLETE")
        cursor.execute(sql.SQL("DROP TABLE {}._gold_build_binding").format(sql.Identifier(schema)))
        _reconcile(cursor, publication_id, metadata)
        inventory = relation_inventory(cursor, schema)
        output_bytes = sum(item["total_bytes"] for item in inventory.values())
        total_bytes = _candidate_bytes(cursor, publication_id)
        if total_bytes > storage_budget_bytes:
            raise ValueError("PUBLICATION_STORAGE_CAPACITY")
        cursor.execute("SELECT pg_database_size(current_database())")
        database_bytes = int(_one(cursor)[0])
    receipt.update(
        metadata=metadata,
        inventory=inventory,
        metrics={
            "output_bytes": output_bytes,
            "candidate_bytes": total_bytes,
            "database_bytes_after_build": database_bytes,
            "build_seconds": round(time.monotonic() - started, 6),
        },
    )
    _remaining(deadline)
    return receipt


def _candidate_bytes(cursor: psycopg.Cursor[Any], publication_id: str) -> int:
    cursor.execute(
        "SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=ANY(%s) AND c.relkind IN ('r','m')",
        (
            [
                f"candidate_{publication_id}_{part}"
                for part in ("input", "staging", "intermediate", "analytics")
            ],
        ),
    )
    return int(_one(cursor)[0])


def _prepare_schemas(
    config: WarehouseConfig, publication_id: str, budget: int, *, deadline: float
) -> None:
    with _connect(config) as database, database.cursor() as raw_cursor:
        cursor = _budget_cursor(raw_cursor, deadline)
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        cursor.execute(
            "SELECT state FROM publication_control.publications WHERE publication_id=%s FOR SHARE",
            (publication_id,),
        )
        if cursor.fetchone() != ("building",):
            raise ValueError("PUBLICATION_NOT_RESERVED")
        cursor.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname=%s", (f"candidate_{publication_id}_input",)
        )
        if cursor.fetchone() is None:
            raise ValueError("PUBLICATION_INPUTS_UNAVAILABLE")
        if _candidate_bytes(cursor, publication_id) > budget:
            raise ValueError("PUBLICATION_STORAGE_CAPACITY")
        for part in ("staging", "intermediate", "analytics"):
            schema = sql.Identifier(f"candidate_{publication_id}_{part}")
            cursor.execute(
                sql.SQL(
                    "CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION secop_publication_builder"
                ).format(schema)
            )
            cursor.execute(sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(schema))


def _write_profile(path: Path, publication_id: str) -> None:
    profile = {
        "type": "postgres",
        "host": "{{ env_var('POSTGRES_HOST') }}",
        "port": "{{ env_var('POSTGRES_PORT') | int }}",
        "user": "{{ env_var('POSTGRES_USER') }}",
        "password": "{{ env_var('POSTGRES_PASSWORD') }}",
        "dbname": "{{ env_var('POSTGRES_DB') }}",
        "schema": f"candidate_{publication_id}_analytics",
        "role": "secop_publication_builder",
        "threads": 2,
        "connect_timeout": 10,
        "retries": 0,
        "sslmode": "prefer",
    }
    path.write_text(
        yaml.safe_dump(
            {"observatorio_secop": {"target": "publication", "outputs": {"publication": profile}}}
        )
    )


def _reconcile(cursor: psycopg.Cursor[Any], publication_id: str, metadata: dict[str, Any]) -> None:
    source = sql.Identifier(f"candidate_{publication_id}_input", "contract_current")
    detail = sql.Identifier(f"candidate_{publication_id}_analytics", "mart_contract_detail")
    cursor.execute(sql.SQL("SELECT count(*),sum(contract_value_cop) FROM {}").format(source))
    expected = cursor.fetchone()
    cursor.execute(sql.SQL("SELECT count(*),sum(contract_value_cop) FROM {}").format(detail))
    if cursor.fetchone() != expected:
        raise ValueError("PUBLICATION_RECONCILIATION_FAILED")
    cursor.execute(
        sql.SQL(
            "SELECT DISTINCT silver_version_id,silver_manifest_sha256,scope_id,"
            "catalog_sha256,policy_sha256 FROM {}"
        ).format(detail)
    )
    if cursor.fetchall() != [
        (
            metadata.get("silver_version_id"),
            metadata.get("silver_manifest_sha256"),
            metadata.get("scope_id"),
            metadata["catalog_sha256"],
            metadata["policy_sha256"],
        )
    ]:
        raise ValueError("PUBLICATION_OUTPUT_BINDING_CHANGED")
