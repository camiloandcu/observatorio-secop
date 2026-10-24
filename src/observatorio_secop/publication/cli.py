"""Local operator and read-only commands for validated analytical publications."""

from __future__ import annotations

import argparse
import json
import re
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any

import psycopg

from observatorio_secop.warehouse.cli import _bundle
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import WarehouseError

from .build import run_candidate_build
from .capture import capture_inputs
from .errors import PublicationError
from .queries import MACROS, query_metric
from .readers import read_publication, resolve, validate_descriptor
from .store import PublicationStore


def _code(error: Exception, fallback: str) -> str:
    candidate = getattr(error, "code", str(error))
    return candidate if re.fullmatch(r"PUBLICATION_[A-Z0-9_]+", candidate) else fallback


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Build, inspect and consume validated analytics")
    result.add_argument("--config", type=Path, default=Path("config/warehouse.yaml"))
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "bootstrap", help="Initialize dedicated roles/control state with operator access"
    )
    inspect = commands.add_parser("inspect", help="Inspect operator state without changing it")
    inspect.add_argument("--publication-id")
    for name in ("build", "retry"):
        build = commands.add_parser(name, help="Construct a complete private analytical candidate")
        build.add_argument("--publication-id", required=name == "retry")
        if name == "build":
            build.add_argument("--version", required=True)
            build.add_argument("--manifest-sha256", required=True)
        build.add_argument("--artifact-root", type=Path, default=Path("reports/publication"))
        build.add_argument("--timeout-seconds", type=int, required=True)
        build.add_argument("--storage-budget-bytes", type=int, required=True)
    for name in ("activate", "restore"):
        action = commands.add_parser(name)
        action.add_argument("--publication-id", required=True)
        action.add_argument(
            "--expected-publication-id", help="Omit only before the first activation"
        )
        action.add_argument("--expected-generation", type=int, required=True)
    discard = commands.add_parser(
        "discard", help="Explicitly discard an inspected private candidate"
    )
    discard.add_argument("--publication-id", required=True)
    commands.add_parser(
        "cleanup-preview", help="List exact obsolete registered IDs eligible for removal"
    )
    cleanup = commands.add_parser(
        "cleanup", help="Remove only explicitly selected eligible publications"
    )
    cleanup.add_argument("--publication-id", action="append", required=True)
    pin = commands.add_parser(
        "resolve", help="Read-only: choose a publication once for an operation"
    )
    pin.add_argument("--publication-id")
    for name in ("validate-descriptor", "read", "query"):
        action = commands.add_parser(name)
        action.add_argument("--descriptor", type=Path, required=True)
        if name in ("read", "query"):
            if name == "read":
                action.add_argument("--relation", required=True)
            else:
                action.add_argument("--metric", choices=MACROS, required=True)
                action.add_argument("--filters", type=Path)
            action.add_argument("--limit", type=int, default=100)
            action.add_argument("--offset", type=int, default=0)
    return result


def _build(args: argparse.Namespace, store: PublicationStore) -> dict[str, Any]:
    if args.timeout_seconds <= 0 or args.storage_budget_bytes <= 0:
        raise PublicationError("PUBLICATION_INVALID", "Finite positive build budgets are required")
    if args.command == "build":
        # Verify before reservation/database access; never repin mutable current.json.
        bundle = _bundle(store.config.silver_root, args.version, args.manifest_sha256)
        row = store.reserve(
            {
                "silver_version_id": bundle.version_id,
                "silver_manifest_sha256": bundle.manifest_sha256,
            },
            args.publication_id,
        )
    else:
        row = store.inspect(args.publication_id)
        # Do not fetch latest or reset expected active state during a pinned retry.
        bundle = _bundle(
            store.config.silver_root,
            row["metadata"].get("silver_version_id"),
            row["metadata"].get("silver_manifest_sha256"),
        )
        store.retry(row["publication_id"])
    value = row["publication_id"]
    try:
        metadata = row["metadata"]
        if "captured_at" not in metadata:
            captured = capture_inputs(
                store.config, bundle, value, storage_budget_bytes=args.storage_budget_bytes
            )
            metadata = store.update_metadata(value, captured)["metadata"]
        receipt = run_candidate_build(
            store.config,
            value,
            artifact_root=args.artifact_root,
            timeout_seconds=args.timeout_seconds,
            storage_budget_bytes=args.storage_budget_bytes,
            metadata=metadata,
        )
        store.update_metadata(value, receipt["metadata"])
        sealed = store.seal(value, receipt)
        return {
            "publication_id": value,
            "state": sealed["state"],
            "expected_publication_id": row["expected_publication_id"],
            "expected_generation": row["expected_generation"],
            "built_at": sealed["built_at"],
            "metadata": sealed["metadata"],
        }
    except (ValueError, OSError, psycopg.Error) as error:
        code = _code(error, "PUBLICATION_BUILD_FAILED")
        # A failure to record diagnostics must not hide the original rejection.
        with suppress(ValueError, psycopg.Error):
            store.mark_failed(value, code)
        raise


def _run(args: argparse.Namespace) -> Any:
    config = load_warehouse_config(args.config)
    if args.command == "resolve":
        return resolve(config, args.publication_id)
    if args.command in ("read", "query", "validate-descriptor"):
        descriptor = json.loads(args.descriptor.read_text(encoding="utf-8"))
        if args.command == "validate-descriptor":
            return validate_descriptor(config, descriptor)
        if args.command == "query":
            filters = json.loads(args.filters.read_text()) if args.filters else None
            return query_metric(
                config, descriptor, args.metric, filters, limit=args.limit, offset=args.offset
            )
        return read_publication(
            config, descriptor, args.relation, limit=args.limit, offset=args.offset
        )
    store = PublicationStore(config)
    if args.command == "bootstrap":
        return store.bootstrap()
    if args.command == "inspect":
        return store.inspect(args.publication_id)
    if args.command in ("build", "retry"):
        return _build(args, store)
    if args.command in ("activate", "restore"):
        return store.activate(
            args.publication_id,
            args.expected_publication_id,
            args.expected_generation,
            restore=args.command == "restore",
        )
    if args.command == "discard":
        return store.discard(args.publication_id)
    if args.command == "cleanup-preview":
        return store.cleanup_preview()
    return store.cleanup(args.publication_id)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        print(json.dumps(_run(args), sort_keys=True, default=str))
        return 0
    except PublicationError as error:
        code = error.code
    except (WarehouseError, ValueError) as error:
        code = _code(error, "PUBLICATION_INVALID_INPUT")
    except OSError:
        code = "PUBLICATION_LOCAL_RESOURCE_UNAVAILABLE"
    except psycopg.Error:
        code = "PUBLICATION_DATABASE_UNAVAILABLE"
    print(
        json.dumps({"code": code, "message": "Publication operation did not complete"}),
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
