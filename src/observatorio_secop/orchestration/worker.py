"""Private subprocess entry: noisy producers never stream to Airflow diagnostics."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from typing import Any

from observatorio_secop.ingestion.storage import canonical_json
from observatorio_secop.orchestration import operations, publication
from observatorio_secop.orchestration.models import STAGES, PipelineError, Runtime, validate_input
from observatorio_secop.orchestration.receipts import Receipts, digest


def recipe(runtime: Runtime) -> dict[str, Any]:
    from observatorio_secop.publication.build import reviewed_metadata

    resources = {}
    for folder in ("contracts", "config"):
        for path in sorted((runtime.project / folder).rglob("*")):
            if path.is_file():
                resources[str(path.relative_to(runtime.project))] = digest(path.read_text())
    return {
        **reviewed_metadata(runtime.project),
        "resources_sha256": digest(resources),
        "runtime_image": os.environ.get("SECOP_IMAGE_REVISION", "local-locked"),
        "roots": [
            str(p.resolve())
            for p in (runtime.state, runtime.bronze, runtime.silver, runtime.reports)
        ],
        "database": {
            key: os.environ.get(key)
            for key in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER")
        },
    }


def code(error: Exception) -> str:
    import psycopg

    from observatorio_secop.ingestion.errors import ConcurrentRunError, TransportError

    if isinstance(
        error, psycopg.OperationalError | psycopg.InterfaceError | ConcurrentRunError | TimeoutError
    ):
        return "PIPELINE_TRANSIENT"
    if isinstance(error, TransportError) and (
        error.status is None or error.status == 429 or error.status >= 500
    ):
        return "PIPELINE_TRANSIENT"
    value = getattr(error, "code", "")
    if isinstance(value, str) and re.fullmatch(r"(?:PIPELINE|PUBLICATION)_[A-Z0-9_]+", value):
        return value
    cause = error.__cause__
    if cause is not None and cause is not error and code(cause) == "PIPELINE_TRANSIENT":
        return "PIPELINE_TRANSIENT"
    return "PIPELINE_STAGE_REJECTED"


def dispatch(stage: str, runtime: Runtime, document: dict[str, Any]) -> dict[str, Any]:
    if stage == "validate":
        from observatorio_secop.publication.store import PublicationStore
        from observatorio_secop.source_profile.contract import load_contract

        operations.baseline(runtime, document["parameters"])
        load_contract(runtime.project / "contracts/secop_source.yaml")
        store = PublicationStore(operations.warehouse_config(runtime))
        store.bootstrap()
        from .binding import bind

        bind(runtime, document)
        active = store.inspect()["active"]
        if document["expected_active"] is None:
            document["expected_active"] = {k: active[k] for k in ("publication_id", "generation")}
        return {}
    if stage == "outcome":
        return {"publication_id": document["publication_id"], "scoring_state": "ABSENT"}
    function = {
        "acquire": operations.acquisition,
        "silver": operations.silver,
        "warehouse": operations.warehouse,
        "candidate": publication.candidate,
        "activate": publication.activate,
    }[stage]
    return function(runtime, document)


def run(stage: str, execution: str, parameters: Any = None) -> None:
    if stage not in STAGES:
        raise PipelineError("PIPELINE_INVALID_STAGE")
    runtime = Runtime.environment()
    receipts = Receipts(runtime.state, execution)
    with receipts.locked():
        if stage == "validate":
            parameters = validate_input(parameters)
            runtime.check(parameters["mode"])
            document = receipts.create(parameters, recipe(runtime))
        else:
            document = receipts.read()
            runtime.check(document["parameters"]["mode"])
            if recipe(runtime) != document["recipe"]:
                raise PipelineError("PIPELINE_INPUT_CHANGED")
        for prior in STAGES[: STAGES.index(stage)]:
            if document["stages"].get(prior, {}).get("status") != "success":
                raise PipelineError("PIPELINE_UPSTREAM_INCOMPLETE")
        prior_result = document["stages"].get(stage, {})
        result = {
            "rows": None,
            "requests": None,
            "run_id": document["campaign_id"],
            "silver_version_id": document["stages"].get("silver", {}).get("silver_version_id"),
            "manifest_sha256": document["stages"].get("silver", {}).get("manifest_sha256"),
            "publication_id": document["stages"].get("candidate", {}).get("publication_id"),
            **prior_result,
            "status": "running",
            "attempt": prior_result.get("attempt", 0) + 1,
            "started_at_utc": datetime.now(UTC).isoformat(),
            "code": None,
        }
        document["stages"][stage] = result
        receipts.write(document)  # intent is durable before producer effects
        started = time.monotonic()
        try:
            output = dispatch(stage, runtime, document)
            faults = os.environ.get("SECOP_PIPELINE_FAULT", "").split(",")
            if runtime.mode == "demo" and stage + ":after_effect" in faults:
                marker = receipts.directory / (stage + ".fault-consumed")
                if not marker.exists():
                    from observatorio_secop.ingestion.storage import durable_write

                    durable_write(marker, b"one-shot crash boundary\n")
                    os._exit(75)  # actual crash before receipt/output acknowledgement
            result.update(output)
            result["status"] = "success"
            document["status"] = "success" if stage == "outcome" else "running"
        except Exception as error:
            result.update(status="failed", code=code(error))
            document["status"] = (
                "incomplete" if result["code"] == "PIPELINE_COVERAGE_INCOMPLETE" else "failed"
            )
            raise PipelineError(result["code"]) from None
        finally:
            result["duration_seconds"] = round(time.monotonic() - started, 6)
            result["finished_at_utc"] = datetime.now(UTC).isoformat()
            receipts.write(document)


def main() -> int:
    os.umask(0o077)
    try:
        payload = json.loads(sys.stdin.buffer.read())
        run(payload["stage"], payload["execution_id"], payload.get("parameters"))
        return 0
    except Exception as error:
        # Deliberately emit only a stable code; detailed producer output is private.
        sys.stderr.write(canonical_json({"code": code(error)}).decode() + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
