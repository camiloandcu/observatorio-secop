"""Safe task process boundary and operator status surface."""

from __future__ import annotations

import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

from observatorio_secop.ingestion.storage import canonical_json
from observatorio_secop.orchestration.models import (
    STAGES,
    PipelineError,
    Runtime,
    contained,
    identifier,
    validate_input,
)
from observatorio_secop.orchestration.receipts import Receipts, public_summary


def _safe_result_code(path: Path) -> str | None:
    """Read only the worker's final bounded JSON diagnostic, never expose its log."""
    with path.open("rb") as stream:
        stream.seek(0, os.SEEK_END)
        size = stream.tell()
        stream.seek(max(0, size - 512))
        tail = stream.read(512)
    try:
        result = json.loads(tail.splitlines()[-1])
    except (ValueError, IndexError):
        return None
    code = result.get("code") if isinstance(result, dict) and set(result) == {"code"} else None
    if isinstance(code, str) and re.fullmatch(r"(?:PIPELINE|PUBLICATION)_[A-Z0-9_]{1,100}", code):
        return code
    return None


def run_stage(stage: str, execution: str, parameters: Any = None) -> dict[str, Any]:
    if stage not in STAGES:
        raise PipelineError("PIPELINE_INVALID_STAGE")
    identifier(execution)
    runtime = Runtime.environment()
    receipts = Receipts(runtime.state, execution)
    if stage == "validate":
        parameters = validate_input(parameters)
        runtime.check(parameters["mode"])
    else:
        try:
            document = receipts.read()
        except OSError:
            raise PipelineError("PIPELINE_RECEIPT_UNAVAILABLE") from None
        runtime.check(document["parameters"]["mode"])
    directory = contained(runtime.reports, "private-task-logs", execution)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = contained(directory, stage + "-" + uuid.uuid4().hex + ".log")
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        from .process import private_worker

        returncode = private_worker(
            canonical_json({"stage": stage, "execution_id": execution, "parameters": parameters}),
            output,
            timeout=1800,
            cwd=str(runtime.project),
        )
    if returncode:
        safe_code = (
            "PIPELINE_TRANSIENT"
            if returncode < 0 or returncode == 75
            else (_safe_result_code(path) or "PIPELINE_STAGE_REJECTED")
        )
        raise PipelineError(safe_code)
    return public_summary(receipts.read())


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Bounded local analytical pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("run", help="Run all stages with private fixed input JSON")
    start.add_argument("--execution-id", required=True)
    start.add_argument("--input", type=Path, required=True)
    inspect = commands.add_parser("inspect", help="Show only sanitized durable progress")
    inspect.add_argument("--execution-id", required=True)
    resume = commands.add_parser("stage", help="Retry exactly one stage using its existing binding")
    resume.add_argument("--execution-id", required=True)
    resume.add_argument("--stage", choices=STAGES, required=True)
    extend = commands.add_parser(
        "extend-grant", help="Explicit new invocation within fixed campaign ceilings"
    )
    extend.add_argument("--execution-id", required=True)
    extend.add_argument("--grant-id", required=True)
    extend.add_argument("--rows", type=int, required=True)
    extend.add_argument("--requests", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "extend-grant":
            from .recovery import extend_grant

            print(
                json.dumps(extend_grant(args.execution_id, args.grant_id, args.rows, args.requests))
            )
        elif args.command == "inspect":
            print(
                json.dumps(
                    public_summary(Receipts(Runtime.environment().state, args.execution_id).read())
                )
            )
        elif args.command == "run":
            parameters = json.loads(args.input.read_text())
            for stage in STAGES:
                report = run_stage(
                    stage, args.execution_id, parameters if stage == "validate" else None
                )
            print(json.dumps(report))
        else:
            parameters = None
            if args.stage == "validate":
                parameters = Receipts(Runtime.environment().state, args.execution_id).read()[
                    "parameters"
                ]
            print(json.dumps(run_stage(args.stage, args.execution_id, parameters)))
        return 0
    except PipelineError as error:
        print(json.dumps({"code": error.code}), file=sys.stderr)
        return 2
    except Exception:
        print(json.dumps({"code": "PIPELINE_OPERATION_REJECTED"}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
