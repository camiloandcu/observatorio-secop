"""Explicit operator recovery never relaxes an execution's fixed total ceilings."""

from __future__ import annotations

from datetime import UTC, datetime

from .models import PipelineError, Runtime, identifier
from .receipts import Receipts, public_summary


def extend_grant(execution: str, grant_id: str, rows: int, requests: int) -> dict:
    identifier(grant_id)
    runtime = Runtime.environment()
    receipts = Receipts(runtime.state, execution)
    with receipts.locked():
        document = receipts.read()
        p = document["parameters"]
        runtime.check(p["mode"])
        if (
            type(rows) is not int
            or type(requests) is not int
            or not 0 < rows <= p["total_rows"]
            or not 0 < requests <= p["total_requests"]
        ):
            raise PipelineError("PIPELINE_INVALID_LIMIT")
        if document["stages"].get("acquire", {}).get("code") != "PIPELINE_COVERAGE_INCOMPLETE":
            raise PipelineError("PIPELINE_RECOVERY_NOT_APPLICABLE")
        used = {document["grant"]["id"]} | {
            item["grant"]["id"] for item in document["grant_history"]
        }
        if grant_id in used:
            raise PipelineError("PIPELINE_GRANT_CONFLICT")
        document["grant_history"].append(
            {"grant": document["grant"], "changed_at_utc": datetime.now(UTC).isoformat()}
        )
        document["grant"] = {"id": grant_id, "rows": rows, "requests": requests}
        receipts.write(document)
        return public_summary(document)
