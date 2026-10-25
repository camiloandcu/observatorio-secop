"""Pin publication construction and activation to the execution's original state."""

from __future__ import annotations

import argparse
from typing import Any

from observatorio_secop.orchestration.models import PipelineError, Runtime, contained
from observatorio_secop.orchestration.operations import bundle_for, warehouse_config
from observatorio_secop.publication.cli import _build
from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.store import PublicationStore


def candidate(runtime: Runtime, document: dict[str, Any]) -> dict[str, Any]:
    store = PublicationStore(warehouse_config(runtime))
    bundle = bundle_for(runtime, document)
    identity = document["publication_id"]
    try:
        row = store.inspect(identity)
    except PublicationError as error:
        if error.code != "PUBLICATION_UNKNOWN":
            raise
        row = store.reserve(
            {
                "silver_version_id": bundle.version_id,
                "silver_manifest_sha256": bundle.manifest_sha256,
            },
            identity,
        )
    expected = document["expected_active"]
    if (row["expected_publication_id"], row["expected_generation"]) != (
        expected["publication_id"],
        expected["generation"],
    ):
        raise PipelineError("PIPELINE_STALE_PUBLICATION")
    if (
        row["metadata"].get("silver_version_id"),
        row["metadata"].get("silver_manifest_sha256"),
    ) != (bundle.version_id, bundle.manifest_sha256):
        raise PipelineError("PIPELINE_PUBLICATION_CONFLICT")
    if row["state"] not in ("sealed", "retained"):
        p = document["parameters"]
        _build(
            argparse.Namespace(
                command="retry",
                publication_id=identity,
                timeout_seconds=p["build_seconds"],
                storage_budget_bytes=p["storage_bytes"],
                artifact_root=contained(runtime.reports, "publication"),
            ),
            store,
        )
    return {
        "publication_id": identity,
        "silver_version_id": bundle.version_id,
        "manifest_sha256": bundle.manifest_sha256,
        "rows": bundle.row_count,
    }


def activate(runtime: Runtime, document: dict[str, Any]) -> dict[str, Any]:
    store = PublicationStore(warehouse_config(runtime))
    row = store.inspect(document["publication_id"])
    active = store.inspect()["active"]
    if row["ever_activated"] and active["publication_id"] != document["publication_id"]:
        raise PipelineError("PIPELINE_ACTIVATION_SUPERSEDED")
    expected = document["expected_active"]
    result = store.activate(
        document["publication_id"], expected["publication_id"], expected["generation"]
    )
    return {"publication_id": document["publication_id"], "generation": result["generation"]}
