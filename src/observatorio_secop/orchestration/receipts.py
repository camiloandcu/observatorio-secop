"""Private atomic receipts survive scheduler/XCom acknowledgement loss."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from observatorio_secop.ingestion.storage import canonical_json, durable_write
from observatorio_secop.orchestration.models import PipelineError, contained, identifier


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


class Receipts:
    def __init__(self, root: Path, execution: str):
        self.execution = identifier(execution)
        self.directory = contained(root, self.execution)
        self.path = contained(self.directory, "receipt.json")

    @contextmanager
    def locked(self) -> Iterator[None]:
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = contained(self.directory, "execution.lock")
        with lock.open("a+") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise PipelineError("PIPELINE_BUSY") from error
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def write(self, document: dict[str, Any]) -> None:
        document = dict(document)
        document.pop("receipt_sha256", None)
        document["receipt_sha256"] = digest(document)
        previous = os.umask(0o077)
        try:
            durable_write(self.path, canonical_json(document) + b"\n")
        finally:
            os.umask(previous)

    def read(self) -> dict[str, Any]:
        document = json.loads(self.path.read_bytes())
        expected = document.pop("receipt_sha256", None)
        if expected != digest(document) or document.get("execution_id") != self.execution:
            raise PipelineError("PIPELINE_RECEIPT_CONFLICT")
        return document

    def create(self, parameters: dict[str, Any], recipe: dict[str, Any]) -> dict[str, Any]:
        if self.path.exists():
            document = self.read()
            if document["parameters"] != parameters or document["recipe"] != recipe:
                raise PipelineError("PIPELINE_INPUT_CHANGED")
            return document
        document = {
            "schema_version": 1,
            "execution_id": self.execution,
            "parameters": parameters,
            "recipe": recipe,
            "campaign_id": "pipeline-" + self.execution,
            "publication_id": digest({"execution_id": self.execution})[:32],
            "expected_active": None,
            "grant": {
                "id": "initial",
                "rows": parameters["grant_rows"],
                "requests": parameters["grant_requests"],
            },
            "grant_history": [],
            "stages": {},
            "status": "running",
        }
        self.write(document)
        return document


def public_summary(document: dict[str, Any]) -> dict[str, Any]:
    """Never export arbitrary receipt keys or producer metadata."""
    return {
        "execution_id": document["execution_id"],
        "status": document["status"],
        "mode": document["parameters"]["mode"],
        "campaign_id": document["campaign_id"],
        "publication_id": document["publication_id"],
        "stages": {
            name: {
                key: value
                for key, value in stage.items()
                if key
                in {
                    "status",
                    "code",
                    "attempt",
                    "duration_seconds",
                    "rows",
                    "requests",
                    "run_id",
                    "silver_version_id",
                    "manifest_sha256",
                    "publication_id",
                }
            }
            for name, stage in document["stages"].items()
        },
    }
