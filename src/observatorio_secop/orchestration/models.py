"""Strict trigger values and operator-owned runtime locations."""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from observatorio_secop.population import PopulationScope

STAGES = ("validate", "acquire", "silver", "warehouse", "candidate", "activate", "outcome")
IDENTIFIER = re.compile(r"^[a-zA-Z0-9_-]{1,100}$")
CHECKSUM = re.compile(r"^[0-9a-f]{64}$")


class PipelineError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def identifier(value: Any) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise PipelineError("PIPELINE_INVALID_ID")
    return value


def validate_input(value: Any) -> dict[str, Any]:
    required = {
        "mode",
        "signing_from",
        "signing_to",
        "baseline_version",
        "baseline_sha256",
        "total_rows",
        "total_requests",
        "grant_rows",
        "grant_requests",
        "page_size",
        "build_seconds",
        "storage_bytes",
        "fixture_id",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise PipelineError("PIPELINE_INVALID_INPUT")
    if value["mode"] not in ("demo", "live"):
        raise PipelineError("PIPELINE_INVALID_MODE")
    PopulationScope(value["signing_from"], value["signing_to"])
    for name in (
        "total_rows",
        "total_requests",
        "grant_rows",
        "grant_requests",
        "page_size",
        "build_seconds",
        "storage_bytes",
    ):
        if type(value[name]) is not int or not 0 < value[name] <= 2**63 - 1:
            raise PipelineError("PIPELINE_INVALID_LIMIT")
    if (
        value["page_size"] > 250
        or value["grant_rows"] > value["total_rows"]
        or value["grant_requests"] > value["total_requests"]
        or value["build_seconds"] > 1800
    ):
        raise PipelineError("PIPELINE_INVALID_LIMIT")
    if (value["baseline_version"] is None) != (value["baseline_sha256"] is None):
        raise PipelineError("PIPELINE_INVALID_BASELINE")
    if value["baseline_version"] is not None and not all(
        isinstance(value[k], str) and CHECKSUM.fullmatch(value[k])
        for k in ("baseline_version", "baseline_sha256")
    ):
        raise PipelineError("PIPELINE_INVALID_BASELINE")
    if value["mode"] == "demo":
        if value["fixture_id"] not in ("demo-a", "demo-b", "demo-rejected"):
            raise PipelineError("PIPELINE_INVALID_FIXTURE")
        if (value["signing_from"], value["signing_to"]) != ("2026-01-01", "2026-02-01"):
            raise PipelineError("PIPELINE_INVALID_FIXTURE_SCOPE")
    elif value["fixture_id"] is not None:
        raise PipelineError("PIPELINE_INVALID_FIXTURE")
    return dict(value)


def execution_id(dag_id: str, run_id: str) -> str:
    return hashlib.sha256((dag_id + "\0" + run_id).encode()).hexdigest()[:32]


def contained(root: Path, *parts: str) -> Path:
    path = root.joinpath(*parts)
    if not path.resolve().is_relative_to(root.resolve()):
        raise PipelineError("PIPELINE_UNSAFE_PATH")
    return path


@dataclass(frozen=True)
class Runtime:
    project: Path
    state: Path
    bronze: Path
    silver: Path
    reports: Path
    mode: str

    @classmethod
    def environment(cls) -> Runtime:
        project = Path(__file__).resolve().parents[3]
        return cls(
            project,
            Path(os.environ.get("SECOP_PIPELINE_ROOT", "data/orchestration")),
            Path(os.environ.get("BRONZE_ROOT", "data/bronze/secop_contracts")),
            Path(os.environ.get("SILVER_ROOT", "data/silver/secop_contracts")),
            Path(os.environ.get("SECOP_REPORT_ROOT", "reports/orchestration")),
            os.environ.get("SECOP_RUNTIME_MODE", "unconfigured"),
        )

    def check(self, mode: str) -> None:
        if self.mode != mode:
            raise PipelineError("PIPELINE_RUNTIME_MODE_MISMATCH")
        roots = (self.state, self.bronze, self.silver, self.reports)
        resolved = [p.resolve() for p in roots]
        if len(set(resolved)) != 4 or any(
            a.is_relative_to(b) or b.is_relative_to(a)
            for i, a in enumerate(resolved)
            for b in resolved[i + 1 :]
        ):
            raise PipelineError("PIPELINE_ROOT_OVERLAP")
        for root in roots:
            if root.is_symlink():
                raise PipelineError("PIPELINE_UNSAFE_PATH")
