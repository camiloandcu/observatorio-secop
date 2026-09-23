"""Durable local Bronze page and manifest storage."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Any

from observatorio_secop.ingestion.errors import ResumeError
from observatorio_secop.ingestion.models import PageRecord, RunManifest


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def durable_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class BronzeStorage:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.staging_root = root / "staging"
        self.runs_root = root / "runs"
        self.state_root = root / "state"
        self.locks_root = root / "locks"

    def staging_path(self, run_id: str) -> Path:
        return self.staging_root / run_id

    def run_path(self, run_id: str) -> Path:
        return self.runs_root / run_id

    def create_staging(self, manifest: RunManifest) -> Path:
        target = self.staging_path(manifest.run_id)
        if target.exists() or self.run_path(manifest.run_id).exists():
            raise ResumeError(f"Run {manifest.run_id} already exists")
        (target / "pages").mkdir(parents=True)
        self.write_page_state(manifest.run_id, manifest)
        return target

    def write_page(self, run_id: str, lane: str, sequence: int, raw: bytes) -> tuple[str, str]:
        relative = f"pages/{lane}-{sequence:05d}.json"
        path = self.staging_path(run_id) / relative
        durable_write(path, raw)
        return relative, sha256_bytes(raw)

    def write_page_state(self, run_id: str, manifest: RunManifest) -> None:
        path = self.staging_path(run_id) / "page-state.json"
        durable_write(path, canonical_json(manifest.as_dict()) + b"\n")

    def load_staging(self, run_id: str) -> RunManifest:
        path = self.staging_path(run_id) / "page-state.json"
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ResumeError(f"Could not load staging for {run_id}: {error}") from error
        raw["pages"] = [PageRecord(**page) for page in raw.get("pages", [])]
        manifest = RunManifest(**raw)
        self.verify_pages(self.staging_path(run_id), manifest)
        return manifest

    def verify_pages(self, directory: Path, manifest: RunManifest) -> None:
        for page in manifest.pages:
            path = directory / page.path
            try:
                content = path.read_bytes()
                rows = json.loads(content)
            except (OSError, json.JSONDecodeError) as error:
                raise ResumeError(f"Page {page.path} is missing or invalid: {error}") from error
            if sha256_bytes(content) != page.sha256:
                raise ResumeError(f"Page {page.path} hash does not match staging state")
            if not isinstance(rows, list) or len(rows) != page.row_count:
                raise ResumeError(f"Page {page.path} row count does not match staging state")

    def prepare(self, manifest: RunManifest) -> Path:
        manifest.status = "prepared"
        staging = self.staging_path(manifest.run_id)
        self.verify_pages(staging, manifest)
        durable_write(staging / "manifest.json", canonical_json(manifest.as_dict()) + b"\n")
        final = self.run_path(manifest.run_id)
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            raise ResumeError(f"Published run {manifest.run_id} already exists")
        os.replace(staging, final)
        return final

    def mark_committed(self, manifest: RunManifest) -> None:
        manifest.status = "committed"
        durable_write(
            self.run_path(manifest.run_id) / "manifest.json",
            canonical_json(manifest.as_dict()) + b"\n",
        )

    def load_published(self, path: Path) -> RunManifest:
        try:
            raw = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ResumeError(f"Could not load published run {path.name}: {error}") from error
        raw["pages"] = [PageRecord(**page) for page in raw.get("pages", [])]
        manifest = RunManifest(**raw)
        self.verify_pages(path, manifest)
        return manifest

    def prepared_runs(self) -> list[Path]:
        if not self.runs_root.exists():
            return []
        result = []
        for path in sorted(item for item in self.runs_root.iterdir() if item.is_dir()):
            try:
                state = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if state.get("status") == "prepared":
                result.append(path)
        return result

    def clean_staging(self) -> int:
        if not self.staging_root.exists():
            return 0
        targets = [path for path in self.staging_root.iterdir() if path.is_dir()]
        for path in targets:
            shutil.rmtree(path)
        return len(targets)

    def inspect(self) -> dict[str, Any]:
        staging = len(list(self.staging_root.glob("*/page-state.json")))
        runs = len(list(self.runs_root.glob("*/manifest.json")))
        return {"root": str(self.root), "staging_runs": staging, "published_runs": runs}


def page_to_dict(page: PageRecord) -> dict[str, Any]:
    return asdict(page)
