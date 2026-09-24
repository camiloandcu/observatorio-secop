"""Resolve and verify committed Bronze runs before Spark transformation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from observatorio_secop.processing.silver.errors import BronzeEvidenceError
from observatorio_secop.processing.silver.models import BronzePage, VerifiedBronzeRun
from observatorio_secop.processing.silver.schemas import BRONZE_FIELDS, BRONZE_SCHEMA


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise BronzeEvidenceError(f"Could not read Bronze evidence {path}: {error}") from error
    return digest.hexdigest()


def resolve_bronze_runs(
    bronze_root: Path,
    *,
    dataset_id: str,
    contract_sha256: str,
    run_ids: tuple[str, ...] = (),
) -> tuple[VerifiedBronzeRun, ...]:
    runs_root = bronze_root / "runs"
    if run_ids:
        if len(run_ids) != len(set(run_ids)):
            raise BronzeEvidenceError("Bronze run identifiers must be unique")
        candidates = [runs_root / run_id for run_id in sorted(run_ids)]
    elif runs_root.exists():
        candidates = sorted(path for path in runs_root.iterdir() if path.is_dir())
    else:
        candidates = []
    verified = []
    for directory in candidates:
        manifest_path = directory / "manifest.json"
        if not manifest_path.exists():
            if run_ids:
                raise BronzeEvidenceError(f"Bronze run {directory.name} has no manifest")
            continue
        manifest = _load_manifest(manifest_path)
        if manifest.get("status") != "committed":
            if run_ids:
                raise BronzeEvidenceError(
                    f"Bronze run {directory.name} is {manifest.get('status')!r}, not committed"
                )
            continue
        if manifest.get("run_id") != directory.name:
            raise BronzeEvidenceError(f"Bronze run directory does not match manifest: {directory}")
        if manifest.get("dataset_id") != dataset_id:
            raise BronzeEvidenceError(f"Bronze run {directory.name} uses another dataset")
        if manifest.get("contract_sha256") != contract_sha256:
            raise BronzeEvidenceError(f"Bronze run {directory.name} uses another source contract")
        pages = tuple(_verify_page(directory, directory.name, item) for item in manifest["pages"])
        written = manifest.get("counts", {}).get("written")
        if written != sum(page.row_count for page in pages):
            raise BronzeEvidenceError(
                f"Bronze run {directory.name} written count does not match its pages"
            )
        verified.append(
            VerifiedBronzeRun(
                run_id=directory.name,
                dataset_id=dataset_id,
                contract_sha256=contract_sha256,
                manifest_path=manifest_path,
                manifest_sha256=sha256_file(manifest_path),
                pages=pages,
            )
        )
    if run_ids and len(verified) != len(run_ids):
        raise BronzeEvidenceError("Not every requested Bronze run is committed and verifiable")
    return tuple(verified)


def read_bronze_dataframe(spark: SparkSession, runs: tuple[VerifiedBronzeRun, ...]) -> DataFrame:
    records: list[tuple[Any, ...]] = []
    for run in runs:
        for page in run.pages:
            rows = _load_page_rows(page)
            for ordinal, raw in enumerate(rows):
                projected = {field: raw.get(field) for field in BRONZE_FIELDS}
                projected_text = {field: _source_text(projected[field]) for field in BRONZE_FIELDS}
                payload_json = json.dumps(
                    projected, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                records.append(
                    tuple(projected_text[field] for field in BRONZE_FIELDS)
                    + (
                        run.run_id,
                        page.relative_path,
                        page.lane,
                        page.sequence,
                        ordinal,
                        page.fetched_at_utc,
                        payload_json,
                    )
                )
    return spark.createDataFrame(records, BRONZE_SCHEMA)


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BronzeEvidenceError(f"Invalid Bronze manifest {path}: {error}") from error
    if not isinstance(raw, dict) or not isinstance(raw.get("pages"), list):
        raise BronzeEvidenceError(f"Bronze manifest {path} lacks a pages list")
    return raw


def _verify_page(directory: Path, run_id: str, raw: Any) -> BronzePage:
    if not isinstance(raw, dict):
        raise BronzeEvidenceError(f"Bronze run {run_id} has an invalid page record")
    try:
        relative = raw["path"]
        path = directory / relative
        page = BronzePage(
            run_id=run_id,
            path=path,
            relative_path=relative,
            lane=raw["lane"],
            sequence=int(raw["sequence"]),
            fetched_at_utc=raw["fetched_at_utc"],
            sha256=raw["sha256"],
            row_count=int(raw["row_count"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise BronzeEvidenceError(f"Bronze run {run_id} has an invalid page record") from error
    if path.parent.resolve() != (directory / "pages").resolve():
        raise BronzeEvidenceError(f"Bronze page escapes its run directory: {relative}")
    if sha256_file(path) != page.sha256:
        raise BronzeEvidenceError(f"Bronze page hash mismatch: {run_id}/{relative}")
    rows = _load_page_rows(page)
    if len(rows) != page.row_count:
        raise BronzeEvidenceError(f"Bronze page row count mismatch: {run_id}/{relative}")
    return page


def _load_page_rows(page: BronzePage) -> list[dict[str, Any]]:
    try:
        raw = json.loads(page.path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BronzeEvidenceError(f"Invalid Bronze page {page.path}: {error}") from error
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise BronzeEvidenceError(f"Bronze page {page.path} must be an array of objects")
    return raw


def _source_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
