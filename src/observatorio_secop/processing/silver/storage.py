"""Durable staging, verification, publication, and rollback for Silver bundles."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession

from observatorio_secop.processing.silver.errors import PublicationError
from observatorio_secop.processing.silver.models import SilverManifest
from observatorio_secop.processing.silver.schemas import REJECTED_SCHEMA, SILVER_SCHEMA


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
    _fsync_directory(path.parent)


class SilverStorage:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.staging_root = root / "staging"
        self.versions_root = root / "versions"
        self.invalid_root = root / "invalid"
        self.current_path = root / "current.json"

    def staging_path(self, version_id: str) -> Path:
        return self.staging_root / version_id

    def version_path(self, version_id: str) -> Path:
        return self.versions_root / version_id

    def invalid_path(self, version_id: str) -> Path:
        return self.invalid_root / version_id

    def create_staging(self, version_id: str) -> Path:
        target = self.staging_path(version_id)
        if self.version_path(version_id).exists():
            raise PublicationError(f"Silver version {version_id} already exists")
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True)
        return target

    def write_data(
        self,
        valid: DataFrame,
        rejected: DataFrame,
        version_id: str,
        *,
        codec: str,
        partitions: int,
    ) -> Path:
        staging = self.staging_path(version_id)
        valid.coalesce(partitions).write.mode("errorifexists").option(
            "compression", codec
        ).partitionBy("signing_year").parquet(str(staging / "data"))
        rejected.coalesce(partitions).write.mode("errorifexists").option(
            "compression", codec
        ).parquet(str(staging / "rejected"))
        return staging

    def write_quality(self, version_id: str, manifest: SilverManifest) -> None:
        results = [item.as_dict() for item in manifest.quality_results]
        durable_write(
            self.staging_path(version_id) / "quality/results.json",
            canonical_json(results) + b"\n",
        )

    def inventory(self, version_id: str) -> list[dict[str, Any]]:
        staging = self.staging_path(version_id)
        result = []
        for path in sorted(item for item in staging.rglob("*") if item.is_file()):
            if path.name == "manifest.json" or path.name.startswith(".") or path.suffix == ".crc":
                continue
            relative = path.relative_to(staging).as_posix()
            result.append(
                {
                    "path": relative,
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256_path(path),
                }
            )
        return result

    def write_manifest(self, version_id: str, manifest: SilverManifest) -> Path:
        path = self.staging_path(version_id) / "manifest.json"
        durable_write(path, canonical_json(manifest.as_dict()) + b"\n")
        return path

    def verify_staging(
        self,
        spark: SparkSession,
        manifest: SilverManifest,
        *,
        codec: str,
    ) -> None:
        staging = self.staging_path(manifest.silver_version_id)
        if not staging.exists():
            raise PublicationError("Silver staging directory is missing")
        for item in manifest.files:
            path = staging / item["path"]
            if not path.is_file() or _sha256_path(path) != item["sha256"]:
                raise PublicationError(f"Silver staging file failed hash verification: {path}")
        valid = spark.read.schema(SILVER_SCHEMA).parquet(str(staging / "data"))
        rejected = spark.read.schema(REJECTED_SCHEMA).parquet(str(staging / "rejected"))
        _verify_schema(valid, SILVER_SCHEMA, "Silver")
        _verify_schema(rejected, REJECTED_SCHEMA, "rejected")
        if valid.count() != manifest.counts["output_rows"]:
            raise PublicationError("Silver Parquet count differs from the manifest")
        if rejected.count() != manifest.counts["rejected_rows"]:
            raise PublicationError("Rejected Parquet count differs from the manifest")
        partition_values = {
            path.name.split("=", 1)[1]
            for path in (staging / "data").glob("signing_year=*")
            if path.is_dir()
        }
        observed_values = {row[0] for row in valid.select("signing_year").distinct().collect()}
        if partition_values != observed_values:
            raise PublicationError("signing_year partitions differ from the data")
        self._verify_codec(staging, codec)

    def invalidate(self, manifest: SilverManifest) -> Path:
        source = self.staging_path(manifest.silver_version_id)
        target = self.invalid_path(manifest.silver_version_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            shutil.rmtree(target)
        os.replace(source, target)
        _fsync_directory(target.parent)
        return target

    def promote(self, manifest: SilverManifest) -> Path:
        source = self.staging_path(manifest.silver_version_id)
        target = self.version_path(manifest.silver_version_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            existing = self.load_manifest(target)
            if _logical_manifest(existing) != _logical_manifest(manifest.as_dict()):
                raise PublicationError(
                    f"Silver version {manifest.silver_version_id} has conflicting content"
                )
            if source.exists():
                shutil.rmtree(source)
        else:
            os.replace(source, target)
            _fsync_directory(target.parent)
        self._write_current(manifest.silver_version_id, target / "manifest.json")
        return target

    def rollback(self, version_id: str) -> dict[str, Any]:
        target = self.version_path(version_id)
        manifest = self.load_manifest(target)
        if manifest.get("status") != "valid":
            raise PublicationError(f"Silver version {version_id} is not valid")
        self._write_current(version_id, target / "manifest.json")
        return self.current()

    def current(self) -> dict[str, Any]:
        if not self.current_path.exists():
            return {"current": None}
        try:
            value = json.loads(self.current_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise PublicationError(f"Could not read current Silver pointer: {error}") from error
        return value

    def existing_manifest(self, version_id: str) -> dict[str, Any] | None:
        path = self.version_path(version_id)
        return self.load_manifest(path) if path.exists() else None

    def verify_existing(self, version_id: str, manifest: dict[str, Any]) -> None:
        directory = self.version_path(version_id)
        if manifest.get("silver_version_id") != version_id or manifest.get("status") != "valid":
            raise PublicationError(f"Silver version {version_id} has an invalid manifest identity")
        if any(
            not item.get("passed", False)
            for item in manifest.get("quality_results", [])
            if item.get("severity") == "critical"
        ):
            raise PublicationError(f"Silver version {version_id} has failed critical controls")
        for item in manifest.get("files", []):
            path = directory / item.get("path", "")
            if (
                not path.is_file()
                or path.stat().st_size != item.get("size_bytes")
                or _sha256_path(path) != item.get("sha256")
            ):
                raise PublicationError(f"Published Silver file failed verification: {path}")
        current = self.current()
        if current.get("silver_version_id") == version_id:
            manifest_path = directory / "manifest.json"
            if current.get("manifest_sha256") != _sha256_path(manifest_path):
                raise PublicationError("Current Silver manifest hash does not match its pointer")

    def clean_staging(self) -> int:
        if not self.staging_root.exists():
            return 0
        targets = [path for path in self.staging_root.iterdir() if path.is_dir()]
        for path in targets:
            shutil.rmtree(path)
        return len(targets)

    def inspect(self) -> dict[str, Any]:
        return {
            "root": str(self.root),
            "current": self.current().get("silver_version_id"),
            "versions": len(list(self.versions_root.glob("*/manifest.json"))),
            "invalid": len(list(self.invalid_root.glob("*/manifest.json"))),
            "staging": len(list(self.staging_root.glob("*"))),
        }

    def load_manifest(self, directory: Path) -> dict[str, Any]:
        try:
            result = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise PublicationError(
                f"Could not load Silver manifest from {directory}: {error}"
            ) from error
        if not isinstance(result, dict):
            raise PublicationError(f"Silver manifest in {directory} is not an object")
        return result

    def _write_current(self, version_id: str, manifest_path: Path) -> None:
        pointer = {
            "silver_version_id": version_id,
            "manifest": str(manifest_path.relative_to(self.root)),
            "manifest_sha256": _sha256_path(manifest_path),
        }
        durable_write(self.current_path, canonical_json(pointer) + b"\n")

    @staticmethod
    def _verify_codec(staging: Path, codec: str) -> None:
        expected = codec.upper()
        for path in sorted(staging.rglob("*.parquet")):
            metadata = pq.ParquetFile(path).metadata
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                for column_index in range(row_group.num_columns):
                    if row_group.column(column_index).compression != expected:
                        raise PublicationError(
                            f"Parquet file {path} is not compressed with {codec}"
                        )


def _verify_schema(frame: DataFrame, expected: Any, label: str) -> None:
    observed = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
    required = [(field.name, field.dataType.simpleString()) for field in expected.fields]
    if observed != required:
        raise PublicationError(f"{label} Parquet schema differs: {observed!r} != {required!r}")


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _logical_manifest(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item for key, item in value.items() if key not in {"started_at_utc", "finished_at_utc"}
    }


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
