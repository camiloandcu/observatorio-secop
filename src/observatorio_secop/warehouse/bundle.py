"""Resolve and verify the immutable Silver bundle selected by current.json."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.dataset as ds

from observatorio_secop.warehouse.errors import SilverBundleError

EXPECTED_FIELDS: tuple[tuple[str, pa.DataType, bool], ...] = (
    ("silver_version_id", pa.string(), False),
    ("contract_key", pa.string(), False),
    ("source_dataset_id", pa.string(), False),
    ("source_contract_id", pa.string(), False),
    ("contract_reference", pa.string(), True),
    ("procurement_process_id", pa.string(), True),
    ("entity_name", pa.string(), True),
    ("department_raw", pa.string(), False),
    ("department_name", pa.string(), False),
    ("department_key", pa.string(), False),
    ("municipality_raw", pa.string(), True),
    ("municipality_name", pa.string(), True),
    ("municipality_key", pa.string(), True),
    ("municipality_status", pa.string(), False),
    ("is_medellin", pa.bool_(), False),
    ("is_valle_de_aburra", pa.bool_(), False),
    ("contract_status", pa.string(), True),
    ("main_category_code", pa.string(), True),
    ("process_description", pa.string(), True),
    ("contract_type", pa.string(), True),
    ("procurement_method", pa.string(), True),
    ("signing_date_raw", pa.string(), True),
    ("signing_date", pa.date32(), True),
    ("start_date_raw", pa.string(), True),
    ("start_date", pa.date32(), True),
    ("end_date_raw", pa.string(), True),
    ("end_date", pa.date32(), True),
    ("contract_value_raw", pa.string(), False),
    ("contract_value_cop", pa.decimal128(20, 2), False),
    ("source_updated_at_raw", pa.string(), True),
    ("source_updated_at", pa.timestamp("us"), True),
    ("source_url", pa.string(), True),
    ("quality_warning_codes", pa.list_(pa.string()), False),
    ("payload_sha256", pa.string(), False),
    ("bronze_run_id", pa.string(), False),
    ("bronze_page_path", pa.string(), False),
    ("bronze_lane", pa.string(), False),
    ("bronze_page_sequence", pa.int32(), False),
    ("bronze_row_ordinal", pa.int64(), False),
    ("bronze_fetched_at_utc", pa.timestamp("us"), False),
    ("signing_year", pa.string(), False),
)
EXPECTED_SCHEMA = pa.schema(
    [pa.field(name, kind, nullable) for name, kind, nullable in EXPECTED_FIELDS]
)


@dataclass(frozen=True)
class VerifiedSilverBundle:
    version_id: str
    manifest_path: Path
    manifest_sha256: str
    manifest_finished_at: datetime
    row_count: int
    table: pa.Table


def verify_current_bundle(root: Path) -> VerifiedSilverBundle:
    current_path = root / "current.json"
    current = _read_object(current_path, "Silver current pointer")
    version_id = _non_empty_text(current.get("silver_version_id"), "silver_version_id")
    expected_manifest = (root / "versions" / version_id / "manifest.json").resolve()
    manifest_value = current.get("manifest")
    manifest_path = (root / _non_empty_text(manifest_value, "manifest")).resolve()
    if manifest_path != expected_manifest or not manifest_path.is_relative_to(root.resolve()):
        raise SilverBundleError("Silver current pointer does not target its immutable version")
    manifest_sha256 = _sha256_path(manifest_path)
    if current.get("manifest_sha256") != manifest_sha256:
        raise SilverBundleError("Silver current manifest hash does not match its pointer")

    manifest = _read_object(manifest_path, "Silver manifest")
    if manifest.get("silver_version_id") != version_id or manifest.get("status") != "valid":
        raise SilverBundleError("Silver manifest identity or status is invalid")
    if any(
        not item.get("passed", False)
        for item in manifest.get("quality_results", [])
        if item.get("severity") == "critical"
    ):
        raise SilverBundleError("Silver manifest contains a failed critical control")
    _verify_inventory(manifest_path.parent, manifest.get("files"))

    expected_rows = _non_negative_integer(
        _mapping(manifest.get("counts"), "counts").get("output_rows"), "output_rows"
    )
    finished_at = _timestamp(manifest.get("finished_at_utc"), "finished_at_utc")
    table = _read_table(manifest_path.parent / "data")
    _verify_table(table, version_id, expected_rows)
    return VerifiedSilverBundle(
        version_id=version_id,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
        manifest_finished_at=finished_at,
        row_count=expected_rows,
        table=table,
    )


def _read_table(data_path: Path) -> pa.Table:
    files = sorted(data_path.rglob("*.parquet")) if data_path.is_dir() else []
    if not files:
        raise SilverBundleError("Published Silver data contains no Parquet files")
    try:
        partitioning = ds.partitioning(
            pa.schema([pa.field("signing_year", pa.string())]), flavor="hive"
        )
        return ds.dataset(data_path, format="parquet", partitioning=partitioning).to_table()
    except (OSError, pa.ArrowException) as error:
        raise SilverBundleError(f"Could not read published Silver Parquet: {error}") from error


def _verify_table(table: pa.Table, version_id: str, expected_rows: int) -> None:
    observed = [(field.name, field.type) for field in table.schema]
    expected = [(field.name, field.type) for field in EXPECTED_SCHEMA]
    if observed != expected:
        raise SilverBundleError(f"Silver Parquet schema differs: {table.schema}")
    if table.num_rows != expected_rows:
        raise SilverBundleError("Silver Parquet row count differs from the manifest")
    null_fields = [
        field.name
        for field in EXPECTED_SCHEMA
        if not field.nullable and table.column(field.name).null_count
    ]
    if null_fields:
        raise SilverBundleError(f"Silver required columns contain nulls: {null_fields}")
    rows = table.select(["silver_version_id", "contract_key", "source_contract_id"])
    versions = set(rows.column("silver_version_id").to_pylist())
    keys = rows.column("contract_key").to_pylist()
    identifiers = rows.column("source_contract_id").to_pylist()
    if versions != {version_id}:
        raise SilverBundleError("Silver rows do not belong to the selected version")
    if any(not value for value in keys) or len(keys) != len(set(keys)):
        raise SilverBundleError("Silver contract_key values must be non-null and unique")
    if any(not value for value in identifiers):
        raise SilverBundleError("Silver source_contract_id values must be non-null")


def _verify_inventory(directory: Path, value: Any) -> None:
    if not isinstance(value, list) or not value:
        raise SilverBundleError("Silver manifest file inventory is missing")
    declared: set[str] = set()
    for item in value:
        details = _mapping(item, "files item")
        relative = _non_empty_text(details.get("path"), "files.path")
        path = (directory / relative).resolve()
        if not path.is_relative_to(directory.resolve()) or not path.is_file():
            raise SilverBundleError(f"Silver inventory file is missing: {relative}")
        if relative in declared:
            raise SilverBundleError(f"Silver inventory path is duplicated: {relative}")
        declared.add(relative)
        if path.stat().st_size != _non_negative_integer(details.get("size_bytes"), "size_bytes"):
            raise SilverBundleError(f"Silver inventory size differs: {relative}")
        if _sha256_path(path) != details.get("sha256"):
            raise SilverBundleError(f"Silver inventory hash differs: {relative}")


def _read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SilverBundleError(f"Could not read {label}: {error}") from error
    return _mapping(value, label)


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SilverBundleError(f"{name} must be an object")
    return cast(dict[str, Any], value)


def _non_empty_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SilverBundleError(f"{name} must be non-empty text")
    return value


def _non_negative_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SilverBundleError(f"{name} must be a non-negative integer")
    return int(value)


def _timestamp(value: Any, name: str) -> datetime:
    text = _non_empty_text(value, name)
    try:
        result = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise SilverBundleError(f"{name} must be an ISO-8601 timestamp") from error
    if result.tzinfo is None:
        raise SilverBundleError(f"{name} must include a UTC offset")
    return result


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise SilverBundleError(f"Could not hash Silver file {path.name}: {error}") from error
    return digest.hexdigest()
