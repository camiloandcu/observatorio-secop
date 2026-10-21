"""Resolve and verify the immutable Silver bundle selected by current.json."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from observatorio_secop.identity import (
    IDENTITY_RULE_VERSION,
    IDENTITY_TEXT_FIELDS,
    LEGACY_PROJECTION,
    REQUIRED_IDENTITY_FIELDS,
    identity_record_valid,
    key_set_digest,
    load_identity_rules,
)
from observatorio_secop.ingestion.compatibility import CompatibilityRegistry
from observatorio_secop.ingestion.errors import SourceContractError
from observatorio_secop.warehouse.errors import SilverBundleError
from observatorio_secop.warehouse.inventory import sha256_path, verify_inventory
from observatorio_secop.warehouse.timestamps import TIMESTAMP_COLUMNS, normalize_timestamps

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
IDENTITY_SCHEMA = pa.schema(
    [
        *list(EXPECTED_SCHEMA)[:-1],
        *[
            pa.field(name, pa.string(), name not in REQUIRED_IDENTITY_FIELDS)
            for name in IDENTITY_TEXT_FIELDS
        ],
        list(EXPECTED_SCHEMA)[-1],
    ]
)


@dataclass(frozen=True)
class VerifiedSilverBundle:
    version_id: str
    manifest_path: Path
    manifest_sha256: str
    manifest_finished_at: datetime
    row_count: int
    table: pa.Table
    schema_version: int = 1
    coverage: dict[str, Any] | None = None
    reconciliation: dict[str, Any] | None = None
    condition_summaries: list[dict[str, Any]] = field(default_factory=list)
    condition_links: list[dict[str, Any]] = field(default_factory=list)
    selection_mode: str | None = None


def verify_current_bundle(root: Path) -> VerifiedSilverBundle:
    current_path = root / "current.json"
    current = _read_object(current_path, "Silver current pointer")
    version_id = _non_empty_text(current.get("silver_version_id"), "silver_version_id")
    expected_manifest = (root / "versions" / version_id / "manifest.json").resolve()
    manifest_value = current.get("manifest")
    manifest_path = (root / _non_empty_text(manifest_value, "manifest")).resolve()
    if manifest_path != expected_manifest or not manifest_path.is_relative_to(root.resolve()):
        raise SilverBundleError("Silver current pointer does not target its immutable version")
    manifest_sha256 = sha256_path(manifest_path)
    if current.get("manifest_sha256") != manifest_sha256:
        raise SilverBundleError("Silver current manifest hash does not match its pointer")

    return verify_manifest_bundle(manifest_path, expected_hash=manifest_sha256)


def verify_manifest_bundle(
    manifest_path: Path,
    *,
    expected_hash: str | None = None,
    verify_refresh: bool = True,
    _seen: frozenset[Path] = frozenset(),
) -> VerifiedSilverBundle:
    resolved = manifest_path.resolve()
    if resolved in _seen:
        raise SilverBundleError("Silver population dependency cycle")
    ancestors = _seen | {resolved}
    manifest_sha256 = sha256_path(manifest_path)
    if expected_hash is not None and expected_hash != manifest_sha256:
        raise SilverBundleError("Silver manifest hash differs from pinned evidence")
    manifest = _read_object(manifest_path, "Silver manifest")
    version_id = manifest_path.parent.name
    if manifest_path.name != "manifest.json":
        raise SilverBundleError("Silver evidence must identify an immutable version manifest")
    if manifest.get("silver_version_id") != version_id or manifest.get("status") != "valid":
        raise SilverBundleError("Silver manifest identity or status is invalid")
    if any(
        not item.get("passed", False)
        for item in manifest.get("quality_results", [])
        if item.get("severity") == "critical"
    ):
        raise SilverBundleError("Silver manifest contains a failed critical control")
    schema_version = manifest.get("schema_version", 1)
    if type(schema_version) is not int or schema_version not in (1, 2, 3):
        raise SilverBundleError("Unsupported Silver manifest schema_version")
    if schema_version == 3 and manifest.get("row_schema_version") != 2:
        raise SilverBundleError("Population envelope requires explicit contract row schema 2")
    if schema_version != 3 and manifest.get("population") is not None:
        raise SilverBundleError("Population metadata requires explicit envelope 3")
    data_files = verify_inventory(manifest_path.parent, manifest.get("files"))

    expected_rows = _non_negative_integer(
        _mapping(manifest.get("counts"), "counts").get("output_rows"), "output_rows"
    )
    finished_at = _timestamp(manifest.get("finished_at_utc"), "finished_at_utc")
    table = _read_table(manifest_path.parent / "data", data_files)
    _verify_table(
        table,
        version_id,
        expected_rows,
        IDENTITY_SCHEMA if schema_version in (2, 3) else EXPECTED_SCHEMA,
    )
    if schema_version == 1:
        for field in IDENTITY_SCHEMA:
            if field.name not in table.column_names:
                value = {
                    "entity_identity_status": "UNKNOWN",
                    "supplier_identity_status": "UNKNOWN",
                    "entity_identity_reason": "LEGACY_PROJECTION",
                    "supplier_identity_reason": "LEGACY_PROJECTION",
                    "identity_rule_version": IDENTITY_RULE_VERSION,
                    "source_contract_sha256": manifest.get("contract_sha256"),
                    "source_projection_version": LEGACY_PROJECTION,
                }.get(field.name)
                table = table.append_column(
                    field, pa.array([value] * expected_rows, type=field.type)
                )
        table = table.select(IDENTITY_SCHEMA.names)
    else:
        _verify_identity(table, manifest)
    coverage = reconciliation = None
    summaries: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []
    if schema_version == 3:
        from observatorio_secop.warehouse.population_bundle import verify_population_bundle

        def verify_related(path: Path, checksum: str) -> VerifiedSilverBundle:
            return verify_manifest_bundle(path, expected_hash=checksum, _seen=ancestors)

        coverage, reconciliation, summaries, links = verify_population_bundle(
            table, manifest, manifest_path, verify_related
        )
    elif verify_refresh:
        _verify_refresh(table, manifest, manifest_path)
    return VerifiedSilverBundle(
        version_id=version_id,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha256,
        manifest_finished_at=finished_at,
        row_count=expected_rows,
        table=table,
        schema_version=schema_version,
        coverage=coverage,
        reconciliation=reconciliation,
        condition_summaries=summaries,
        condition_links=links,
        selection_mode=manifest.get("selection_mode"),
    )


def _read_table(data_path: Path, files: tuple[Path, ...]) -> pa.Table:
    try:
        partitioning = ds.partitioning(
            pa.schema([pa.field("signing_year", pa.string())]), flavor="hive"
        )
        tables = []
        for path in files:
            # Read each physical schema before combining: a dataset-wide target can
            # silently coerce later fragments to the first fragment's precision.
            table = ds.dataset(
                [str(path)],
                format="parquet",
                partitioning=partitioning,
                partition_base_dir=str(data_path),
            ).to_table()
            parquet = pq.ParquetFile(path)
            legacy_names = [
                parquet.schema.column(i).path
                for i in range(len(parquet.schema))
                if parquet.schema.column(i).physical_type == "INT96"
                and parquet.schema.column(i).path in TIMESTAMP_COLUMNS
            ]
            reference = (
                pq.ParquetFile(path, coerce_int96_timestamp_unit="us").read(columns=legacy_names)
                if legacy_names
                else None
            )
            tables.append(normalize_timestamps(table, reference))
        return pa.concat_tables(tables)
    except (OSError, pa.ArrowException) as error:
        raise SilverBundleError(f"Could not read published Silver Parquet: {error}") from error


def _verify_table(
    table: pa.Table, version_id: str, expected_rows: int, schema: pa.Schema = EXPECTED_SCHEMA
) -> None:
    observed = [(field.name, field.type) for field in table.schema]
    expected = [(field.name, field.type) for field in schema]
    if observed != expected:
        raise SilverBundleError(f"Silver Parquet schema differs: {table.schema}")
    if table.num_rows != expected_rows:
        raise SilverBundleError("Silver Parquet row count differs from the manifest")
    null_fields = [
        field.name for field in schema if not field.nullable and table.column(field.name).null_count
    ]
    if null_fields:
        raise SilverBundleError(f"Silver required columns contain nulls: {null_fields}")
    rows = table.select(["silver_version_id", "contract_key", "source_contract_id"])
    versions = set(rows.column("silver_version_id").to_pylist())
    keys = rows.column("contract_key").to_pylist()
    identifiers = rows.column("source_contract_id").to_pylist()
    if versions != ({version_id} if expected_rows else set()):
        raise SilverBundleError("Silver rows do not belong to the selected version")
    if any(not value for value in keys) or len(keys) != len(set(keys)):
        raise SilverBundleError("Silver contract_key values must be non-null and unique")
    if any(not value for value in identifiers):
        raise SilverBundleError("Silver source_contract_id values must be non-null")


def _verify_identity(table: pa.Table, manifest: dict[str, Any]) -> None:
    try:
        rules = load_identity_rules()
        registry = CompatibilityRegistry()
        profiles = {(p.contract_sha256, p.projection_version) for p in registry.profiles}
        if (
            manifest.get("identity_rule_version") != IDENTITY_RULE_VERSION
            or manifest.get("compatibility_sha256") != registry.digest
            or manifest.get("dataset_id") != "jbjy-vk9h"
        ):
            raise ValueError("Silver identity rule/compatibility metadata differs")
        purposes = manifest.get("input_purposes")
        if (
            not isinstance(purposes, dict)
            or set(purposes) != {run["run_id"] for run in manifest.get("bronze_runs", [])}
            or any(
                value
                not in (
                    ("incremental", "identity_refresh", "population_reconciliation")
                    if manifest.get("schema_version") == 3
                    else ("incremental", "identity_refresh")
                )
                for value in purposes.values()
            )
            or (
                "identity_refresh" in purposes.values()
                and not manifest.get("refresh")
                and manifest.get("schema_version") != 3
            )
        ):
            raise ValueError("Silver identity refresh input-purpose evidence is missing or invalid")
        for row in table.select(
            [*IDENTITY_TEXT_FIELDS, "source_dataset_id", "bronze_run_id"]
        ).to_pylist():
            if row["source_dataset_id"] != "jbjy-vk9h" or row["bronze_run_id"] not in purposes:
                raise ValueError("Silver identity dataset or winning Bronze lineage differs")
            if not identity_record_valid(row, rules):
                raise ValueError("Silver identity code/status/reason is inconsistent")
            if (row["source_contract_sha256"], row["source_projection_version"]) not in profiles:
                raise ValueError("Silver source profile is not registered")
    except (OSError, ValueError, KeyError, TypeError, SourceContractError) as error:
        raise SilverBundleError(str(error)) from error


def _verify_refresh(table: pa.Table, manifest: dict[str, Any], manifest_path: Path) -> None:
    refresh = manifest.get("refresh")
    if refresh is None:
        return
    if not isinstance(refresh, dict) or refresh.get("complete") is not True:
        raise SilverBundleError("Silver identity refresh evidence is incomplete")
    baseline_id = _non_empty_text(refresh.get("baseline_version_id"), "refresh baseline")
    baseline_path = manifest_path.parent.parent / baseline_id / "manifest.json"
    if (
        baseline_id == manifest_path.parent.name
        or baseline_path.resolve().parent.parent != manifest_path.resolve().parent.parent
    ):
        raise SilverBundleError("Invalid identity refresh baseline reference")
    baseline = verify_manifest_bundle(
        baseline_path,
        expected_hash=_non_empty_text(
            refresh.get("baseline_manifest_sha256"), "refresh baseline hash"
        ),
        verify_refresh=False,
    )
    actual = key_set_digest(table.column("contract_key").to_pylist())
    expected = key_set_digest(baseline.table.column("contract_key").to_pylist())
    baseline_manifest = _read_object(baseline_path, "refresh baseline")
    if (
        actual != expected
        or refresh.get("baseline_keys_sha256") != expected
        or refresh.get("candidate_keys_sha256") != actual
        or refresh.get("baseline_bronze_runs") != baseline_manifest.get("bronze_runs")
    ):
        raise SilverBundleError(
            "Identity refresh baseline/dependency/key-set reconciliation differs"
        )
    inputs = {(run["run_id"], run["manifest_sha256"]) for run in manifest.get("bronze_runs", [])}
    if not all(
        (run["run_id"], run["manifest_sha256"]) in inputs
        for run in baseline_manifest.get("bronze_runs", [])
    ):
        raise SilverBundleError("Identity refresh omits baseline Bronze dependencies")
    summaries = refresh.get("refresh_runs")
    if not isinstance(summaries, list) or not summaries:
        raise SilverBundleError("Identity refresh completion summaries are missing")
    if any(
        item.get("complete") is not True
        or item.get("truncated") is not False
        or (item.get("run_id"), item.get("manifest_sha256")) not in inputs
        for item in summaries
    ):
        raise SilverBundleError("Identity refresh completion summary is invalid")
    if {item["run_id"] for item in summaries} != {
        key for key, purpose in manifest["input_purposes"].items() if purpose == "identity_refresh"
    }:
        raise SilverBundleError("Identity refresh summaries differ from declared input purposes")


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
