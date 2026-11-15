"""Reviewed static projection and portable types; never infer new source columns."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml

from .errors import ExportError

ROOT = Path(__file__).resolve().parents[3]
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*\Z")


@dataclass(frozen=True)
class Column:
    name: str
    source_type: str
    codec: str
    precision: int = 0
    scale: int = 0
    json_policy: str | None = None
    minimization_policy: str | None = None

    @property
    def arrow_type(self) -> pa.DataType:
        if self.codec == "decimal":
            return pa.decimal128(self.precision, self.scale)
        return {
            "text": pa.string(),
            "int16": pa.int16(),
            "int32": pa.int32(),
            "int64": pa.int64(),
            "bool": pa.bool_(),
            "date": pa.date32(),
            "decimal-text-v1": pa.string(),
            "wall-clock-us-v1": pa.string(),
            "utc-instant-us-v1": pa.string(),
            "json-text-v1": pa.string(),
            "array-json-v1": pa.string(),
        }[self.codec]

    @property
    def glue_type(self) -> str:
        if self.codec == "decimal":
            return f"decimal({self.precision},{self.scale})"
        return {
            "int16": "smallint",
            "int32": "int",
            "int64": "bigint",
            "bool": "boolean",
            "date": "date",
        }.get(self.codec, "string")


@dataclass(frozen=True)
class Relation:
    name: str
    columns: tuple[Column, ...]
    partition_by_signing: bool

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([pa.field(c.name, c.arrow_type) for c in self.columns])


@dataclass(frozen=True)
class Contract:
    digest: str
    version: str
    relations: dict[str, Relation]
    policies: dict[str, Any]
    max_json_depth: int
    scalar_max_bytes: int
    url_hosts: tuple[str, ...]


def load_contract() -> Contract:
    data = (ROOT / "contracts/cloud_export.yaml").read_bytes()
    raw = yaml.safe_load(data)
    if raw["version"] != "export-v2" or raw["allow_new_columns"] is not False:
        raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Unsupported export contract")
    relations = {}
    for name, spec in raw["relations"].items():
        columns = tuple(Column(**v) for v in spec["columns"])
        if not IDENTIFIER.fullmatch(name) or len({c.name for c in columns}) != len(columns):
            raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Invalid relation contract")
        for c in columns:
            if not IDENTIFIER.fullmatch(c.name):
                raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Invalid column identifier")
            if c.codec == "decimal" and not 0 <= c.scale <= c.precision <= 38:
                raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Unsupported decimal precision")
            if c.minimization_policy is not None and (
                c.minimization_policy != "supplier-name-null-v1"
                or c.name != "supplier_name"
                or name not in ("dim_supplier", "mart_contract_detail")
            ):
                raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Unreviewed minimization field")
            _ = c.arrow_type
            if c.json_policy and c.json_policy not in raw["json_policies"]:
                raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Missing nested projection policy")
        relations[name] = Relation(name, columns, spec["partition_by_signing"])
    if len(relations) != 13:
        raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Expected thirteen reviewed relations")
    return Contract(
        hashlib.sha256(data).hexdigest(),
        raw["version"],
        relations,
        raw["json_policies"],
        raw["max_json_depth"],
        raw["json_scalar_max_bytes"],
        tuple(raw["url_hosts"]),
    )
