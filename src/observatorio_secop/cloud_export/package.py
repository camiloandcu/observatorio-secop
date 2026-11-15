"""Private deterministic Parquet packages with complete inventories and verification."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .budget import Budget
from .codecs import validate_stored
from .contract import Contract, Relation
from .ddl import ddl
from .errors import ExportError
from .identity import export_identity
from .logical import LogicalDigest
from .minimization import KEYS, validate_policy
from .package_json import (
    canonical_json as canonical_json,
)  # re-export for existing internal consumers
from .package_json import parse_object
from .reconciliation import reconciliation_sql

LIMITATIONS = [
    "Private bounded export. Exact codecs retain upstream meaning.",
    "Logical multiset reconciliation relies on SHA-256 collision resistance.",
    "Cloud execution and billing are not verified by a local package.",
    "Source comparison follows the declared approved minimization projection.",
]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def partition(row: dict[str, Any], relation: Relation) -> str:
    if not relation.partition_by_signing:
        return ""
    value = row["signing_date"]
    year, month = (f"{value.year:04}", f"{value.month:02}") if value else ("unknown", "unknown")
    return f"signing_year_partition={year}/signing_month_partition={month}"


class RelationWriter:
    def __init__(self, root: Path, relation: Relation, budget: Budget):
        self.root, self.relation, self.budget = root, relation, budget
        self.digest = LogicalDigest(relation)
        self.writers: dict[str, Any] = {}

    def add(self, rows: list[dict[str, Any]]) -> None:
        self.budget.admit(self.digest.add(rows))
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            groups.setdefault(partition(row, self.relation), []).append(row)
        for key, values in groups.items():
            if key not in self.writers:
                self._open(key)
            self.budget.check()
            table = pa.Table.from_pylist(values, schema=self.relation.schema)
            self.budget.check()
            self.writers[key].write_table(table, row_group_size=1000)
            self.budget.files(self.root)

    def _open(self, key: str) -> None:
        path = self.root / self.relation.name / key / "part-00000.parquet"
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        # Staging is exclusively owned by this attempt; never open a destination.
        self.writers[key] = pq.ParquetWriter(
            path,
            self.relation.schema,
            compression="snappy",
            version="2.6",
            use_dictionary=False,
            write_statistics=True,
        )
        path.chmod(0o600)

    def close(self) -> dict[str, Any]:
        if not self.writers:
            self._open(
                "signing_year_partition=unknown/signing_month_partition=unknown"
                if self.relation.partition_by_signing
                else ""
            )
        for writer in self.writers.values():
            writer.close()
        self.budget.files(self.root)
        return self.digest.summary()

    def abort(self) -> None:
        for writer in self.writers.values():
            writer.close()


def inventory(root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": p.relative_to(root).as_posix(),
            "size_bytes": p.stat().st_size,
            "sha256": sha256(p),
        }
        for p in sorted(root.rglob("*"))
        if p.is_file() and p != root / "manifest.json"
    ]


def verify(
    root: Path, contract: Contract, budget: Budget | None = None, *, allow_staging: bool = False
) -> dict[str, Any]:
    budget = budget or Budget()
    if root.is_symlink() or not root.is_dir() or not (root / "manifest.json").is_file():
        raise ExportError("EXPORT_INCOMPLETE", "Complete package manifest is missing")
    if any(p.is_symlink() for p in [root, *root.rglob("*")]):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Package contains a symlink")
    manifest = parse_object((root / "manifest.json").read_bytes())
    allowed_fields = {
        "version",
        "export_id",
        "publication_id",
        "writer",
        "contract_sha256",
        "binding",
        "schemas",
        "relations",
        "files",
        "logical_digest",
        "exact_source_comparison",
        "foreign_key_checks",
        "limitations",
        "minimization",
    }
    if set(manifest) != allowed_fields:
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Manifest fields differ from the allowlist")
    if manifest["logical_digest"] != "logical-digest-v1" or manifest["limitations"] != LIMITATIONS:
        raise ExportError(
            "EXPORT_PACKAGE_CONFLICT", "Manifest semantics differ from the reviewed contract"
        )
    if (
        manifest.get("version") != contract.version
        or manifest.get("contract_sha256") != contract.digest
        or manifest.get("schemas") != schemas(contract)
        or set(manifest.get("relations", {})) != set(contract.relations)
    ):
        raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Package contract differs")
    binding = manifest.get("binding")
    writer = manifest.get("writer")
    if (
        not isinstance(binding, dict)
        or set(binding) != {"silver_version_id", "silver_manifest_sha256", "scope_id"}
        or not all(
            isinstance(binding[k], str) and re.fullmatch("[0-9a-f]{64}", binding[k])
            for k in ("silver_version_id", "silver_manifest_sha256")
        )
        or (
            binding["scope_id"] is not None
            and not re.fullmatch("[0-9a-f]{64}", str(binding["scope_id"]))
        )
        or not re.fullmatch("[0-9a-f]{32}", str(manifest.get("publication_id", "")))
        or not isinstance(writer, dict)
        or set(writer)
        != {"revision", "pyarrow", "compression", "parquet_version", "row_group_size", "dictionary"}
        or not re.fullmatch("[0-9a-f]{64}", str(writer.get("revision", "")))
        or not re.fullmatch(r"[0-9]+[.][0-9]+[.][0-9]+", str(writer.get("pyarrow", "")))
        or writer.get("compression") != "snappy"
        or writer.get("parquet_version") != "2.6"
        or writer.get("row_group_size") != "1000"
        or writer.get("dictionary") != "false"
        or manifest.get("exact_source_comparison") != {"relations": 13, "mismatches": 0}
        or type(manifest.get("foreign_key_checks")) is not int
        or manifest["foreign_key_checks"] < 0
    ):
        raise ExportError(
            "EXPORT_INPUT_CONFLICT", "Package identity or verification metadata is incomplete"
        )
    validate_policy(manifest["minimization"])
    if manifest.get("export_id") != export_identity(
        manifest["publication_id"], binding, contract.digest, writer, manifest["minimization"]
    ):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Canonical export identity differs")
    if not allow_staging and root.name != manifest.get("export_id"):
        raise ExportError("EXPORT_INCOMPLETE", "Staging is not a sealed package")
    for item in manifest.get("files", []):
        rel = PurePosixPath(item["path"])
        if rel.is_absolute() or ".." in rel.parts or rel.as_posix() != item["path"]:
            raise ExportError("EXPORT_PACKAGE_CONFLICT", "Inventory path is not contained")
    budget.files(root)
    if manifest.get("files") != inventory(root):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Package inventory, bytes or hashes differ")
    if (root / "schema.sql").read_text(encoding="utf-8") != ddl(contract):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "DDL differs from the reviewed contract")
    if any(
        i["path"] not in ("schema.sql", "reconciliation.sql") and not i["path"].endswith(".parquet")
        for i in manifest["files"]
    ):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Unapproved package artifact")
    if (root / "reconciliation.sql").read_text(encoding="utf-8") != reconciliation_sql(contract):
        raise ExportError(
            "EXPORT_PACKAGE_CONFLICT", "Reconciliation SQL differs from the reviewed contract"
        )
    known = {i["path"] for i in manifest["files"] if i["path"].endswith(".parquet")}
    minimized: dict[str, set[str]] = {k: set() for k in KEYS}
    verified_rows = 0
    for name, relation in contract.relations.items():
        digest = LogicalDigest(relation)
        paths = sorted(p for p in known if p.startswith(name + "/"))
        if not paths:
            raise ExportError("EXPORT_INCOMPLETE", "Relation file is missing")
        for path in paths:
            parts = PurePosixPath(path).parts
            if relation.partition_by_signing:
                valid_path = (
                    len(parts) == 4
                    and parts[3] == "part-00000.parquet"
                    and bool(re.fullmatch(r"signing_year_partition=(?:[0-9]{4}|unknown)", parts[1]))
                    and bool(
                        re.fullmatch(r"signing_month_partition=(?:0[1-9]|1[0-2]|unknown)", parts[2])
                    )
                )
            else:
                valid_path = parts == (name, "part-00000.parquet")
            if not valid_path:
                raise ExportError("EXPORT_PACKAGE_CONFLICT", "Unexpected physical file layout")
            file = pq.ParquetFile(root / path)
            verified_rows += file.metadata.num_rows
            if verified_rows > budget.max_rows or any(
                file.metadata.row_group(i).total_byte_size > 96 * 1024 * 1024
                for i in range(file.metadata.num_row_groups)
            ):
                raise ExportError("EXPORT_LIMIT", "Parquet decompression admission exceeded")
            if not file.schema_arrow.equals(relation.schema, check_metadata=True):
                raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Parquet schema differs")
            for batch in file.iter_batches(batch_size=1000, use_threads=False):
                budget.check()
                rows = batch.to_pylist()
                for row in rows:
                    if name in KEYS and manifest["minimization"]["policy"] != "none":
                        key = row[KEYS[name]]
                        if key in manifest["minimization"]["targets"][name]:
                            if row["supplier_name"] is not None or key in minimized[name]:
                                raise ExportError(
                                    "EXPORT_PACKAGE_CONFLICT",
                                    "Minimization target is not a unique NULL",
                                )
                            minimized[name].add(key)
                    for column in relation.columns:
                        validate_stored(row[column.name], column, contract)
                if any(
                    path
                    != f"{name}/{partition(row, relation)}/part-00000.parquet".replace("//", "/")
                    for row in rows
                ):
                    raise ExportError("EXPORT_PACKAGE_CONFLICT", "Partition content differs")
                sizes = digest.add(rows)
                # Verification counts rows independently from source build budget.
                if any(n > budget.row_bytes for n in sizes) or sum(sizes) > budget.batch_bytes:
                    raise ExportError("EXPORT_LIMIT", "Verified row/batch exceeds limit")
        if digest.summary() != manifest["relations"][name]:
            raise ExportError("EXPORT_PACKAGE_CONFLICT", "Full logical relation digest differs")
        known.difference_update(paths)
    if manifest["minimization"]["policy"] != "none" and any(
        minimized[k] != set(manifest["minimization"]["targets"][k]) for k in KEYS
    ):
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Minimization targets are missing")
    if known:
        raise ExportError("EXPORT_PACKAGE_CONFLICT", "Unexpected Parquet relation")
    budget.check()
    return manifest


def schemas(contract: Contract) -> dict[str, Any]:
    return {
        r.name: [
            {
                "name": c.name,
                "logical_type": c.source_type,
                "codec": c.codec,
                "minimization_policy": c.minimization_policy,
                "parquet": str(c.arrow_type),
                "glue": c.glue_type,
            }
            for c in r.columns
        ]
        for r in contract.relations.values()
    }
