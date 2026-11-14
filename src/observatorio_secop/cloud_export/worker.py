"""Supervised finite phases; source material never enters diagnostics."""

from __future__ import annotations

import gc
import json
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import WarehouseError

from .binding import Binding
from .budget import Budget
from .codecs import encode
from .contract import load_contract
from .ddl import ddl
from .errors import ExportError
from .exact import ExactRows
from .identity import writer_recipe
from .keys import Keys
from .minimization import Minimizer, public_policy, validate_policy
from .package import LIMITATIONS, RelationWriter, canonical_json, inventory, schemas, verify
from .reader import batches, checked
from .reconciliation import reconciliation_sql


def _read(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_bytes()))


def validate(request: dict[str, Any], budget: Budget) -> dict[str, Any]:
    contract = load_contract()
    root, scratch = Path(request["stage"]), Path(request["scratch"])
    descriptor = request["descriptor"]
    metadata = descriptor["metadata"]
    version, checksum = metadata["silver_version_id"], metadata["silver_manifest_sha256"]
    if (
        not isinstance(version, str)
        or len(version) != 64
        or any(c not in "0123456789abcdef" for c in version)
    ):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Invalid Silver version binding")
    silver = Path(request["silver_root"]).resolve()
    manifest = silver / "versions" / version / "manifest.json"
    if not manifest.resolve().is_relative_to(silver):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Silver path escapes source root")
    # Admit the decompressed physical data before invoking shared validation.
    source = _read(manifest)
    for item in source.get("files", []):
        if item.get("path", "").startswith("data/") and item["path"].endswith(".parquet"):
            p = (manifest.parent / item["path"]).resolve()
            if not p.is_relative_to(manifest.parent.resolve()):
                raise ExportError("EXPORT_INPUT_CONFLICT", "Silver inventory escape")
            file = pq.ParquetFile(p)
            for i in range(file.metadata.num_row_groups):
                group = file.metadata.row_group(i)
                if group.total_byte_size > 96 * 1024 * 1024:
                    raise ExportError("EXPORT_LIMIT", "Silver row group cannot be safely admitted")
    budget.check()
    bundle = verify_manifest_bundle(manifest, expected_hash=checksum, spill_directory=scratch)
    budget.check()
    coverage = bundle.coverage or {}
    if metadata["scope_id"] != coverage.get("scope_id") or metadata["scope"] != coverage.get(
        "scope"
    ):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Silver/publication identity or scope differs")
    relation = contract.relations["silver_valid"]
    # Drop large auxiliary Python evidence after its shared validators succeeded.
    table = bundle.table.select([c.name for c in relation.columns])
    del bundle
    gc.collect()
    pa.default_memory_pool().release_unused()
    writer = RelationWriter(root, relation, budget)
    exact = ExactRows(scratch / "silver-exact.sqlite")
    try:
        for batch in table.to_batches(max_chunksize=1000):
            values = [
                {c.name: encode(row[c.name], c, contract) for c in relation.columns}
                for row in batch.to_pylist()
            ]
            exact.add(values, relation)
            writer.add(values)
        result = writer.close()
        exact.compare(root, relation, budget)
        exact.close()
    except BaseException:
        writer.abort()
        raise
    return {"silver_valid": result}


def export(request: dict[str, Any], budget: Budget) -> dict[str, Any]:
    contract = load_contract()
    root = Path(request["stage"])
    descriptor = request["descriptor"]
    config = load_warehouse_config(Path(request["config"]))
    checked(config, descriptor, budget)
    summaries = request["silver_summary"]
    minimizer = Minimizer(request.get("minimization_approval"))
    validate_policy(request["minimization"])
    if public_policy(request.get("minimization_approval")) != request["minimization"]:
        raise ExportError("EXPORT_INPUT_CONFLICT", "Minimization request differs from approval")
    budget.rows = summaries["silver_valid"]["rows"]
    exact = ExactRows(Path(request["scratch"]) / "gold-exact.sqlite")
    keys = Keys(Path(request["scratch"]) / "keys.sqlite")
    binding = Binding(Path(request["scratch"]) / "binding.sqlite", contract)
    for p in sorted((root / "silver_valid").rglob("*.parquet")):
        for batch in pq.ParquetFile(p).iter_batches(batch_size=1000, use_threads=False):
            budget.check()
            binding.add(batch.to_pylist())
    for name, relation in contract.relations.items():
        if name == "silver_valid":
            continue
        writer = RelationWriter(root, relation, budget)
        try:
            for rows in batches(config, descriptor, relation, contract, budget, minimizer):
                if name == "mart_contract_detail":
                    binding.compare(rows)
                if name == "publication_metadata" and (
                    len(rows) != 1
                    or any(
                        rows[0][key] != descriptor["metadata"][key]
                        for key in ("silver_version_id", "silver_manifest_sha256", "scope_id")
                    )
                ):
                    raise ExportError("EXPORT_INPUT_CONFLICT", "Metadata relation binding differs")
                keys.add(name, rows)
                exact.add(rows, relation)
                writer.add(rows)
            summaries[name] = writer.close()
            exact.compare(root, relation, budget)
        except BaseException as error:
            details = getattr(error, "safe_details", {})
            details.setdefault("relation", name)
            cast(Any, error).safe_details = details
            writer.abort()
            raise
    minimizer.finish()
    binding.finish()
    exact.close()
    foreign_key_checks = keys.close()
    (root / "schema.sql").write_text(ddl(contract), encoding="utf-8")
    (root / "schema.sql").chmod(0o600)
    (root / "reconciliation.sql").write_text(reconciliation_sql(contract), encoding="utf-8")
    (root / "reconciliation.sql").chmod(0o600)
    metadata = descriptor["metadata"]
    manifest = {
        "version": contract.version,
        "export_id": request["export_id"],
        "writer": request["writer"],
        "minimization": request["minimization"],
        "publication_id": descriptor["publication_id"],
        "contract_sha256": contract.digest,
        "binding": {
            k: metadata[k] for k in ("silver_version_id", "silver_manifest_sha256", "scope_id")
        },
        "schemas": schemas(contract),
        "relations": summaries,
        "files": inventory(root),
        "logical_digest": "logical-digest-v1",
        "exact_source_comparison": {"relations": 13, "mismatches": 0},
        "foreign_key_checks": foreign_key_checks,
        "limitations": LIMITATIONS,
    }
    # A complete manifest is written only after every source relation was read.
    (root / "manifest.json").write_bytes(canonical_json(manifest))
    (root / "manifest.json").chmod(0o600)
    verified = verify(root, contract, budget, allow_staging=True)
    checked(config, descriptor, budget)
    return verified


def main() -> int:
    try:
        request = _read(Path(sys.argv[2]))
        budget = Budget(seconds=request["remaining_seconds"])
        Path(request["baseline_file"]).write_bytes(canonical_json({"baseline": budget.baseline}))
        # The supervisor starts its deadline before launching validation; each
        # blocking statement uses the same remaining horizon supplied to this phase.
        if sys.argv[1] != "verify" and request["writer"] != writer_recipe():
            raise ExportError("EXPORT_INPUT_CONFLICT", "Writer recipe changed before worker start")
        if sys.argv[1] == "validate":
            result = validate(request, budget)
        elif sys.argv[1] == "verify":
            result = verify(Path(request["package"]), load_contract(), budget)
        else:
            result = export(request, budget)
        budget.check()
        result_path = Path(request["result"])
        result_path.write_bytes(
            canonical_json(
                {
                    "result": result,
                    "baseline": budget.baseline,
                    "peak_increment": __import__("resource").getrusage(0).ru_maxrss * 1024
                    - budget.baseline,
                }
            )
        )
        result_path.chmod(0o600)
        return 0
    except (ExportError, PublicationError, WarehouseError) as error:
        (Path(request["scratch"]) / "failure.json").write_bytes(
            canonical_json(
                {
                    "phase": sys.argv[1],
                    "code": getattr(error, "code", "EXPORT_INPUT_CONFLICT"),
                    "safe_details": getattr(error, "safe_details", {}),
                    "message": str(error)
                    if isinstance(error, ExportError)
                    else "Input validation failed",
                }
            )
        )
        print(
            json.dumps({"code": getattr(error, "code", "EXPORT_INPUT_CONFLICT")}), file=sys.stderr
        )
        return 2
    except Exception as error:
        import traceback

        frames = [
            {"file": Path(f.filename).name, "function": f.name, "line": f.lineno}
            for f in traceback.extract_tb(error.__traceback__)
        ]
        (Path(request["scratch"]) / "failure.json").write_bytes(
            canonical_json(
                {"phase": sys.argv[1], "exception_type": type(error).__name__, "frames": frames}
            )
        )
        print('{"code":"EXPORT_INCOMPLETE"}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
