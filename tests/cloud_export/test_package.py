from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from observatorio_secop.cloud_export.budget import Budget
from observatorio_secop.cloud_export.contract import load_contract
from observatorio_secop.cloud_export.ddl import ddl
from observatorio_secop.cloud_export.errors import ExportError
from observatorio_secop.cloud_export.exact import ExactRows
from observatorio_secop.cloud_export.identity import export_identity, writer_recipe
from observatorio_secop.cloud_export.logical import LogicalDigest
from observatorio_secop.cloud_export.package import (
    LIMITATIONS,
    RelationWriter,
    canonical_json,
    inventory,
    schemas,
    verify,
)
from observatorio_secop.cloud_export.reconciliation import reconciliation_sql


def identity_fields():
    return {
        "publication_id": "a" * 32,
        "binding": {
            "silver_version_id": "b" * 64,
            "silver_manifest_sha256": "c" * 64,
            "scope_id": "d" * 64,
        },
        "writer": writer_recipe(),
    }


def package_root(base: Path) -> Path:
    fields = identity_fields()
    return base / export_identity(
        fields["publication_id"], fields["binding"], load_contract().digest, fields["writer"]
    )


def fixture(root: Path) -> dict:
    contract = load_contract()
    summaries = {}
    budget = Budget()
    root.mkdir()
    for relation in contract.relations.values():
        writer = RelationWriter(root, relation, budget)
        row = {c.name: None for c in relation.columns}
        if "signing_date" in row:
            row["signing_date"] = date(2026, 2, 1)
        if "contract_value_cop" in row:
            c = next(c for c in relation.columns if c.name == "contract_value_cop")
            row[c.name] = Decimal("123.45") if c.codec == "decimal" else "123.4500"
        writer.add([row, row])
        summaries[relation.name] = writer.close()
    (root / "schema.sql").write_text(ddl(contract))
    (root / "reconciliation.sql").write_text(reconciliation_sql(contract))
    manifest = {
        "version": contract.version,
        "contract_sha256": contract.digest,
        "export_id": root.name,
        **identity_fields(),
        "exact_source_comparison": {"relations": 13, "mismatches": 0},
        "foreign_key_checks": 0,
        "minimization": {"policy": "none", "targets": {}, "counts": {}},
        "logical_digest": "logical-digest-v1",
        "limitations": LIMITATIONS,
        "schemas": schemas(contract),
        "relations": summaries,
        "files": inventory(root),
    }
    (root / "manifest.json").write_bytes(canonical_json(manifest))
    return manifest


def test_two_empty_roots_have_identical_bytes(tmp_path):
    left, right = package_root(tmp_path / "one"), package_root(tmp_path / "two")
    left.parent.mkdir()
    right.parent.mkdir()
    a, b = fixture(left), fixture(right)
    assert a == b
    assert (left / "manifest.json").read_bytes() == (right / "manifest.json").read_bytes()
    assert verify(left, load_contract()) == a
    assert verify(right, load_contract()) == b


@pytest.mark.parametrize(
    "damage", ["extra", "missing", "bytes", "symlink", "escape", "stage", "schema"]
)
def test_verification_rejects_damage(tmp_path, damage):
    root = package_root(tmp_path)
    manifest = fixture(root)
    p = root / manifest["files"][0]["path"]
    if damage == "extra":
        (root / "unexpected.txt").write_text("unexpected")
    if damage == "missing":
        p.unlink()
    if damage == "bytes":
        p.write_bytes(p.read_bytes() + b"changed")
    if damage == "symlink":
        p.unlink()
        p.symlink_to(tmp_path / "outside")
    if damage == "escape":
        manifest["files"][0]["path"] = "../outside"
    if damage == "stage":
        manifest["export_id"] = "other"
    if damage == "schema":
        manifest["schemas"]["dim_date"][0]["codec"] = "changed"
    if damage in ("escape", "stage", "schema"):
        (root / "manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(ExportError):
        verify(root, load_contract())


def test_empty_and_duplicate_logical_multisets():
    import hashlib

    relation = load_contract().relations["dim_entity"]
    row = {c.name: None for c in relation.columns}
    a, b = LogicalDigest(relation), LogicalDigest(relation)
    assert a.summary()["logical_sha256"] == hashlib.sha256(b"").hexdigest()
    a.add([row])
    b.add([row, row])
    assert a.summary()["logical_sha256"] != b.summary()["logical_sha256"]


def test_exact_comparison_rejects_value_swap_preserving_aggregates(tmp_path):
    relation = load_contract().relations["silver_valid"]
    row = {c.name: None for c in relation.columns}
    a = {**row, "contract_key": "a", "contract_value_cop": Decimal("1.00")}
    b = {**row, "contract_key": "b", "contract_value_cop": Decimal("2.00")}
    exact = ExactRows(tmp_path / "exact.sqlite")
    exact.add([a, b], relation)
    writer = RelationWriter(tmp_path, relation, Budget())
    writer.add(
        [
            {**a, "contract_value_cop": b["contract_value_cop"]},
            {**b, "contract_value_cop": a["contract_value_cop"]},
        ]
    )
    writer.close()
    with pytest.raises(ExportError, match="differs"):
        exact.compare(tmp_path, relation, Budget())
    exact.close()


def test_ddl_types_and_partition_columns_do_not_collide():
    contract = load_contract()
    sql = ddl(contract)
    assert sql.count("CREATE EXTERNAL TABLE") == 13
    assert "`contract_value_cop` decimal(20,2)" in sql
    assert "`source_updated_at` string" in sql
    assert "`signing_year_partition` string" in sql
    for relation in contract.relations.values():
        assert "signing_year_partition" not in {c.name for c in relation.columns}


def test_nested_manifest_is_not_hidden_from_inventory(tmp_path):
    root = package_root(tmp_path)
    fixture(root)
    (root / "extra").mkdir()
    (root / "extra" / "manifest.json").write_text("{}")
    with pytest.raises(ExportError):
        verify(root, load_contract())


def test_self_consistent_restricted_content_is_rejected(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    contract = load_contract()
    root = package_root(tmp_path)
    manifest = fixture(root)
    relation = contract.relations["dim_supplier"]
    p = root / "dim_supplier" / "part-00000.parquet"
    rows = pq.ParquetFile(p).read().to_pylist()
    rows[0]["supplier_name"] = "contact@example.test"
    pq.write_table(
        pa.Table.from_pylist(rows, schema=relation.schema),
        p,
        compression="snappy",
        use_dictionary=False,
    )
    digest = LogicalDigest(relation)
    digest.add(rows)
    manifest["relations"][relation.name] = digest.summary()
    manifest["files"] = inventory(root)
    (root / "manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(ExportError):
        verify(root, contract)


def test_self_consistent_unapproved_artifact_is_rejected(tmp_path):
    root = package_root(tmp_path)
    manifest = fixture(root)
    (root / "private.json").write_text("{}")
    manifest["files"] = inventory(root)
    (root / "manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(ExportError):
        verify(root, load_contract())


def test_supervised_verification_does_not_resolve_root_symlink(tmp_path):
    from observatorio_secop.cloud_export.build import supervised_verify

    root = package_root(tmp_path)
    fixture(root)
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(ExportError):
        supervised_verify(alias)


def test_missing_identity_fields_are_rejected(tmp_path):
    root = package_root(tmp_path)
    manifest = fixture(root)
    del manifest["publication_id"]
    (root / "manifest.json").write_bytes(canonical_json(manifest))
    with pytest.raises(ExportError):
        verify(root, load_contract())
