from __future__ import annotations

import hashlib

import pytest

from observatorio_secop.cloud_export.errors import ExportError
from observatorio_secop.cloud_export.minimization import (
    Minimizer,
    load_approval,
    public_policy,
    validate_policy,
)
from observatorio_secop.cloud_export.package_json import canonical_json

LABEL = "contact@example.test"


def approval():
    return {
        "version": "supplier-name-null-v1",
        "publication_id": "d" * 32,
        "silver_manifest_sha256": "e" * 64,
        "label_sha256": hashlib.sha256(LABEL.encode()).hexdigest(),
        "targets": {"dim_supplier": ["a" * 32], "mart_contract_detail": ["b" * 64, "c" * 64]},
    }


def test_only_three_approved_cells_change_and_missing_cells_fail():
    rule = approval()
    minimizer = Minimizer(rule)
    dim = [{"supplier_sk": "a" * 32, "supplier_name": LABEL, "other": "unchanged"}]
    detail = [
        {"contract_key": key, "supplier_name": LABEL, "amount": "123.45"}
        for key in rule["targets"]["mart_contract_detail"]
    ]
    untouched = {"contract_key": "f" * 64, "supplier_name": "Approved company", "amount": "678.90"}
    detail.append(untouched.copy())
    minimizer.apply("dim_supplier", dim)
    minimizer.apply("mart_contract_detail", detail)
    minimizer.finish()
    assert dim == [{"supplier_sk": "a" * 32, "supplier_name": None, "other": "unchanged"}]
    assert detail[-1] == untouched
    assert sum(row["supplier_name"] is None for row in dim + detail) == 3
    assert [r["amount"] for r in detail] == ["123.45", "123.45", "678.90"]
    with pytest.raises(ExportError):
        Minimizer(rule).finish()


def test_changed_label_and_duplicate_cell_are_rejected():
    minimizer = Minimizer(approval())
    with pytest.raises(ExportError):
        minimizer.apply(
            "dim_supplier", [{"supplier_sk": "a" * 32, "supplier_name": "other@example.test"}]
        )
    row = {"supplier_sk": "a" * 32, "supplier_name": LABEL}
    minimizer.apply("dim_supplier", [row.copy()])
    with pytest.raises(ExportError):
        minimizer.apply("dim_supplier", [row.copy()])


def test_no_approval_never_redacts_and_public_metadata_has_no_label_hash():
    rows = [{"supplier_sk": "a" * 32, "supplier_name": LABEL}]
    Minimizer(None).apply("dim_supplier", rows)
    assert rows[0]["supplier_name"] == LABEL
    rendered = canonical_json(public_policy(approval()))
    assert LABEL.encode() not in rendered and b"label_sha256" not in rendered
    validate_policy(public_policy(approval()))
    changed = public_policy(approval())
    changed["counts"]["dim_supplier"] = 2
    with pytest.raises(ExportError):
        validate_policy(changed)


def test_private_approval_is_bound_to_publication_and_hash(tmp_path):
    path = tmp_path / "approval.json"
    path.write_bytes(canonical_json(approval()))
    path.chmod(0o600)
    descriptor = {"publication_id": "d" * 32, "metadata": {"silver_manifest_sha256": "e" * 64}}
    assert load_approval(path, descriptor) == approval()
    with pytest.raises(ExportError):
        load_approval(path, {**descriptor, "publication_id": "f" * 32})
    path.chmod(0o644)
    with pytest.raises(ExportError):
        load_approval(path, descriptor)


@pytest.mark.parametrize("damage", ["extra", "dictionary", "nonstring"])
def test_approval_raw_targets_are_not_normalized_into_authorization(tmp_path, damage):
    rule = approval()
    if damage == "extra":
        rule["targets"]["other_relation"] = []
    if damage == "dictionary":
        rule["targets"]["dim_supplier"] = {"a" * 32: "ignored"}
    if damage == "nonstring":
        rule["targets"]["dim_supplier"] = [1]
    path = tmp_path / "approval.json"
    path.write_bytes(canonical_json(rule))
    path.chmod(0o600)
    descriptor = {"publication_id": "d" * 32, "metadata": {"silver_manifest_sha256": "e" * 64}}
    with pytest.raises(ExportError):
        load_approval(path, descriptor)
