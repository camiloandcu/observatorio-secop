"""Explicit, pinned approval for the three contact-bearing supplier-label cells."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from .errors import ExportError
from .package_json import parse_object

POLICY = "supplier-name-null-v1"
KEYS = {"dim_supplier": "supplier_sk", "mart_contract_detail": "contract_key"}
COUNTS = {"dim_supplier": 1, "mart_contract_detail": 2}
NONE = {"policy": "none", "targets": {}, "counts": {}}
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def public_policy(approval: dict[str, Any] | None) -> dict[str, Any]:
    if approval is None:
        return {"policy": "none", "targets": {}, "counts": {}}
    return {
        "policy": POLICY,
        "targets": {k: sorted(approval["targets"][k]) for k in KEYS},
        "counts": dict(COUNTS),
    }


def validate_policy(value: Any) -> None:
    if value == NONE:
        return
    if (
        not isinstance(value, dict)
        or set(value) != {"policy", "targets", "counts"}
        or value["policy"] != POLICY
        or value["counts"] != COUNTS
        or not isinstance(value["targets"], dict)
        or set(value["targets"]) != set(KEYS)
    ):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Unsupported minimization declaration")
    for relation, n in COUNTS.items():
        targets = value["targets"][relation]
        if (
            not isinstance(targets, list)
            or len(targets) != n
            or len(set(targets)) != n
            or targets != sorted(targets)
            or not all(
                isinstance(k, str)
                and re.fullmatch(
                    "[0-9a-f]{32}" if relation == "dim_supplier" else "[0-9a-f]{64}", k
                )
                for k in targets
            )
        ):
            raise ExportError(
                "EXPORT_INPUT_CONFLICT", "Minimization targets differ from the bounded approval"
            )


def load_approval(path: Path | None, descriptor: dict[str, Any]) -> dict[str, Any] | None:
    if path is None:
        return None
    if (
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & 0o077
        or path.stat().st_size > 65536
    ):
        raise ExportError(
            "EXPORT_INPUT_CONFLICT", "Minimization approval must be a private regular file"
        )
    approval = parse_object(path.read_bytes())
    if (
        set(approval)
        != {"version", "publication_id", "silver_manifest_sha256", "label_sha256", "targets"}
        or approval["version"] != POLICY
        or approval["publication_id"] != descriptor["publication_id"]
        or approval["silver_manifest_sha256"] != descriptor["metadata"]["silver_manifest_sha256"]
        or not re.fullmatch("[0-9a-f]{64}", str(approval["label_sha256"]))
    ):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Approval is not bound to the selected source")
    targets = approval["targets"]
    if (
        not isinstance(targets, dict)
        or set(targets) != set(KEYS)
        or any(
            not isinstance(v, list) or not all(isinstance(k, str) for k in v)
            for v in targets.values()
        )
    ):
        raise ExportError("EXPORT_INPUT_CONFLICT", "Approval targets must be exact relation lists")
    validate_policy(public_policy(approval))
    return approval


class Minimizer:
    def __init__(self, approval: dict[str, Any] | None):
        self.approval = approval
        self.seen: dict[str, set[str]] = {name: set() for name in KEYS}

    def apply(self, name: str, rows: list[dict[str, Any]]) -> None:
        if self.approval is None or name not in KEYS:
            return
        targets = set(self.approval["targets"][name])
        for row in rows:
            key = row[KEYS[name]]
            if key not in targets:
                continue
            value = row["supplier_name"]
            if (
                key in self.seen[name]
                or not isinstance(value, str)
                or not EMAIL.search(value)
                or hashlib.sha256(value.encode()).hexdigest() != self.approval["label_sha256"]
            ):
                raise ExportError(
                    "EXPORT_INPUT_CONFLICT", "Approved label cell changed or repeated"
                )
            row["supplier_name"] = None
            self.seen[name].add(key)

    def finish(self) -> None:
        if self.approval is not None and any(
            self.seen[k] != set(self.approval["targets"][k]) for k in KEYS
        ):
            raise ExportError(
                "EXPORT_INPUT_CONFLICT", "Approved minimization cells were not all observed"
            )
