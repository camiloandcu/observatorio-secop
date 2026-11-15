"""Canonical package identity includes the exact reviewed writer recipe."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pyarrow as pa

from .package_json import canonical_json


def writer_recipe() -> dict[str, str]:
    root = Path(__file__).parent
    checksum = hashlib.sha256()
    for p in sorted(root.glob("*.py")):
        checksum.update(p.name.encode() + b"\0" + p.read_bytes())
    return {
        "revision": checksum.hexdigest(),
        "pyarrow": pa.__version__,
        "compression": "snappy",
        "parquet_version": "2.6",
        "row_group_size": "1000",
        "dictionary": "false",
    }


def export_identity(
    publication_id: str,
    binding: dict[str, Any],
    contract_hash: str,
    writer: dict[str, str],
    minimization: dict[str, Any] | None = None,
) -> str:
    return hashlib.sha256(
        canonical_json(
            {
                "publication_id": publication_id,
                "binding": binding,
                "contract_sha256": contract_hash,
                "writer": writer,
                "minimization": minimization or {"policy": "none", "targets": {}, "counts": {}},
            }
        )
    ).hexdigest()
