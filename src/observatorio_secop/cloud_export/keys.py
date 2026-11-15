"""Disk-backed dimension uniqueness and fact/detail referential checks."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .errors import ExportError

DIMENSIONS = {
    "dim_entity": "entity_sk",
    "dim_supplier": "supplier_sk",
    "dim_municipality": "municipality_sk",
    "dim_modality": "modality_sk",
    "dim_date": "date_sk",
}
REFERENCES = {
    "entity_sk": "dim_entity",
    "supplier_sk": "dim_supplier",
    "municipality_sk": "dim_municipality",
    "modality_sk": "dim_modality",
    "signing_date_sk": "dim_date",
    "start_date_sk": "dim_date",
    "end_date_sk": "dim_date",
}


class Keys:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.execute("PRAGMA cache_size=-2048")
        self.db.execute(
            "CREATE TABLE keys (relation TEXT,k TEXT,PRIMARY KEY(relation,k)) WITHOUT ROWID"
        )
        self.checked = 0

    def add(self, name: str, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            if name in DIMENSIONS or name in ("fact_contract", "mart_contract_detail"):
                key = row[DIMENSIONS.get(name, "contract_key")]
                if key is None:
                    raise ExportError("EXPORT_INPUT_CONFLICT", "Required relation key is null")
                try:
                    self.db.execute("INSERT INTO keys VALUES (?,?)", (name, str(key)))
                except sqlite3.IntegrityError as error:
                    raise ExportError(
                        "EXPORT_INPUT_CONFLICT", "Duplicate declared relation key"
                    ) from error
            if name in ("fact_contract", "mart_contract_detail"):
                for col, dim in REFERENCES.items():
                    if (
                        row[col] is not None
                        and not self.db.execute(
                            "SELECT 1 FROM keys WHERE relation=? AND k=?", (dim, str(row[col]))
                        ).fetchone()
                    ):
                        raise ExportError(
                            "EXPORT_INPUT_CONFLICT", "Published foreign key is absent"
                        )
                    self.checked += 1

    def close(self) -> int:
        self.db.close()
        return self.checked
