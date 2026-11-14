"""Ephemeral exact multiset comparison of every exported cell, without hashes."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .budget import Budget
from .codecs import canonical_cell
from .contract import Relation
from .errors import ExportError


class ExactRows:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.execute("PRAGMA cache_size=-4096")
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("CREATE TABLE rows (value BLOB PRIMARY KEY, n INTEGER) WITHOUT ROWID")

    def key(self, row: dict[str, Any], relation: Relation) -> bytes:
        return b"".join(canonical_cell(row[c.name], c) for c in relation.columns)

    def add(self, rows: list[dict[str, Any]], relation: Relation) -> None:
        self.db.executemany(
            "INSERT INTO rows VALUES (?,1) ON CONFLICT(value) DO UPDATE SET n=n+1",
            [(self.key(r, relation),) for r in rows],
        )

    def compare(self, root: Path, relation: Relation, budget: Budget) -> None:
        for p in sorted((root / relation.name).rglob("*.parquet")):
            for batch in pq.ParquetFile(p).iter_batches(batch_size=1000, use_threads=False):
                budget.check()
                for row in batch.to_pylist():
                    key = self.key(row, relation)
                    found = self.db.execute("SELECT n FROM rows WHERE value=?", (key,)).fetchone()
                    if not found or found[0] < 1:
                        raise ExportError(
                            "EXPORT_PACKAGE_CONFLICT", "Exact decoded row differs from source"
                        )
                    self.db.execute("UPDATE rows SET n=n-1 WHERE value=?", (key,))
        if self.db.execute("SELECT 1 FROM rows WHERE n<>0 LIMIT 1").fetchone():
            raise ExportError("EXPORT_PACKAGE_CONFLICT", "Exact decoded multiset omits source rows")
        self.db.execute("DELETE FROM rows")
        self.db.commit()

    def close(self) -> None:
        self.db.close()
