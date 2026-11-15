"""Disk-backed exact Silver/detail key and shared-value reconciliation."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from typing import Any

from .contract import Contract
from .errors import ExportError

# Only fields with the same upstream meaning are paired. Derived SKs, buckets,
# labels and namespace fields deliberately retain their published semantics.
SHARED = {
    "contract_key": "contract_key",
    "source_contract_id": "contract_id",
    "contract_reference": "contract_reference",
    "procurement_process_id": "process_id",
    "entity_code": "entity_code",
    "entity_identity_status": "entity_identity_status",
    "entity_identity_reason": "entity_identity_reason",
    "supplier_code": "supplier_code",
    "supplier_identity_status": "supplier_identity_status",
    "supplier_identity_reason": "supplier_identity_reason",
    "signing_date": "signing_date",
    "start_date": "start_date",
    "end_date": "end_date",
    "contract_value_cop": "contract_value_cop",
    "contract_status": "status_raw",
    "source_updated_at": "source_updated_at",
    "bronze_fetched_at_utc": "acquisition_observed_at_utc",
    "quality_warning_codes": "quality_warning_codes",
    "payload_sha256": "payload_sha256",
    "bronze_run_id": "bronze_run_id",
    "bronze_row_ordinal": "bronze_row_ordinal",
    "silver_version_id": "silver_version_id",
}


class Binding:
    def __init__(self, path: Path, contract: Contract):
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.execute("PRAGMA cache_size=-4096")
        self.db.execute("CREATE TABLE shared (k TEXT PRIMARY KEY, value BLOB NOT NULL)")
        self.columns = {c.name: c for c in contract.relations["silver_valid"].columns}

    def value(self, row: dict[str, Any], detail: bool) -> bytes:
        from .codecs import canonical_cell

        return hashlib.sha256(
            b"".join(
                canonical_cell(row[dst if detail else src], self.columns[src])
                for src, dst in SHARED.items()
            )
        ).digest()

    def add(self, rows: list[dict[str, Any]]) -> None:
        try:
            self.db.executemany(
                "INSERT INTO shared VALUES (?,?)",
                [(r["contract_key"], self.value(r, False)) for r in rows],
            )
        except sqlite3.IntegrityError as error:
            raise ExportError("EXPORT_INPUT_CONFLICT", "Duplicate Silver key") from error

    def compare(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            actual = self.db.execute(
                "SELECT value FROM shared WHERE k=?", (row["contract_key"],)
            ).fetchone()
            if actual is None or actual[0] != self.value(row, True):
                raise ExportError(
                    "EXPORT_INPUT_CONFLICT", "Silver/detail key or shared values differ"
                )
            self.db.execute("DELETE FROM shared WHERE k=?", (row["contract_key"],))

    def finish(self) -> None:
        if self.db.execute("SELECT count(*) FROM shared").fetchone()[0]:
            raise ExportError("EXPORT_INPUT_CONFLICT", "Published detail omits Silver keys")
        self.db.close()
