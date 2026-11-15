"""Exact full-row multiset reconciliation, independent of file/order layout."""

from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal, localcontext
from typing import Any

from .codecs import canonical_cell
from .contract import Relation
from .errors import ExportError


class LogicalDigest:
    def __init__(self, relation: Relation):
        self.relation = relation
        self.hashes: list[str] = []
        self.nulls = {c.name: 0 for c in relation.columns}
        self.money = Decimal(0)
        self.dates: dict[str, list[str | None]] = {
            c.name: [None, None] for c in relation.columns if c.codec == "date"
        }

    def add(self, rows: list[dict[str, Any]]) -> list[int]:
        sizes = []
        for row in rows:
            data = b"".join(canonical_cell(row[c.name], c) for c in self.relation.columns)
            sizes.append(len(data))
            self.hashes.append(hashlib.sha256(data).hexdigest())
            if len(self.hashes) * 64 > 8 * 1024 * 1024:
                raise ExportError("EXPORT_LIMIT", "Logical digest concatenation exceeds 8 MiB")
            for name in self.nulls:
                self.nulls[name] += row[name] is None
            for name, endpoints in self.dates.items():
                if isinstance(row[name], date):
                    text = row[name].isoformat()
                    endpoints[0] = min(endpoints[0], text) if endpoints[0] else text
                    endpoints[1] = max(endpoints[1], text) if endpoints[1] else text
            if "contract_value_cop" in row and row["contract_value_cop"] is not None:
                with localcontext() as context:
                    context.prec = max(
                        100,
                        len(self.money.as_tuple().digits)
                        + len(Decimal(row["contract_value_cop"]).as_tuple().digits)
                        + 1,
                    )
                    self.money += Decimal(row["contract_value_cop"])
        return sizes

    def summary(self) -> dict[str, Any]:
        return {
            "rows": len(self.hashes),
            "logical_sha256": hashlib.sha256(
                "".join(sorted(self.hashes)).encode("ascii")
            ).hexdigest(),
            "nulls": self.nulls,
            "date_ranges": self.dates,
            "contract_value_cop_sum": str(self.money),
        }
