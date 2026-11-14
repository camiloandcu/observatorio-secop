"""Finite local-export admission and observed-resource limits."""

from __future__ import annotations

import resource
import time
from dataclasses import dataclass, field
from pathlib import Path

from .errors import ExportError

MIB = 1024 * 1024


def rss_bytes() -> int:
    # Linux is the supported exporter runtime; ru_maxrss is KiB here.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


@dataclass
class Budget:
    seconds: float = 300
    max_rows: int = 1_000_000
    row_bytes: int = 8 * MIB
    batch_bytes: int = 16 * MIB
    max_files: int = 400
    output_bytes: int = 80 * MIB
    memory_bytes: int = 256 * MIB
    started: float = field(default_factory=time.monotonic)
    baseline: int = field(default_factory=rss_bytes)
    rows: int = 0

    def check(self) -> None:
        if time.monotonic() - self.started >= self.seconds:
            raise ExportError("EXPORT_LIMIT", "Global export deadline exceeded")
        if rss_bytes() - self.baseline > self.memory_bytes:
            raise ExportError("EXPORT_LIMIT", "Observed incremental memory limit exceeded")

    def remaining_ms(self, ceiling: int = 60_000) -> int:
        self.check()
        return max(1, min(ceiling, int((self.seconds - (time.monotonic() - self.started)) * 1000)))

    def admit(self, sizes: list[int]) -> None:
        self.check()
        if (
            len(sizes) > 1000
            or any(n > self.row_bytes for n in sizes)
            or sum(sizes) > self.batch_bytes
        ):
            raise ExportError("EXPORT_LIMIT", "Encoded row or batch byte limit exceeded")
        self.rows += len(sizes)
        if self.rows > self.max_rows:
            raise ExportError("EXPORT_LIMIT", "Combined relation row limit exceeded")

    def files(self, root: Path) -> None:
        self.check()
        paths = [p for p in root.rglob("*") if p.is_file()]
        if len(paths) > self.max_files or sum(p.stat().st_size for p in paths) > self.output_bytes:
            raise ExportError("EXPORT_LIMIT", "Package inventory limit exceeded")
