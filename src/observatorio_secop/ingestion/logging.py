"""Safe JSON-line event logging."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from observatorio_secop.ingestion.security import redact


class EventLogger:
    def __init__(
        self, sink: Callable[[str], None] = print, *, secrets: tuple[str, ...] = ()
    ) -> None:
        self._sink = sink
        self._secrets = secrets

    def emit(self, event: str, *, run_id: str, **fields: Any) -> None:
        record = {"run_id": run_id, "event": event, **fields}
        self._sink(json.dumps(redact(record, secrets=self._secrets), sort_keys=True))
