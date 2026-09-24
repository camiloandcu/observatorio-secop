"""Small structured logger that never receives connection secrets."""

from __future__ import annotations

import json
import sys
from typing import Any


def log_event(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    print(json.dumps(payload, sort_keys=True, default=str), file=sys.stderr)
