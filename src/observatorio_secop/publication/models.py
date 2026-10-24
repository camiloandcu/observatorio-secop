"""Shared publication identifiers, relation inventory and reviewed bounds."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any

import psycopg

from .errors import PublicationError

PUBLICATION_LOCK_ID = 7404026
OWNER_ROLE = "secop_publication_owner"
BUILDER_ROLE = "secop_publication_builder"
READER_ROLES = ("secop_bi_reader", "secop_api_reader")
OUTPUT_RELATIONS = (
    "fact_contract",
    "dim_entity",
    "dim_supplier",
    "dim_municipality",
    "dim_modality",
    "dim_date",
    "mart_contract_overview",
    "mart_entity_monthly",
    "mart_supplier_concentration",
    "mart_contract_detail",
    "mart_data_quality",
)
MAX_RETAINED = 3
RETENTION_HOURS = 24
OPERATION_SECONDS = 3600
CONTROL_SCHEMA = "publication_control"


def publication_id(value: str | None = None) -> str:
    """Accept only the exact canonical safe UUID representation used in schemas."""
    result = uuid.uuid4().hex if value is None else value
    if not isinstance(result, str) or re.fullmatch(r"[0-9a-f]{32}", result) is None:
        raise PublicationError("PUBLICATION_INVALID", "Publication ID must be canonical UUID hex")
    return result


def namespaces(value: str) -> dict[str, str]:
    value = publication_id(value)
    return {
        "input": f"candidate_{value}_input",
        "staging": f"candidate_{value}_staging",
        "intermediate": f"candidate_{value}_intermediate",
        "analytics": f"candidate_{value}_analytics",
        "data": f"publication_data_{value}",
        "read": f"publication_{value}",
    }


def digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def candidate_lock_id(value: str) -> int:
    return int.from_bytes(hashlib.sha256(publication_id(value).encode()).digest()[:8], "big") >> 1


def one(cursor: psycopg.Cursor[Any]) -> Any:
    result = cursor.fetchone()
    if result is None:
        raise PublicationError("PUBLICATION_STATE", "Required publication metadata is absent")
    return result
