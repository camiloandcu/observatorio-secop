"""Read-only, server-timed publication descriptors across independent connections."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from observatorio_secop.warehouse.config import WarehouseConfig

from .errors import PublicationError
from .models import OPERATION_SECONDS, OUTPUT_RELATIONS, PUBLICATION_LOCK_ID, namespaces
from .models import publication_id as canonical_id


@contextmanager
def reader_cursor(
    config: WarehouseConfig, *, statement_timeout_ms: int = 60_000
) -> Iterator[psycopg.Cursor[Any]]:
    """A short read-only transaction, with no lease/control writes or unsafe paths."""
    if type(statement_timeout_ms) is not int or statement_timeout_ms <= 0:
        raise PublicationError("PUBLICATION_INVALID", "A finite positive read timeout is required")
    try:
        with (
            psycopg.connect(
                host=config.host,
                port=config.port,
                dbname=config.dbname,
                user=config.user,
                password=config.password,
                connect_timeout=10,
                row_factory=dict_row,
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL timezone='UTC'")
            cursor.execute("SET LOCAL search_path=pg_catalog")
            cursor.execute("SET LOCAL lock_timeout='5s'")
            cursor.execute(
                "SELECT set_config('statement_timeout',%s,true)",
                (f"{statement_timeout_ms}ms",),
            )
            yield cursor
    except psycopg.Error as error:
        if error.sqlstate == "42501":
            code = "PUBLICATION_DENIED"
        elif error.sqlstate in ("42P01", "3F000"):
            code = "PUBLICATION_UNAVAILABLE"
        elif error.sqlstate == "55P03":
            code = "PUBLICATION_BUSY"
        elif error.sqlstate == "57014":
            code = "PUBLICATION_QUERY_TIMEOUT"
        else:
            code = "PUBLICATION_DATABASE_UNAVAILABLE"
        raise PublicationError(code, "Publication read could not complete") from error


def _catalog(cursor: psycopg.Cursor[Any], value: str | None) -> dict[str, Any]:
    cursor.execute("SELECT pg_advisory_xact_lock_shared(%s)", (PUBLICATION_LOCK_ID,))
    if value is None:
        cursor.execute(
            "SELECT *,clock_timestamp() AS server_now FROM published.catalog WHERE is_active"
        )
    else:
        value = canonical_id(value)
        cursor.execute(
            "SELECT *,clock_timestamp() AS server_now FROM published.catalog "
            "WHERE publication_id=%s",
            (value,),
        )
    row = cursor.fetchone()
    if row is None:
        code = "PUBLICATION_UNAVAILABLE" if value is None else "PUBLICATION_UNKNOWN"
        raise PublicationError(code, "No available publication matches the selection")
    if (
        row["state"] != "retained"
        or row["read_namespace"] != namespaces(row["publication_id"])["read"]
    ):
        raise PublicationError("PUBLICATION_UNAVAILABLE", "Publication is no longer retained")
    return dict(row)


def resolve(config: WarehouseConfig, publication_id: str | None = None) -> dict[str, Any]:
    """Resolve latest once, or explicitly select a retained identity with a full horizon."""
    with reader_cursor(config) as cursor:
        row = _catalog(cursor, publication_id)
        now = row["server_now"]
        deadline = now + timedelta(seconds=OPERATION_SECONDS)
        if not (row["is_active"] or row["is_previous"]) and (
            row["retain_until"] is None or row["retain_until"] < deadline
        ):
            raise PublicationError(
                "PUBLICATION_EXPIRING", "Publication lacks a full operation horizon"
            )
        return {
            "version": 1,
            "publication_id": row["publication_id"],
            "read_namespace": row["read_namespace"],
            "built_at": row["built_at"].isoformat(),
            "metadata": row["metadata"],
            "resolved_at": now.isoformat(),
            "operation_expires_at": deadline.isoformat(),
        }


def _instant(value: Any) -> datetime:
    if not isinstance(value, str):
        raise PublicationError("PUBLICATION_INVALID", "Descriptor time must be an ISO instant")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise PublicationError("PUBLICATION_INVALID", "Descriptor time is invalid") from error
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise PublicationError("PUBLICATION_INVALID", "Descriptor time must be UTC")
    return result.astimezone(UTC)


def _validate(cursor: psycopg.Cursor[Any], descriptor: dict[str, Any]) -> dict[str, Any]:
    if (
        not isinstance(descriptor, dict)
        or type(descriptor.get("version")) is not int
        or descriptor.get("version") != 1
    ):
        raise PublicationError("PUBLICATION_INVALID", "Unsupported read descriptor")
    value = canonical_id(descriptor.get("publication_id", ""))
    if descriptor.get("read_namespace") != namespaces(value)["read"]:
        raise PublicationError("PUBLICATION_INVALID", "Descriptor namespace is not allowlisted")
    resolved = _instant(descriptor.get("resolved_at"))
    deadline = _instant(descriptor.get("operation_expires_at"))
    if deadline - resolved != timedelta(seconds=OPERATION_SECONDS):
        raise PublicationError("PUBLICATION_INVALID", "Descriptor lifetime differs from the policy")
    row = _catalog(cursor, value)
    if resolved > row["server_now"]:
        raise PublicationError("PUBLICATION_INVALID", "Descriptor starts in the future")
    if row["server_now"] >= deadline:
        raise PublicationError(
            "PUBLICATION_EXPIRED", "Restart the complete operation with a new descriptor"
        )
    if (
        descriptor.get("metadata") != row["metadata"]
        or descriptor.get("built_at") != row["built_at"].isoformat()
    ):
        raise PublicationError("PUBLICATION_CONFLICT", "Immutable descriptor binding changed")
    return dict(descriptor)


def validate_descriptor(config: WarehouseConfig, descriptor: dict[str, Any]) -> dict[str, Any]:
    with reader_cursor(config) as cursor:
        return _validate(cursor, descriptor)


def read_publication(
    config: WarehouseConfig,
    descriptor: dict[str, Any],
    relation: str,
    *,
    limit: int = 100,
    offset: int = 0,
    statement_timeout_ms: int = 60_000,
) -> list[dict[str, Any]]:
    """Bounded deterministic SQL read. HTTP cursor/filter contracts remain API-owned."""
    if relation not in (*OUTPUT_RELATIONS, "publication_metadata"):
        raise PublicationError("PUBLICATION_INVALID", "Relation is not a publication output")
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise PublicationError("PUBLICATION_INVALID", "Invalid bounded read pagination")
    # Release the short registry lock before the bounded data statement. Retention
    # protects the descriptor horizon; ordinary PostgreSQL relation locks protect
    # the statement without blocking unrelated publication activations globally.
    checked = validate_descriptor(config, descriptor)
    with reader_cursor(config, statement_timeout_ms=statement_timeout_ms) as cursor:
        cursor.execute(
            sql.SQL(
                'SELECT v.* FROM {} v ORDER BY to_jsonb(v)::text COLLATE "C" LIMIT %s OFFSET %s'
            ).format(sql.Identifier(checked["read_namespace"], relation)),
            (limit, offset),
        )
        return [dict(row) for row in cursor.fetchall()]


def scoring_compatibility(
    publication_id: str,
    score_publication_id: str | None = None,
    *,
    failed: bool = False,
) -> dict[str, Any]:
    """Pure optional binding check: it cannot mutate analytical state or invent scores."""
    value = canonical_id(publication_id)
    if failed:
        state = "FAILED"
    elif score_publication_id is None:
        state = "ABSENT"
    else:
        state = "COMPATIBLE" if canonical_id(score_publication_id) == value else "MISMATCHED"
    return {"publication_id": value, "state": state}
