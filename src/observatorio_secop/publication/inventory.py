"""Exact table-content and signature binding, independent of row ordering."""

from __future__ import annotations

import hashlib
import re
from typing import Any

import psycopg
from psycopg import sql

from .errors import PublicationError
from .models import OUTPUT_RELATIONS, OWNER_ROLE, one


def relation_inventory(cursor: psycopg.Cursor[Any], schema: str) -> dict[str, Any]:
    # JSON rendering of timestamptz depends on session timezone, not on row identity.
    cursor.execute("SET LOCAL timezone='UTC'")
    cursor.execute(
        "SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relkind IN ('r','p','v','m','f') ORDER BY c.relname",
        (schema,),
    )
    observed = {row[0]: row[1] for row in cursor.fetchall()}
    if re.fullmatch(r"candidate_[0-9a-f]{32}_analytics", schema):
        # dbt's private binding receipt is not an analytical output and never moves.
        binding = observed.pop("_gold_build_binding", None)
        if binding not in (None, "r"):
            raise PublicationError(
                "PUBLICATION_INVENTORY", "Private binding relation has invalid kind"
            )
    if observed != dict.fromkeys(OUTPUT_RELATIONS, "r"):
        raise PublicationError(
            "PUBLICATION_INVENTORY", "Publication must contain the eleven materialized tables"
        )
    result: dict[str, Any] = {}
    for relation in OUTPUT_RELATIONS:
        cursor.execute(
            "SELECT a.attname,format_type(a.atttypid,a.atttypmod) "
            "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (schema, relation),
        )
        columns = [list(row) for row in cursor.fetchall()]
        cursor.execute(
            sql.SQL("SELECT to_jsonb(t)::text FROM {}.{} t ORDER BY to_jsonb(t)::text").format(
                sql.Identifier(schema), sql.Identifier(relation)
            )
        )
        checksum = hashlib.sha256()
        count = 0
        while rows := cursor.fetchmany(1000):
            for row in rows:
                data = row[0].encode("utf-8")
                checksum.update(len(data).to_bytes(8, "big"))
                checksum.update(data)
                count += 1
        cursor.execute(
            "SELECT pg_total_relation_size(c.oid) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s",
            (schema, relation),
        )
        result[relation] = {
            "columns": columns,
            "row_count": count,
            "sha256": checksum.hexdigest(),
            "total_bytes": one(cursor)[0],
        }
    return result


def logical_inventory(value: dict[str, Any]) -> dict[str, Any]:
    """Storage layout may change independently of logical immutable contents."""
    return {
        name: {key: v for key, v in item.items() if key != "total_bytes"}
        for name, item in value.items()
    }


def require_matching(actual: dict[str, Any], expected: dict[str, Any]) -> None:
    if logical_inventory(actual) != logical_inventory(expected):
        raise PublicationError("PUBLICATION_CONFLICT", "Publication contents or schema changed")


def require_frozen_ownership(cursor: psycopg.Cursor[Any], schema: str) -> None:
    cursor.execute(
        "SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner "
        "WHERE n.nspname=%s",
        (schema,),
    )
    if one(cursor)[0] != OWNER_ROLE:
        raise PublicationError("PUBLICATION_CONFLICT", "Immutable schema ownership changed")
    cursor.execute(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "JOIN pg_roles r ON r.oid=c.relowner WHERE n.nspname=%s AND c.relkind IN ('r','v') "
        "AND r.rolname<>%s",
        (schema, OWNER_ROLE),
    )
    if one(cursor)[0]:
        raise PublicationError("PUBLICATION_CONFLICT", "Immutable relation ownership changed")


def view_inventory(
    cursor: psycopg.Cursor[Any], schema: str, *, catalog: bool = False
) -> dict[str, Any]:
    """Bind safe projections as well as underlying contents to the immutable ID."""
    require_frozen_ownership(cursor, schema)
    expected = {*OUTPUT_RELATIONS, "publication_metadata"}
    if catalog:
        expected.add("catalog")
    cursor.execute(
        "SELECT c.relname,c.relkind,pg_get_viewdef(c.oid,true) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s "
        "AND c.relkind IN ('r','v','m','p','f') ORDER BY c.relname",
        (schema,),
    )
    records = cursor.fetchall()
    if {row[0] for row in records} != expected or any(row[1] != "v" for row in records):
        raise PublicationError("PUBLICATION_CONFLICT", "Safe view inventory changed")
    result = {}
    for name, _, definition in records:
        cursor.execute(
            "SELECT a.attname,format_type(a.atttypid,a.atttypmod) FROM pg_attribute a "
            "JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (schema, name),
        )
        result[name] = {
            "definition": definition,
            "columns": [list(row) for row in cursor.fetchall()],
        }
    return result
