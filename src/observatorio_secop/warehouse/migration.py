"""Transactional relational identity migration, separate from immutable bundle audit."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql

from observatorio_secop.identity import (
    IDENTITY_RULE_VERSION,
    IDENTITY_TEXT_FIELDS,
    LEGACY_PROJECTION,
    REQUIRED_IDENTITY_FIELDS,
)
from observatorio_secop.warehouse.errors import WarehouseLoadError


def migrate_identity(
    cursor: psycopg.Cursor[Any], current: sql.Identifier, revision: sql.Identifier
) -> None:
    cursor.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} (singleton boolean PRIMARY KEY CHECK(singleton), "
            "revision integer NOT NULL CHECK(revision = 2))"
        ).format(revision)
    )
    cursor.execute(sql.SQL("SELECT revision FROM {} WHERE singleton").format(revision))
    existing_revision = cursor.fetchone()
    if existing_revision is not None and existing_revision != (2,):
        raise WarehouseLoadError("Unsupported relational identity schema revision")
    for name in IDENTITY_TEXT_FIELDS:
        cursor.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text").format(
                current, sql.Identifier(name)
            )
        )
    # Old rows have no identity-rule marker. Never overwrite existing new-profile values.
    cursor.execute(
        sql.SQL(
            "UPDATE {} SET entity_identity_status = 'UNKNOWN', "
            "supplier_identity_status = 'UNKNOWN', "
            "entity_identity_reason = 'LEGACY_PROJECTION', "
            "supplier_identity_reason = 'LEGACY_PROJECTION', "
            "identity_rule_version = %s, source_projection_version = %s "
            "WHERE identity_rule_version IS NULL"
        ).format(current),
        (IDENTITY_RULE_VERSION, LEGACY_PROJECTION),
    )
    for name in sorted(REQUIRED_IDENTITY_FIELDS):
        cursor.execute(
            sql.SQL("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL").format(
                current, sql.Identifier(name)
            )
        )
    cursor.execute(
        sql.SQL(
            "SELECT count(*) FROM {} WHERE entity_identity_status NOT IN ('KNOWN', 'UNKNOWN') "
            "OR supplier_identity_status NOT IN ('KNOWN', 'UNKNOWN') "
            "OR (entity_identity_status = 'KNOWN') IS DISTINCT FROM (entity_code IS NOT NULL) "
            "OR (supplier_identity_status = 'KNOWN') IS DISTINCT FROM (supplier_code IS NOT NULL) "
            "OR (entity_identity_status = 'UNKNOWN') IS DISTINCT FROM "
            "(entity_identity_reason IS NOT NULL) "
            "OR (supplier_identity_status = 'UNKNOWN') IS DISTINCT FROM "
            "(supplier_identity_reason IS NOT NULL)"
        ).format(current)
    )
    result = cursor.fetchone()
    if result is None or result[0]:
        raise WarehouseLoadError("Existing relational identity state is inconsistent")
    cursor.execute(
        sql.SQL("INSERT INTO {} VALUES (true, 2) ON CONFLICT(singleton) DO NOTHING").format(
            revision
        )
    )
