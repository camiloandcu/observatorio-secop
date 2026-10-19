"""Locked activation metadata and evidence-preserving migration of legacy loads."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql

from observatorio_secop.warehouse.config import IDENTIFIER, WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConfigurationError, WarehouseLoadError

Identity = tuple[str, str]


class ActivationStore:
    """Operate inside the caller's transaction after acquiring its publication lock."""

    def __init__(self, config: WarehouseConfig) -> None:
        names = [
            config.contract_table,
            config.audit_table,
            config.audit_table + "_active",
            config.audit_table + "_activation",
            config.audit_table + "_identity",
            config.audit_table + "_schema",
        ]
        if (
            len(set(names)) != len(names)
            or any(not IDENTIFIER.fullmatch(name) or len(name.encode()) > 63 for name in names)
            or not IDENTIFIER.fullmatch(config.schema)
            or len(config.schema.encode()) > 63
        ):
            raise WarehouseConfigurationError("Warehouse relation names collide or exceed limits")
        (
            self.current,
            self.audit,
            self.active,
            self.events,
            self.identity_index,
            self.schema_revision,
        ) = [sql.Identifier(config.schema, name) for name in names]
        self.index_name = sql.Identifier(names[4])

    def ensure(self, cursor: psycopg.Cursor[Any]) -> Identity | None:
        cursor.execute(
            "SELECT to_regclass(%s) IS NOT NULL, to_regclass(%s) IS NOT NULL",
            (self.active.as_string(), self.events.as_string()),
        )
        present = cursor.fetchone()
        assert present is not None
        if present[0] != present[1]:
            raise WarehouseLoadError("Warehouse activation metadata is incomplete; inspect backup")
        if present[0]:
            return self._read_active(cursor)

        # A unique pair allows both new relations to enforce the registered hash
        # without changing legacy registry records or their primary key.
        cursor.execute(
            sql.SQL("CREATE UNIQUE INDEX {} ON {} (silver_version_id, manifest_sha256)").format(
                self.index_name, self.audit
            )
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {} (
                    activation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    silver_version_id text NOT NULL,
                    manifest_sha256 text NOT NULL,
                    previous_version_id text,
                    previous_manifest_sha256 text,
                    event_kind text NOT NULL
                        CHECK (event_kind IN ('activation', 'restoration', 'legacy_baseline')),
                    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                    source_rows bigint NOT NULL CHECK (source_rows >= 0),
                    inserted_rows bigint CHECK (inserted_rows >= 0),
                    updated_rows bigint CHECK (updated_rows >= 0),
                    deleted_rows bigint CHECK (deleted_rows >= 0),
                    UNIQUE (activation_id, silver_version_id, manifest_sha256),
                    FOREIGN KEY (silver_version_id, manifest_sha256)
                        REFERENCES {} (silver_version_id, manifest_sha256),
                    FOREIGN KEY (previous_version_id, previous_manifest_sha256)
                        REFERENCES {} (silver_version_id, manifest_sha256) MATCH FULL,
                    CHECK (
                        (event_kind = 'legacy_baseline' AND inserted_rows IS NULL
                            AND updated_rows IS NULL AND deleted_rows IS NULL)
                        OR (event_kind <> 'legacy_baseline' AND inserted_rows IS NOT NULL
                            AND updated_rows IS NOT NULL AND deleted_rows IS NOT NULL)
                    )
                )
                """
            ).format(self.events, self.audit, self.audit)
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE {} (
                    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                    silver_version_id text NOT NULL,
                    manifest_sha256 text NOT NULL,
                    activation_id bigint NOT NULL,
                    FOREIGN KEY (activation_id, silver_version_id, manifest_sha256)
                        REFERENCES {} (activation_id, silver_version_id, manifest_sha256)
                )
                """
            ).format(self.active, self.events)
        )
        return self._legacy_baseline(cursor)

    def _read_active(self, cursor: psycopg.Cursor[Any]) -> Identity | None:
        cursor.execute(
            sql.SQL("SELECT silver_version_id, manifest_sha256, activation_id FROM {}").format(
                self.active
            )
        )
        active = cursor.fetchone()
        cursor.execute(
            sql.SQL(
                "SELECT silver_version_id, manifest_sha256, activation_id "
                "FROM {} ORDER BY activation_id DESC LIMIT 1"
            ).format(self.events)
        )
        latest = cursor.fetchone()
        if active != latest:
            raise WarehouseLoadError("Warehouse active state and activation history disagree")
        if active is None:
            cursor.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {})").format(self.current))
            has_rows = cursor.fetchone()
            assert has_rows is not None
            if has_rows[0]:
                raise WarehouseLoadError("Warehouse rows have no established activation state")
            return None
        return active[0], active[1]

    def _legacy_baseline(self, cursor: psycopg.Cursor[Any]) -> Identity | None:
        cursor.execute(
            sql.SQL(
                "SELECT silver_version_id, silver_manifest_sha256, loaded_at, count(*) "
                "FROM {} GROUP BY 1, 2, 3 LIMIT 2"
            ).format(self.current)
        )
        groups = cursor.fetchall()
        if not groups:
            return None
        if len(groups) != 1:
            raise WarehouseLoadError("Legacy warehouse snapshot has inconsistent identities")
        version, manifest_hash, loaded_at, row_count = groups[0]
        cursor.execute(
            sql.SQL(
                "SELECT manifest_sha256, manifest_finished_at, source_rows "
                "FROM {} WHERE silver_version_id = %s"
            ).format(self.audit),
            (version,),
        )
        if cursor.fetchone() != (manifest_hash, loaded_at, row_count):
            raise WarehouseLoadError("Legacy warehouse snapshot does not match audit evidence")
        identity = (version, manifest_hash)
        self.activate(cursor, identity, None, "legacy_baseline", row_count, None, None, None)
        return identity

    def activate(
        self,
        cursor: psycopg.Cursor[Any],
        identity: Identity,
        previous: Identity | None,
        kind: str,
        source_rows: int,
        inserted: int | None,
        updated: int | None,
        deleted: int | None,
    ) -> None:
        cursor.execute(
            sql.SQL(
                "INSERT INTO {} (silver_version_id, manifest_sha256, previous_version_id, "
                "previous_manifest_sha256, event_kind, source_rows, inserted_rows, "
                "updated_rows, deleted_rows) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "RETURNING activation_id"
            ).format(self.events),
            (*identity, *(previous or (None, None)), kind, source_rows, inserted, updated, deleted),
        )
        event = cursor.fetchone()
        assert event is not None
        cursor.execute(
            sql.SQL(
                "INSERT INTO {} (singleton, silver_version_id, manifest_sha256, activation_id) "
                "VALUES (true, %s, %s, %s) ON CONFLICT (singleton) DO UPDATE SET "
                "silver_version_id = EXCLUDED.silver_version_id, "
                "manifest_sha256 = EXCLUDED.manifest_sha256, activation_id = EXCLUDED.activation_id"
            ).format(self.active),
            (*identity, event[0]),
        )
