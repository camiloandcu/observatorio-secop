"""Transactional registration, private candidate slots and immutable sealing."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from observatorio_secop.warehouse.config import WarehouseConfig

from .errors import PublicationError
from .inventory import (
    relation_inventory,
    require_frozen_ownership,
    require_matching,
    view_inventory,
)
from .models import (
    MAX_RETAINED,
    OUTPUT_RELATIONS,
    OWNER_ROLE,
    PUBLICATION_LOCK_ID,
    candidate_lock_id,
    digest,
    namespaces,
    one,
)
from .models import (
    publication_id as canonical_id,
)
from .security import bootstrap_roles


class PublicationStore:
    """Trusted operator API; consumers use the separate read-only resolver."""

    def __init__(self, config: WarehouseConfig, *, fault: Callable[[str], None] | None = None):
        self.config = config
        self.fault = fault

    def checkpoint(self, point: str) -> None:
        if self.fault is not None:
            self.fault(point)

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Cursor[Any]]:
        with (
            psycopg.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=10,
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SET LOCAL timezone='UTC'")
            cursor.execute("SET LOCAL search_path=pg_catalog")
            cursor.execute("SET LOCAL lock_timeout='5s'")
            cursor.execute("SET LOCAL statement_timeout='30s'")
            yield cursor

    @staticmethod
    def _lock(cursor: psycopg.Cursor[Any]) -> None:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLICATION_LOCK_ID,))

    def bootstrap(self) -> dict[str, Any]:
        with self.transaction() as cursor:
            self._lock(cursor)
            bootstrap_roles(cursor)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS publication_control.revision
                    (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), revision integer);
                INSERT INTO publication_control.revision VALUES(true,1) ON CONFLICT DO NOTHING;
                CREATE TABLE IF NOT EXISTS publication_control.publications (
                    publication_id text PRIMARY KEY CHECK(publication_id ~ '^[0-9a-f]{32}$'),
                    state text NOT NULL CHECK(state IN
                        ('building','failed','sealed','retained','pruned','discarded')),
                    metadata jsonb NOT NULL DEFAULT '{}', inventory jsonb, receipt jsonb,
                    expected_nodes jsonb, failure_code text, projection_inventory jsonb,
                    created_at timestamptz NOT NULL DEFAULT clock_timestamp(), built_at timestamptz,
                    deactivated_at timestamptz, retain_until timestamptz,
                    ever_activated boolean NOT NULL DEFAULT false,
                    expected_publication_id text, expected_generation bigint NOT NULL,
                    identity_sha256 text
                );
                CREATE UNIQUE INDEX IF NOT EXISTS publication_candidate_slot
                    ON publication_control.publications ((true))
                    WHERE state IN ('building','failed','sealed');
                CREATE TABLE IF NOT EXISTS publication_control.active (
                    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                    publication_id text REFERENCES publication_control.publications,
                    generation bigint NOT NULL DEFAULT 0, previous_id text,
                    activated_at timestamptz, view_inventory jsonb
                );
                INSERT INTO publication_control.active(singleton) VALUES(true)
                    ON CONFLICT DO NOTHING;
                CREATE TABLE IF NOT EXISTS publication_control.activations (
                    generation bigint PRIMARY KEY, publication_id text NOT NULL,
                    previous_id text, kind text NOT NULL, activated_at timestamptz NOT NULL
                )
            """)
            cursor.execute("SELECT revision FROM publication_control.revision")
            if one(cursor)[0] != 1:
                raise PublicationError(
                    "PUBLICATION_SCHEMA", "Unsupported publication schema revision"
                )
            cursor.execute(
                "ALTER TABLE publication_control.publications "
                "ADD COLUMN IF NOT EXISTS expected_nodes jsonb"
            )
            cursor.execute(
                "ALTER TABLE publication_control.publications "
                "ADD COLUMN IF NOT EXISTS failure_code text"
            )
            cursor.execute(
                "ALTER TABLE publication_control.publications "
                "ADD COLUMN IF NOT EXISTS projection_inventory jsonb"
            )
            cursor.execute(
                "ALTER TABLE publication_control.active "
                "ADD COLUMN IF NOT EXISTS view_inventory jsonb"
            )
            cursor.execute("""
                CREATE OR REPLACE VIEW published.catalog AS
                SELECT p.publication_id,
                    CASE WHEN p.state='retained' THEN 'publication_'||p.publication_id END
                        AS read_namespace,
                    p.state,p.built_at,p.retain_until,
                    coalesce(p.publication_id=a.publication_id,false) AS is_active,
                    coalesce(p.publication_id=a.previous_id,false) AS is_previous,
                    jsonb_build_object(
                        'silver_version_id',p.metadata->'silver_version_id',
                        'silver_manifest_sha256',p.metadata->'silver_manifest_sha256',
                        'scope_id',p.metadata->'scope_id',
                        'catalog_sha256',p.metadata->'catalog_sha256',
                        'dbt_graph_sha256',p.metadata->'dbt_graph_sha256',
                        'code_revision',p.metadata->'code_revision',
                        'policy_sha256',p.metadata->'policy_sha256',
                        'policy_version',p.metadata->'policy_version',
                        'identity_rule_version',p.metadata->'identity_rule_version',
                        'source_dataset_id',p.metadata->'source_dataset_id',
                        'coverage_state',p.metadata->'coverage_state',
                        'coverage_known',p.metadata->'coverage_known',
                        'scope',p.metadata->'scope',
                        'observed_at',p.metadata->'observed_at',
                        'pipeline_completed_at',p.metadata->'pipeline_completed_at',
                        'captured_at',p.metadata->'captured_at',
                        'scoring_state','ABSENT'
                    ) AS metadata
                FROM publication_control.publications p
                CROSS JOIN publication_control.active a WHERE p.ever_activated
            """)
            for name in ("revision", "publications", "active", "activations"):
                cursor.execute(
                    sql.SQL("ALTER TABLE publication_control.{} OWNER TO {}").format(
                        sql.Identifier(name), sql.Identifier(OWNER_ROLE)
                    )
                )
                cursor.execute(
                    sql.SQL("REVOKE ALL ON publication_control.{} FROM PUBLIC").format(
                        sql.Identifier(name)
                    )
                )
            cursor.execute(
                sql.SQL("ALTER VIEW published.catalog OWNER TO {}").format(
                    sql.Identifier(OWNER_ROLE)
                )
            )
            from .security import grant_consumption

            grant_consumption(cursor, "published")
        return {"status": "ready", "schema_revision": 1}

    @staticmethod
    def _active(cursor: psycopg.Cursor[Any]) -> dict[str, Any]:
        cursor.execute(
            "SELECT publication_id,generation,previous_id,activated_at,view_inventory "
            "FROM publication_control.active WHERE singleton"
        )
        row = one(cursor)
        return dict(
            zip(
                ("publication_id", "generation", "previous_id", "activated_at", "view_inventory"),
                row,
                strict=True,
            )
        )

    @staticmethod
    def _get(cursor: psycopg.Cursor[Any], value: str) -> dict[str, Any]:
        value = canonical_id(value)
        cursor.execute(
            "SELECT * FROM publication_control.publications WHERE publication_id=%s", (value,)
        )
        row = cursor.fetchone()
        if row is None:
            raise PublicationError("PUBLICATION_UNKNOWN", "Publication is not registered")
        result = dict(zip((column.name for column in cursor.description or ()), row, strict=True))
        result["namespaces"] = namespaces(value)
        return result

    def inspect(self, publication_id: str | None = None) -> dict[str, Any]:
        with self.transaction() as cursor:
            self._lock(cursor)
            if publication_id is not None:
                return self._get(cursor, publication_id)
            active = self._active(cursor)
            cursor.execute(
                "SELECT publication_id FROM publication_control.publications "
                "ORDER BY created_at,publication_id"
            )
            rows = [self._get(cursor, row[0]) for row in cursor.fetchall()]
            candidates = [row for row in rows if row["state"] in ("building", "failed", "sealed")]
            return {
                "active": active,
                "publications": rows,
                "candidate": candidates[0] if candidates else None,
            }

    def reserve(
        self, metadata: dict[str, Any], publication_id: str | None = None
    ) -> dict[str, Any]:
        value = canonical_id(publication_id)
        with self.transaction() as cursor:
            self._lock(cursor)
            cursor.execute(
                "SELECT state,metadata FROM publication_control.publications "
                "WHERE publication_id=%s",
                (value,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing[0] in ("building", "failed") and existing[1] == metadata:
                    return self._get(cursor, value)
                raise PublicationError(
                    "PUBLICATION_CONFLICT", "Publication identity already exists"
                )
            cursor.execute(
                "SELECT count(*) FROM publication_control.publications "
                "WHERE state IN ('building','failed','sealed')"
            )
            if one(cursor)[0]:
                raise PublicationError(
                    "PUBLICATION_CANDIDATE_BUSY", "Inspect the existing candidate first"
                )
            cursor.execute(
                "SELECT count(*) FROM publication_control.publications WHERE state='retained'"
            )
            if one(cursor)[0] >= MAX_RETAINED:
                raise PublicationError(
                    "PUBLICATION_CAPACITY", "Cleanup eligible publications before building"
                )
            active = self._active(cursor)
            cursor.execute(
                "INSERT INTO publication_control.publications "
                "(publication_id,state,metadata,expected_publication_id,expected_generation) "
                "VALUES(%s,'building',%s,%s,%s)",
                (value, Jsonb(metadata), active["publication_id"], active["generation"]),
            )
            return self._get(cursor, value)

    def update_metadata(self, publication_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        with self.transaction() as cursor:
            self._lock(cursor)
            row = self._get(cursor, publication_id)
            if row["state"] not in ("building", "failed") or any(
                key in metadata and metadata[key] != value for key, value in row["metadata"].items()
            ):
                raise PublicationError(
                    "PUBLICATION_CONFLICT", "Immutable candidate metadata changed"
                )
            merged = row["metadata"] | metadata
            cursor.execute(
                "UPDATE publication_control.publications SET metadata=%s WHERE publication_id=%s",
                (Jsonb(merged), publication_id),
            )
            return self._get(cursor, publication_id)

    def register_expected_nodes(self, publication_id: str, required_nodes: list[str]) -> None:
        """Pin the full trusted parsed graph before any dbt result can be accepted."""
        if (
            not required_nodes
            or any(not isinstance(node, str) for node in required_nodes)
            or len(required_nodes) != len(set(required_nodes))
        ):
            raise PublicationError("PUBLICATION_VALIDATION", "Parsed node inventory is invalid")
        nodes = sorted(required_nodes)
        with self.transaction() as cursor:
            self._lock(cursor)
            row = self._get(cursor, publication_id)
            if row["state"] not in ("building", "failed") or (
                row["expected_nodes"] is not None and row["expected_nodes"] != nodes
            ):
                raise PublicationError("PUBLICATION_CONFLICT", "Parsed node inventory changed")
            cursor.execute(
                "UPDATE publication_control.publications SET expected_nodes=%s "
                "WHERE publication_id=%s",
                (Jsonb(nodes), publication_id),
            )

    def mark_failed(self, publication_id: str, code: str = "PUBLICATION_BUILD_FAILED") -> None:
        if re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", code) is None:
            raise PublicationError("PUBLICATION_INVALID", "Failure code is not a safe identifier")
        with self.transaction() as cursor:
            self._lock(cursor)
            row = self._get(cursor, publication_id)
            if row["state"] not in ("building", "failed"):
                return
            cursor.execute(
                "UPDATE publication_control.publications SET state='failed',failure_code=%s "
                "WHERE publication_id=%s",
                (code, publication_id),
            )

    def retry(self, publication_id: str) -> dict[str, Any]:
        with self.transaction() as cursor:
            self._lock(cursor)
            row = self._get(cursor, publication_id)
            if row["state"] not in ("building", "failed"):
                raise PublicationError(
                    "PUBLICATION_INVALID", "Only unfinished candidates can retry"
                )
            cursor.execute(
                "SELECT pg_try_advisory_xact_lock(%s)", (candidate_lock_id(publication_id),)
            )
            if not one(cursor)[0]:
                raise PublicationError(
                    "PUBLICATION_CANDIDATE_BUSY", "A live build owns this candidate"
                )
            cursor.execute(
                "UPDATE publication_control.publications SET state='building',failure_code=NULL "
                "WHERE publication_id=%s",
                (publication_id,),
            )
            return self._get(cursor, publication_id)

    def seal(self, publication_id: str, receipt: dict[str, Any]) -> dict[str, Any]:
        from .projections import create_views

        with self.transaction() as cursor:
            self._lock(cursor)
            row = self._get(cursor, publication_id)
            _validate_receipt(publication_id, row, receipt)
            cursor.execute(
                "SELECT pg_try_advisory_xact_lock(%s)", (candidate_lock_id(publication_id),)
            )
            if not one(cursor)[0]:
                raise PublicationError(
                    "PUBLICATION_CANDIDATE_BUSY", "A live build owns this candidate"
                )
            if row["state"] in ("sealed", "retained"):
                if row["receipt"] != receipt:
                    raise PublicationError(
                        "PUBLICATION_CONFLICT", "Sealed validation receipt changed"
                    )
                require_matching(
                    relation_inventory(cursor, row["namespaces"]["data"]), row["inventory"]
                )
                self._require_immutable(cursor, row)
                return row
            if row["state"] not in ("building", "failed"):
                raise PublicationError("PUBLICATION_UNAVAILABLE", "Candidate cannot be sealed")
            space = row["namespaces"]
            inventory = relation_inventory(cursor, space["analytics"])
            require_matching(inventory, receipt.get("inventory", {}))
            cursor.execute("SELECT clock_timestamp()")
            built_at = one(cursor)[0]
            for schema in (space["data"], space["read"]):
                cursor.execute(
                    sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                        sql.Identifier(schema), sql.Identifier(OWNER_ROLE)
                    )
                )
            for name in OUTPUT_RELATIONS:
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} SET SCHEMA {}").format(
                        sql.Identifier(space["analytics"]),
                        sql.Identifier(name),
                        sql.Identifier(space["data"]),
                    )
                )
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} OWNER TO {}").format(
                        sql.Identifier(space["data"]),
                        sql.Identifier(name),
                        sql.Identifier(OWNER_ROLE),
                    )
                )
                cursor.execute(
                    sql.SQL("REVOKE ALL ON {}.{} FROM PUBLIC, secop_publication_builder").format(
                        sql.Identifier(space["data"]), sql.Identifier(name)
                    )
                )
            self.checkpoint("seal_after_transfer")
            create_views(
                cursor,
                publication_id,
                space["data"],
                space["read"],
                row["metadata"] | {"built_at": built_at.isoformat()},
            )
            from .lifecycle import _discard_workspace

            _discard_workspace(cursor, publication_id)
            require_frozen_ownership(cursor, space["data"])
            projections = view_inventory(cursor, space["read"])
            cursor.execute(
                "UPDATE publication_control.publications SET state='sealed',inventory=%s,"
                "receipt=%s,built_at=%s,projection_inventory=%s,identity_sha256=%s "
                "WHERE publication_id=%s",
                (
                    Jsonb(inventory),
                    Jsonb(receipt),
                    built_at,
                    Jsonb(projections),
                    digest(
                        {
                            "metadata": row["metadata"],
                            "inventory": inventory,
                            "receipt": receipt,
                            "built_at": built_at,
                            "projection_inventory": projections,
                        }
                    ),
                    publication_id,
                ),
            )
            return self._get(cursor, publication_id)

    @staticmethod
    def _require_immutable(cursor: psycopg.Cursor[Any], row: dict[str, Any]) -> None:
        require_frozen_ownership(cursor, row["namespaces"]["data"])
        expected = digest(
            {
                key: row[key]
                for key in ("metadata", "inventory", "receipt", "built_at", "projection_inventory")
            }
        )
        if (
            expected != row["identity_sha256"]
            or view_inventory(cursor, row["namespaces"]["read"]) != row["projection_inventory"]
        ):
            raise PublicationError(
                "PUBLICATION_CONFLICT", "Immutable metadata or projections changed"
            )

    def activate(
        self,
        publication_id: str,
        expected_publication_id: str | None,
        expected_generation: int,
        *,
        restore: bool = False,
    ) -> dict[str, Any]:
        from .lifecycle import activate

        return activate(
            self, publication_id, expected_publication_id, expected_generation, restore=restore
        )

    def cleanup_preview(self) -> list[dict[str, Any]]:
        from .lifecycle import cleanup_preview

        return cleanup_preview(self)

    def cleanup(self, publication_ids: list[str]) -> dict[str, Any]:
        from .lifecycle import cleanup

        return cleanup(self, publication_ids)

    def discard(self, publication_id: str) -> dict[str, Any]:
        from .lifecycle import discard

        return discard(self, publication_id)


def _validate_receipt(value: str, row: dict[str, Any], receipt: dict[str, Any]) -> None:
    nodes = receipt.get("required_nodes", [])
    results = receipt.get("results", [])
    observed = {
        item.get("unique_id"): item.get("status") for item in results if isinstance(item, dict)
    }
    models, tests = receipt.get("model_nodes", []), receipt.get("test_nodes", [])
    if (
        receipt.get("version") != 1
        or receipt.get("publication_id") != value
        or receipt.get("metadata") != row["metadata"]
        or not nodes
        or sorted(nodes) != row["expected_nodes"]
        or not tests
        or len(observed) != len(results)
        or set(nodes) != set(models) | set(tests)
        or any(not node.startswith("model.") for node in models)
        or any(not node.startswith("test.") for node in tests)
        or not set(OUTPUT_RELATIONS).issubset({node.rsplit(".", 1)[-1] for node in models})
        or any(observed.get(node) != "success" for node in models)
        or any(observed.get(node) != "pass" for node in tests)
        or any(
            not isinstance(receipt.get(key), str)
            or re.fullmatch(r"[0-9a-f]{64}", receipt[key]) is None
            for key in ("manifest_sha256", "run_results_sha256")
        )
    ):
        raise PublicationError(
            "PUBLICATION_VALIDATION", "Complete matching successful validation is required"
        )
