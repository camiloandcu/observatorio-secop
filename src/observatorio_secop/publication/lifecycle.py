"""Atomic activation, bounded retention and dependency-safe owned cleanup."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .errors import PublicationError
from .inventory import relation_inventory, require_matching, view_inventory
from .models import (
    MAX_RETAINED,
    OUTPUT_RELATIONS,
    OWNER_ROLE,
    candidate_lock_id,
    namespaces,
    one,
    publication_id,
)
from .security import grant_consumption, verify_reader_boundary

if TYPE_CHECKING:
    from .store import PublicationStore


def _columns(cursor: psycopg.Cursor[Any], schema: str, name: str) -> list[tuple[str, str]]:
    cursor.execute(
        "SELECT a.attname,format_type(a.atttypid,a.atttypmod) "
        "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s "
        "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
        (schema, name),
    )
    return cursor.fetchall()


def activate(
    store: PublicationStore,
    value: str,
    expected_id: str | None,
    expected_generation: int,
    *,
    restore: bool = False,
) -> dict[str, Any]:
    value = publication_id(value)
    if expected_id is not None:
        publication_id(expected_id)
    if (
        isinstance(expected_generation, bool)
        or not isinstance(expected_generation, int)
        or expected_generation < 0
    ):
        raise PublicationError("PUBLICATION_INVALID", "Expected activation generation is invalid")
    with store.transaction() as cursor:
        store._lock(cursor)
        target = store._get(cursor, value)
        active = store._active(cursor)
        if target["state"] not in ("sealed", "retained"):
            raise PublicationError(
                "PUBLICATION_UNAVAILABLE", "Publication is not sealed and available"
            )
        require_matching(
            relation_inventory(cursor, target["namespaces"]["data"]), target["inventory"]
        )
        store._require_immutable(cursor, target)
        verify_reader_boundary(cursor)
        if active["publication_id"] == value:
            if view_inventory(cursor, "published", catalog=True) != active["view_inventory"]:
                raise PublicationError("PUBLICATION_CONFLICT", "Active consumption views changed")
            return {"status": "noop", **active}
        if (expected_id, expected_generation) != (active["publication_id"], active["generation"]):
            raise PublicationError(
                "PUBLICATION_STALE", "Expected active identity or generation changed"
            )
        if restore and not target["ever_activated"]:
            raise PublicationError(
                "PUBLICATION_INVALID", "Restore requires a previously activated publication"
            )
        cursor.execute(
            "SELECT count(*) FROM publication_control.publications WHERE state='retained'"
        )
        if target["state"] == "sealed" and one(cursor)[0] >= MAX_RETAINED:
            raise PublicationError("PUBLICATION_CAPACITY", "Retention capacity is full")
        for name in (*OUTPUT_RELATIONS, "publication_metadata"):
            signature = _columns(cursor, target["namespaces"]["read"], name)
            previous = _columns(cursor, "published", name)
            if not signature or (previous and previous != signature):
                raise PublicationError(
                    "PUBLICATION_SCHEMA", "Consumer view signature is incompatible"
                )
            cursor.execute(
                sql.SQL("CREATE OR REPLACE VIEW published.{} AS SELECT {} FROM {}.{}").format(
                    sql.Identifier(name),
                    sql.SQL(",").join(sql.Identifier(column[0]) for column in signature),
                    sql.Identifier(target["namespaces"]["read"]),
                    sql.Identifier(name),
                )
            )
            cursor.execute(
                sql.SQL("ALTER VIEW published.{} OWNER TO {}").format(
                    sql.Identifier(name), sql.Identifier(OWNER_ROLE)
                )
            )
        store.checkpoint("activation_after_views")
        stable_inventory = view_inventory(cursor, "published", catalog=True)
        old_protected = {active["publication_id"], active["previous_id"]} - {None}
        new_protected = {value, active["publication_id"]} - {None}
        extending = old_protected - new_protected
        if active["publication_id"] is not None:
            extending.add(active["publication_id"])
            cursor.execute(
                "UPDATE publication_control.publications SET deactivated_at=clock_timestamp() "
                "WHERE publication_id=%s",
                (active["publication_id"],),
            )
        if extending:
            cursor.execute(
                "UPDATE publication_control.publications SET retain_until="
                "greatest(coalesce(retain_until,'-infinity'),"
                "clock_timestamp()+interval '24 hours') "
                "WHERE publication_id=ANY(%s)",
                (list(extending),),
            )
        cursor.execute(
            "UPDATE publication_control.active SET publication_id=%s,generation=generation+1,"
            "previous_id=%s,activated_at=clock_timestamp(),view_inventory=%s WHERE singleton "
            "RETURNING generation,activated_at",
            (value, active["publication_id"], Jsonb(stable_inventory)),
        )
        generation, activated_at = one(cursor)
        store.checkpoint("activation_after_pointer")
        cursor.execute(
            "UPDATE publication_control.publications SET state='retained',ever_activated=true "
            "WHERE publication_id=%s",
            (value,),
        )
        cursor.execute(
            "INSERT INTO publication_control.activations VALUES(%s,%s,%s,%s,%s)",
            (
                generation,
                value,
                active["publication_id"],
                "restoration" if restore else "activation",
                activated_at,
            ),
        )
        store.checkpoint("activation_after_event")
        grant_consumption(cursor, target["namespaces"]["read"])
        grant_consumption(cursor, "published")
        _discard_workspace(cursor, value)
        return {
            "status": "restored" if restore else "activated",
            "publication_id": value,
            "generation": generation,
            "previous_id": active["publication_id"],
            "activated_at": activated_at,
        }


def _eligible(cursor: psycopg.Cursor[Any], active: dict[str, Any]) -> list[str]:
    cursor.execute(
        "SELECT publication_id FROM publication_control.publications "
        "WHERE state='retained' AND retain_until<=clock_timestamp() "
        "AND publication_id IS DISTINCT FROM %s AND publication_id IS DISTINCT FROM %s "
        "ORDER BY created_at,publication_id",
        (active["publication_id"], active["previous_id"]),
    )
    return [row[0] for row in cursor.fetchall()]


def cleanup_preview(store: PublicationStore) -> list[dict[str, Any]]:
    with store.transaction() as cursor:
        store._lock(cursor)
        active = store._active(cursor)
        return [store._get(cursor, value) for value in _eligible(cursor, active)]


def cleanup(store: PublicationStore, values: list[str]) -> dict[str, Any]:
    values = [publication_id(value) for value in values]
    if len(values) != len(set(values)):
        raise PublicationError("PUBLICATION_INVALID", "Cleanup IDs must be distinct")
    with store.transaction() as cursor:
        store._lock(cursor)
        eligible = set(_eligible(cursor, store._active(cursor)))
        if not set(values).issubset(eligible):
            raise PublicationError(
                "PUBLICATION_PROTECTED", "Cleanup selection is protected or not expired"
            )
        for value in values:
            spaces = namespaces(value)
            _check_external_dependencies(cursor, (spaces["data"], spaces["read"]))
            _drop_schema(cursor, spaces["read"], set((*OUTPUT_RELATIONS, "publication_metadata")))
            store.checkpoint("cleanup_after_views")
            _drop_schema(cursor, spaces["data"], set(OUTPUT_RELATIONS))
            cursor.execute(
                "UPDATE publication_control.publications SET state='pruned' "
                "WHERE publication_id=%s",
                (value,),
            )
        return {"status": "cleaned", "publication_ids": values}


def discard(store: PublicationStore, value: str) -> dict[str, Any]:
    value = publication_id(value)
    with store.transaction() as cursor:
        store._lock(cursor)
        row = store._get(cursor, value)
        if row["ever_activated"] or row["state"] not in ("building", "failed", "sealed"):
            raise PublicationError(
                "PUBLICATION_PROTECTED", "Only private candidates can be discarded"
            )
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (candidate_lock_id(value),))
        if not one(cursor)[0]:
            raise PublicationError("PUBLICATION_CANDIDATE_BUSY", "A live build owns this candidate")
        spaces = namespaces(value)
        all_schemas = tuple(spaces.values())
        _check_external_dependencies(cursor, all_schemas)
        _drop_schema(cursor, spaces["read"], set((*OUTPUT_RELATIONS, "publication_metadata")))
        _drop_schema(cursor, spaces["data"], set(OUTPUT_RELATIONS))
        _discard_workspace(cursor, value)
        cursor.execute(
            "UPDATE publication_control.publications SET state='discarded' WHERE publication_id=%s",
            (value,),
        )
        return {"status": "discarded", "publication_id": value}


def _discard_workspace(cursor: psycopg.Cursor[Any], value: str) -> None:
    spaces = namespaces(value)
    schemas = tuple(spaces[key] for key in ("analytics", "intermediate", "staging", "input"))
    _check_external_dependencies(cursor, schemas)
    allowed = set(OUTPUT_RELATIONS) | {
        "_gold_build_binding",
        "int_contract_enriched",
        "stg_silver_contract",
        "stg_population_coverage",
        "stg_contract_observation_condition",
        "stg_contract_observation_link",
        "stg_quality_summary",
        "contract_current",
        "contract_current_population",
        "contract_current_population_binding",
        "contract_current_observation_condition",
        "contract_current_observation_link",
        "contract_current_quality",
    }
    allowed |= {
        name + suffix for name in tuple(allowed) for suffix in ("__dbt_tmp", "__dbt_backup")
    }
    for schema in schemas:
        _drop_schema(cursor, schema, allowed)


def _check_external_dependencies(cursor: psycopg.Cursor[Any], schemas: tuple[str, ...]) -> None:
    cursor.execute(
        "SELECT DISTINCT dependent_ns.nspname,dependent.relname "
        "FROM pg_depend d JOIN pg_rewrite r ON d.classid='pg_rewrite'::regclass AND r.oid=d.objid "
        "JOIN pg_class dependent ON dependent.oid=r.ev_class "
        "JOIN pg_namespace dependent_ns ON dependent_ns.oid=dependent.relnamespace "
        "JOIN pg_class source ON d.refclassid='pg_class'::regclass AND source.oid=d.refobjid "
        "JOIN pg_namespace source_ns ON source_ns.oid=source.relnamespace "
        "WHERE source_ns.nspname=ANY(%s) AND NOT(dependent_ns.nspname=ANY(%s))",
        (list(schemas), list(schemas)),
    )
    if cursor.fetchall():
        raise PublicationError("PUBLICATION_DEPENDENCY", "Unexpected external relation dependency")


def _drop_schema(cursor: psycopg.Cursor[Any], schema: str, allowed: set[str]) -> None:
    cursor.execute("SELECT oid FROM pg_namespace WHERE nspname=%s", (schema,))
    if cursor.fetchone() is None:
        return
    cursor.execute(
        "SELECT c.relname,c.relkind FROM pg_class c JOIN pg_namespace n "
        "ON n.oid=c.relnamespace WHERE n.nspname=%s "
        "AND c.relkind IN ('r','p','v','m','f','S') ORDER BY c.relname",
        (schema,),
    )
    relations = cursor.fetchall()
    if any(name not in allowed or kind not in ("r", "v") for name, kind in relations):
        raise PublicationError(
            "PUBLICATION_DEPENDENCY", "Owned namespace contains unexpected objects"
        )
    for kind, statement in (("v", "DROP VIEW {}"), ("r", "DROP TABLE {}")):
        selected = [sql.Identifier(schema, name) for name, actual in relations if actual == kind]
        if selected:
            cursor.execute(sql.SQL(statement).format(sql.SQL(",").join(selected)))
    # RESTRICT detects unknown functions/types/objects rather than dropping unrelated dependents.
    cursor.execute(sql.SQL("DROP SCHEMA {} RESTRICT").format(sql.Identifier(schema)))
