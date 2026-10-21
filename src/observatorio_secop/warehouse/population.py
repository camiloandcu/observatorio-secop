"""Population guards and companion evidence inside the warehouse transaction."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from observatorio_secop.identity import key_set_digest
from observatorio_secop.population import PopulationScope, digest_json, legacy_coverage
from observatorio_secop.warehouse.config import IDENTIFIER, WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConfigurationError, WarehouseConflictError

Identity = tuple[str, str]


class PopulationStore:
    """Use only after the caller holds the existing publication advisory lock."""

    def __init__(self, config: WarehouseConfig) -> None:
        suffixes = (
            "_population",
            "_population_binding",
            "_observation_condition",
            "_observation_link",
            "_population_adoption",
        )
        names = [config.contract_table + suffix for suffix in suffixes]
        existing = [
            config.contract_table,
            config.audit_table,
            *[config.audit_table + s for s in ("_active", "_activation", "_identity", "_schema")],
        ]
        if len(set(names + existing)) != len(names + existing) or any(
            not IDENTIFIER.fullmatch(name) or len(name.encode()) > 63 for name in names
        ):
            raise WarehouseConfigurationError("Population relation names collide or exceed limits")
        self.versions, self.binding, self.conditions, self.links, self.adoptions = [
            sql.Identifier(config.schema, name) for name in names
        ]
        self.current = sql.Identifier(config.schema, config.contract_table)

    def ensure(self, cursor: psycopg.Cursor[Any]) -> None:
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (silver_version_id text NOT NULL, "
                "manifest_sha256 text NOT NULL, scope_id text, receipt jsonb NOT NULL, "
                "evidence_sha256 text NOT NULL, PRIMARY KEY(silver_version_id,manifest_sha256))"
            ).format(self.versions)
        )
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (singleton boolean PRIMARY KEY CHECK(singleton), "
                "scope_id text NOT NULL, scope jsonb NOT NULL)"
            ).format(self.binding)
        )
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (silver_version_id text NOT NULL, "
                "manifest_sha256 text NOT NULL, scope_id text NOT NULL, "
                "contract_key text NOT NULL, "
                "condition jsonb NOT NULL, "
                "PRIMARY KEY(silver_version_id,manifest_sha256,contract_key))"
            ).format(self.conditions)
        )
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (silver_version_id text NOT NULL, "
                "manifest_sha256 text NOT NULL, scope_id text NOT NULL, "
                "contract_key text NOT NULL, "
                "link_id text NOT NULL, evidence jsonb NOT NULL, "
                "PRIMARY KEY(silver_version_id,manifest_sha256,link_id))"
            ).format(self.links)
        )
        cursor.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (silver_version_id text NOT NULL, "
                "manifest_sha256 text NOT NULL, scope_id text NOT NULL, keys_sha256 text NOT NULL, "
                "proof jsonb NOT NULL, PRIMARY KEY(silver_version_id,manifest_sha256))"
            ).format(self.adoptions)
        )

    def guard(
        self,
        cursor: psycopg.Cursor[Any],
        bundle: Any,
        active: Identity | None,
        registered: bool,
        *,
        restore: bool = False,
        expected_current: Identity | None = None,
    ) -> None:
        identity = (bundle.version_id, bundle.manifest_sha256)
        receipt = getattr(bundle, "coverage", None)
        cursor.execute(sql.SQL("SELECT scope_id FROM {} WHERE singleton").format(self.binding))
        binding = cursor.fetchone()
        if receipt is None:
            if active != identity:
                if not registered or not restore:
                    raise WarehouseConflictError(
                        "New legacy activation requires a scoped candidate; "
                        "historical restore must be explicit"
                    )
                self._restore_guard(active, registered, expected_current)
            if binding is not None:
                cursor.execute(
                    sql.SQL(
                        "SELECT scope_id FROM {} WHERE silver_version_id=%s AND manifest_sha256=%s"
                    ).format(self.adoptions),
                    identity,
                )
                adoption = cursor.fetchone()
                if adoption != binding or not registered:
                    raise WarehouseConflictError(
                        "Legacy version has no compatible population adoption"
                    )
                if active != identity and not restore:
                    raise WarehouseConflictError(
                        "Adopted legacy activation requires explicit restoration"
                    )
            if restore and active != identity:
                self._restore_guard(active, registered, expected_current)
            return
        if receipt["mode"] != "authoritative_candidate":
            raise WarehouseConflictError("Preview cannot activate warehouse population")
        scope_id = receipt["scope_id"]
        if registered and binding is None:
            raise WarehouseConflictError(
                "Registered population has lost its persistent scope binding"
            )
        if binding is not None and binding[0] != scope_id:
            raise WarehouseConflictError("Population scope differs from bound workspace")
        if active == identity:
            self._verify_existing(cursor, bundle)
            return
        if restore:
            self._restore_guard(active, registered, expected_current)
            self._verify_existing(cursor, bundle)
            return
        if receipt["state"] != "complete" or receipt["basis"] != "bounded_scan_observed":
            raise WarehouseConflictError(
                "Population activation requires complete bounded acquisition"
            )
        parent = receipt["expected_parent"]
        expected = (
            None if parent is None else (parent["silver_version_id"], parent["manifest_sha256"])
        )
        if expected != active:
            raise WarehouseConflictError("Population candidate has stale active parent")
        baseline = receipt["baseline"]
        if active is None:
            if baseline is not None:
                raise WarehouseConflictError("Initial population has an unexpected baseline")
        else:
            if binding is None:
                raise WarehouseConflictError(
                    "Existing legacy population needs explicit verified adoption"
                )
            if (
                baseline is None
                or (baseline["silver_version_id"], baseline["manifest_sha256"]) != active
            ):
                raise WarehouseConflictError("Population baseline differs from active snapshot")
            cursor.execute(sql.SQL("SELECT contract_key FROM {}").format(self.current))
            active_keys = {row[0] for row in cursor.fetchall()}
            incoming_keys = set(bundle.table.column("contract_key").to_pylist())
            if (
                not active_keys <= incoming_keys
                or key_set_digest(sorted(active_keys)) != baseline["keys_sha256"]
            ):
                raise WarehouseConflictError(
                    "Population candidate does not preserve its active baseline"
                )
        scope = PopulationScope.from_dict(receipt["scope"])
        for row in bundle.table.select(["signing_date", "department_name"]).to_pylist():
            if not scope.contains(row["signing_date"], row["department_name"]):
                raise WarehouseConflictError("Population member lies outside its declared scope")
        if binding is None:
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (true,%s,%s)").format(self.binding),
                (scope_id, Jsonb(receipt["scope"])),
            )

    @staticmethod
    def _restore_guard(
        active: Identity | None, registered: bool, expected_current: Identity | None
    ) -> None:
        if not registered or expected_current != active:
            raise WarehouseConflictError(
                "Restoration needs registered target and exact expected current"
            )

    @staticmethod
    def _evidence(bundle: Any) -> dict[str, Any]:
        return {
            "coverage": getattr(bundle, "coverage", None) or legacy_coverage(),
            "conditions": sorted(
                getattr(bundle, "condition_summaries", []), key=lambda row: row["contract_key"]
            ),
            "links": sorted(getattr(bundle, "condition_links", []), key=lambda row: row["link_id"]),
        }

    def _verify_existing(self, cursor: psycopg.Cursor[Any], bundle: Any) -> None:
        identity = (bundle.version_id, bundle.manifest_sha256)
        cursor.execute(
            sql.SQL(
                "SELECT receipt, evidence_sha256 FROM {} "
                "WHERE silver_version_id=%s AND manifest_sha256=%s"
            ).format(self.versions),
            identity,
        )
        registered = cursor.fetchone()
        cursor.execute(
            sql.SQL(
                "SELECT condition FROM {} WHERE silver_version_id=%s AND manifest_sha256=%s "
                "ORDER BY contract_key"
            ).format(self.conditions),
            identity,
        )
        summaries = [row[0] for row in cursor.fetchall()]
        cursor.execute(
            sql.SQL(
                "SELECT evidence FROM {} WHERE silver_version_id=%s AND manifest_sha256=%s "
                "ORDER BY link_id"
            ).format(self.links),
            identity,
        )
        links = [row[0] for row in cursor.fetchall()]
        expected = self._evidence(bundle)
        if (
            registered is None
            or registered[0] != expected["coverage"]
            or registered[1] != digest_json(expected)
            or summaries != expected["conditions"]
            or links != expected["links"]
        ):
            raise WarehouseConflictError("Population evidence differs from registered metadata")

    def adopt(self, cursor: psycopg.Cursor[Any], bundle: Any, record: dict[str, Any]) -> None:
        from observatorio_secop.warehouse.bundle import IDENTITY_SCHEMA

        scope = PopulationScope.from_dict(record["scope"])
        identity = (bundle.version_id, bundle.manifest_sha256)
        cursor.execute(
            sql.SQL("SELECT scope_id, scope FROM {} WHERE singleton").format(self.binding)
        )
        bound = cursor.fetchone()
        if bound is not None and bound != (scope.scope_id, scope.as_dict()):
            raise WarehouseConflictError("Warehouse is already bound to another population")
        cursor.execute(sql.SQL("SELECT contract_key FROM {}").format(self.current))
        actual_keys = {row[0] for row in cursor.fetchall()}
        names = IDENTITY_SCHEMA.names
        cursor.execute(
            sql.SQL("SELECT {} FROM {} ORDER BY contract_key").format(
                sql.SQL(", ").join(map(sql.Identifier, names)), self.current
            )
        )
        actual_rows = cursor.fetchall()
        expected_rows = [
            tuple(row[name] for name in names)
            for row in sorted(bundle.table.to_pylist(), key=lambda row: row["contract_key"])
        ]
        if actual_rows != expected_rows:
            raise WarehouseConflictError("Adoption rows differ from the pinned original snapshot")
        if key_set_digest(sorted(actual_keys)) != record["keys_sha256"]:
            raise WarehouseConflictError("Adoption does not match active warehouse keys")
        cursor.execute(
            sql.SQL(
                "SELECT proof FROM {} WHERE silver_version_id=%s AND manifest_sha256=%s"
            ).format(self.adoptions),
            identity,
        )
        old = cursor.fetchone()
        if old is not None and old[0] != record:
            raise WarehouseConflictError("Immutable population adoption conflict")
        if bound is None:
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (true,%s,%s)").format(self.binding),
                (scope.scope_id, Jsonb(scope.as_dict())),
            )
        if old is None:
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s)").format(self.adoptions),
                (*identity, scope.scope_id, record["keys_sha256"], Jsonb(record)),
            )
        self.persist(cursor, bundle)

    def persist(self, cursor: psycopg.Cursor[Any], bundle: Any) -> None:
        identity = (bundle.version_id, bundle.manifest_sha256)
        evidence = self._evidence(bundle)
        receipt = getattr(bundle, "coverage", None)
        scope_id = receipt["scope_id"] if receipt else None
        checksum = digest_json(evidence)
        cursor.execute(
            sql.SQL(
                "SELECT evidence_sha256 FROM {} WHERE silver_version_id=%s AND manifest_sha256=%s"
            ).format(self.versions),
            identity,
        )
        prior = cursor.fetchone()
        if prior is not None:
            if prior != (checksum,):
                raise WarehouseConflictError("Immutable population evidence conflict")
            self._verify_existing(cursor, bundle)
            return
        cursor.execute(
            sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s)").format(self.versions),
            (*identity, scope_id, Jsonb(evidence["coverage"]), checksum),
        )
        for row in evidence["conditions"]:
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s)").format(self.conditions),
                (*identity, scope_id, row["contract_key"], Jsonb(row)),
            )
        for row in evidence["links"]:
            cursor.execute(
                sql.SQL("INSERT INTO {} VALUES (%s,%s,%s,%s,%s,%s)").format(self.links),
                (*identity, scope_id, row["contract_key"], row["link_id"], Jsonb(row)),
            )
