"""Transactional synchronization of a verified Silver snapshot into PostgreSQL."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import psycopg
from psycopg import sql

from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA, VerifiedSilverBundle
from observatorio_secop.warehouse.config import WarehouseConfig
from observatorio_secop.warehouse.errors import WarehouseConflictError, WarehouseLoadError
from observatorio_secop.warehouse.logging import log_event


@dataclass(frozen=True)
class LoadResult:
    silver_version_id: str
    status: str
    source_rows: int
    inserted_rows: int
    updated_rows: int
    deleted_rows: int

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


class WarehouseLoader:
    def __init__(self, config: WarehouseConfig) -> None:
        self.config = config

    def load(self, bundle: VerifiedSilverBundle) -> LoadResult:
        log_event(
            "warehouse_load_started", silver_version_id=bundle.version_id, rows=bundle.row_count
        )
        try:
            with psycopg.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=10,
            ) as connection:
                result = self._load(connection, bundle)
        except WarehouseConflictError:
            raise
        except psycopg.Error as error:
            raise WarehouseLoadError(
                f"PostgreSQL load failed ({error.__class__.__name__})"
            ) from error
        log_event("warehouse_load_finished", **result.as_dict())
        return result

    def _load(
        self, connection: psycopg.Connection[Any], bundle: VerifiedSilverBundle
    ) -> LoadResult:
        schema = sql.Identifier(self.config.schema)
        current = sql.Identifier(self.config.contract_table)
        audit = sql.Identifier(self.config.audit_table)
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (self.config.advisory_lock_id,))
            self._ensure_relations(cursor, schema, current, audit)
            cursor.execute(
                sql.SQL("SELECT manifest_sha256 FROM {}.{} WHERE silver_version_id = %s").format(
                    schema, audit
                ),
                (bundle.version_id,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing[0] != bundle.manifest_sha256:
                    raise WarehouseConflictError(
                        f"Silver version {bundle.version_id} has conflicting immutable content"
                    )
                return LoadResult(bundle.version_id, "noop", bundle.row_count, 0, 0, 0)

            copy_columns = [field.name for field in EXPECTED_SCHEMA]
            cursor.execute(
                sql.SQL(
                    "CREATE TEMP TABLE incoming_contracts ON COMMIT DROP AS "
                    "SELECT {} FROM {}.{} WITH NO DATA"
                ).format(
                    sql.SQL(", ").join(map(sql.Identifier, copy_columns)),
                    schema,
                    current,
                )
            )
            statement = sql.SQL("COPY incoming_contracts ({}) FROM STDIN").format(
                sql.SQL(", ").join(map(sql.Identifier, copy_columns))
            )
            with cursor.copy(statement) as copy:
                for batch in bundle.table.to_batches(max_chunksize=10_000):
                    for row in batch.to_pylist():
                        copy.write_row(tuple(row[name] for name in copy_columns))

            cursor.execute(
                "SELECT count(*), count(DISTINCT contract_key), "
                "count(*) FILTER (WHERE contract_key IS NULL OR source_contract_id IS NULL) "
                "FROM incoming_contracts"
            )
            staging_counts = cursor.fetchone()
            if staging_counts is None:
                raise WarehouseLoadError("PostgreSQL staging validation returned no result")
            total, distinct_keys, invalid = staging_counts
            if (total, distinct_keys, invalid) != (bundle.row_count, bundle.row_count, 0):
                raise WarehouseLoadError("PostgreSQL staging validation failed")

            cursor.execute(
                sql.SQL(
                    "SELECT count(*) FROM incoming_contracts i JOIN {}.{} c USING (contract_key) "
                    "WHERE c.payload_sha256 IS DISTINCT FROM i.payload_sha256 "
                    "OR c.silver_version_id IS DISTINCT FROM i.silver_version_id"
                ).format(schema, current)
            )
            updated_result = cursor.fetchone()
            if updated_result is None:
                raise WarehouseLoadError("PostgreSQL update count returned no result")
            updated_rows = updated_result[0]
            cursor.execute(
                sql.SQL(
                    "SELECT count(*) FROM incoming_contracts i "
                    "LEFT JOIN {}.{} c USING (contract_key) "
                    "WHERE c.contract_key IS NULL"
                ).format(schema, current)
            )
            inserted_result = cursor.fetchone()
            if inserted_result is None:
                raise WarehouseLoadError("PostgreSQL insert count returned no result")
            inserted_rows = inserted_result[0]
            cursor.execute(
                sql.SQL(
                    "DELETE FROM {}.{} c WHERE NOT EXISTS "
                    "(SELECT 1 FROM incoming_contracts i WHERE i.contract_key = c.contract_key)"
                ).format(schema, current)
            )
            deleted_rows = cursor.rowcount

            all_columns = copy_columns + ["silver_manifest_sha256", "loaded_at"]
            select_columns = [sql.Identifier(name) for name in copy_columns]
            assignments = [
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name))
                for name in all_columns
                if name != "contract_key"
            ]
            cursor.execute(
                sql.SQL(
                    "INSERT INTO {}.{} AS target ({}) SELECT {}, %s, %s FROM incoming_contracts "
                    "ON CONFLICT (contract_key) DO UPDATE SET {} "
                    "WHERE target.payload_sha256 IS DISTINCT FROM EXCLUDED.payload_sha256 "
                    "OR target.silver_version_id IS DISTINCT FROM EXCLUDED.silver_version_id"
                ).format(
                    schema,
                    current,
                    sql.SQL(", ").join(map(sql.Identifier, all_columns)),
                    sql.SQL(", ").join(select_columns),
                    sql.SQL(", ").join(assignments),
                ),
                (bundle.manifest_sha256, bundle.manifest_finished_at),
            )
            cursor.execute(
                sql.SQL(
                    "INSERT INTO {}.{} (silver_version_id, manifest_sha256, manifest_finished_at, "
                    "source_rows, inserted_rows, updated_rows, deleted_rows, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, 'applied')"
                ).format(schema, audit),
                (
                    bundle.version_id,
                    bundle.manifest_sha256,
                    bundle.manifest_finished_at,
                    bundle.row_count,
                    inserted_rows,
                    updated_rows,
                    deleted_rows,
                ),
            )
        return LoadResult(
            bundle.version_id,
            "applied",
            bundle.row_count,
            inserted_rows,
            updated_rows,
            deleted_rows,
        )

    @staticmethod
    def _ensure_relations(
        cursor: psycopg.Cursor[Any],
        schema: sql.Identifier,
        current: sql.Identifier,
        audit: sql.Identifier,
    ) -> None:
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema))
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    silver_version_id text NOT NULL,
                    contract_key text PRIMARY KEY,
                    source_dataset_id text NOT NULL,
                    source_contract_id text NOT NULL,
                    contract_reference text,
                    procurement_process_id text,
                    entity_name text,
                    department_raw text NOT NULL,
                    department_name text NOT NULL,
                    department_key text NOT NULL,
                    municipality_raw text,
                    municipality_name text,
                    municipality_key text,
                    municipality_status text NOT NULL,
                    is_medellin boolean NOT NULL,
                    is_valle_de_aburra boolean NOT NULL,
                    contract_status text,
                    main_category_code text,
                    process_description text,
                    contract_type text,
                    procurement_method text,
                    signing_date_raw text,
                    signing_date date,
                    start_date_raw text,
                    start_date date,
                    end_date_raw text,
                    end_date date,
                    contract_value_raw text NOT NULL,
                    contract_value_cop numeric(20,2) NOT NULL CHECK (contract_value_cop >= 0),
                    source_updated_at_raw text,
                    source_updated_at timestamp,
                    source_url text,
                    quality_warning_codes text[] NOT NULL,
                    payload_sha256 text NOT NULL,
                    bronze_run_id text NOT NULL,
                    bronze_page_path text NOT NULL,
                    bronze_lane text NOT NULL,
                    bronze_page_sequence integer NOT NULL,
                    bronze_row_ordinal bigint NOT NULL,
                    bronze_fetched_at_utc timestamp NOT NULL,
                    signing_year text NOT NULL,
                    silver_manifest_sha256 text NOT NULL,
                    loaded_at timestamptz NOT NULL
                )
                """
            ).format(schema, current)
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    silver_version_id text PRIMARY KEY,
                    manifest_sha256 text NOT NULL,
                    manifest_finished_at timestamptz NOT NULL,
                    attempted_at timestamptz NOT NULL DEFAULT current_timestamp,
                    source_rows bigint NOT NULL CHECK (source_rows >= 0),
                    inserted_rows bigint NOT NULL CHECK (inserted_rows >= 0),
                    updated_rows bigint NOT NULL CHECK (updated_rows >= 0),
                    deleted_rows bigint NOT NULL CHECK (deleted_rows >= 0),
                    status text NOT NULL CHECK (status IN ('applied'))
                )
                """
            ).format(schema, audit)
        )
