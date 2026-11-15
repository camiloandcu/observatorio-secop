"""Pinned read-only server cursors over reviewed publication projections."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from psycopg import sql

from observatorio_secop.publication.readers import _validate, reader_cursor
from observatorio_secop.warehouse.config import WarehouseConfig

from .budget import Budget
from .codecs import encode
from .contract import Column, Contract, Relation
from .errors import ExportError
from .minimization import Minimizer


def checked(config: WarehouseConfig, descriptor: dict[str, Any], budget: Budget) -> None:
    with reader_cursor(config, statement_timeout_ms=budget.remaining_ms()) as cursor:
        cursor.execute(
            "SELECT set_config('lock_timeout',%s,true)", (f"{budget.remaining_ms(5000)}ms",)
        )
        _validate(cursor, descriptor)
    budget.check()


ORDER_KEYS = {
    "fact_contract": "contract_key",
    "mart_contract_detail": "contract_key",
    "dim_entity": "entity_sk",
    "dim_supplier": "supplier_sk",
    "dim_municipality": "municipality_sk",
    "dim_modality": "modality_sk",
    "dim_date": "date_sk",
    "publication_metadata": "publication_id",
}


def order_expression(relation: Relation) -> sql.Composable:
    key = ORDER_KEYS.get(relation.name)
    if key is None:
        return sql.SQL('to_jsonb(v)::text COLLATE "C"')
    column = next(c for c in relation.columns if c.name == key)
    return (
        sql.SQL('{} COLLATE "C"').format(sql.Identifier(key))
        if column.codec == "text"
        else sql.Identifier(key)
    )


def expression(c: Column) -> sql.Composable:
    name = sql.Identifier(c.name)
    if c.codec in ("json-text-v1", "decimal-text-v1"):
        return sql.SQL("{}::text").format(name)
    if c.codec == "array-json-v1":
        return sql.SQL("array_to_json({})::text").format(name)
    return name


def batches(
    config: WarehouseConfig,
    descriptor: dict[str, Any],
    relation: Relation,
    contract: Contract,
    budget: Budget,
    minimizer: Minimizer | None = None,
) -> Iterator[list[dict[str, Any]]]:
    checked(config, descriptor, budget)
    namespace = descriptor["read_namespace"]
    target = sql.Identifier(namespace, relation.name)
    with reader_cursor(config, statement_timeout_ms=budget.remaining_ms()) as cursor:
        cursor.execute(
            "SELECT set_config('lock_timeout',%s,true)", (f"{budget.remaining_ms(5000)}ms",)
        )
        cursor.execute("SHOW server_encoding")
        encoding = cursor.fetchone()
        if encoding is None or encoding["server_encoding"] != "UTF8":
            raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Export requires UTF8 source encoding")
        cursor.execute(
            "SELECT a.attname,format_type(a.atttypid,a.atttypmod) AS dtype "
            "FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0 AND NOT a.attisdropped "
            "ORDER BY a.attnum",
            (namespace, relation.name),
        )
        observed = [(r["attname"], r["dtype"]) for r in cursor.fetchall()]
        expected = [
            (c.name, f"numeric({c.precision},{c.scale})" if c.codec == "decimal" else c.source_type)
            for c in relation.columns
        ]
        if observed != expected:
            error = ExportError(
                "EXPORT_UNSUPPORTED_SCHEMA", "Publication schema differs from contract"
            )
            error.safe_details = {
                "relation": relation.name,
                "observed": observed,
                "expected": expected,
            }
            raise error
        for c in relation.columns:
            if c.codec == "array-json-v1":
                cursor.execute(
                    sql.SQL(
                        "SELECT EXISTS(SELECT 1 FROM {} WHERE {} IS NOT NULL "
                        "AND cardinality({})>0 AND "
                        "(array_ndims({})<>1 OR array_lower({},1)<>1)) AS bad"
                    ).format(target, *[sql.Identifier(c.name)] * 4)
                )
                array_result = cursor.fetchone()
                assert array_result is not None
                if array_result["bad"]:
                    raise ExportError("EXPORT_UNSUPPORTED_SCHEMA", "Unsupported array dimensions")
        # Cast only projected columns. Text byte lengths plus framing and temporal
        # normalization reserve bound transfer before any source row is fetched.
        sizes = [
            sql.SQL("coalesce(octet_length(({})::text),0)+40").format(expression(c))
            for c in relation.columns
        ]
        cursor.execute(
            "SELECT set_config('statement_timeout',%s,true)", (f"{budget.remaining_ms()}ms",)
        )
        cursor.execute(
            sql.SQL("SELECT count(*) AS n,coalesce(max({}),0) AS largest FROM {}").format(
                sql.SQL("+").join(sizes), target
            )
        )
        stats = cursor.fetchone()
        assert stats is not None
        largest = int(stats["largest"])
        if largest > budget.row_bytes or budget.rows + stats["n"] > budget.max_rows:
            raise ExportError("EXPORT_LIMIT", "Source exceeds finite row admission")
        fetch_size = max(1, min(1000, budget.batch_bytes // max(1, largest)))
        projections = sql.SQL(",").join(
            sql.SQL("{} AS {}").format(expression(c), sql.Identifier(c.name))
            for c in relation.columns
        )
        # No global activation advisory lock survives the short checked() call.
        with cursor.connection.cursor(name="secop_export") as stream:
            cursor.execute(
                "SELECT set_config('statement_timeout',%s,true)", (f"{budget.remaining_ms()}ms",)
            )
            stream.execute(
                sql.SQL("SELECT {} FROM {} v ORDER BY {}").format(
                    projections, target, order_expression(relation)
                )
            )
            while True:
                checked(config, descriptor, budget)
                cursor.execute(
                    "SELECT set_config('statement_timeout',%s,true)",
                    (f"{budget.remaining_ms()}ms",),
                )
                values = stream.fetchmany(fetch_size)
                if not values:
                    break
                if minimizer is not None:
                    minimizer.apply(relation.name, values)
                rows = []
                for row in values:
                    encoded = {}
                    for c in relation.columns:
                        try:
                            encoded[c.name] = encode(row[c.name], c, contract)
                        except ExportError as error:
                            error.safe_details = {
                                "relation": relation.name,
                                "column": c.name,
                                "codec": c.codec,
                            }
                            raise
                    rows.append(encoded)
                budget.check()
                yield rows
    checked(config, descriptor, budget)
