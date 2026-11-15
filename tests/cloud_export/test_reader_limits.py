"""Real SQL admission/timeout tests; descriptor binding has its own integration test."""

from __future__ import annotations

import os
import time
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.cloud_export.budget import Budget
from observatorio_secop.cloud_export.contract import Column, Relation, load_contract
from observatorio_secop.cloud_export.errors import ExportError
from observatorio_secop.cloud_export.reader import batches
from observatorio_secop.publication.errors import PublicationError
from warehouse.test_dbt_postgres import _warehouse_config

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="Requires dedicated disposable PostgreSQL")


@pytest.fixture
def sql_source(tmp_path, monkeypatch):
    assert DSN and conninfo_to_dict(DSN)["dbname"].startswith("secop_ci_")
    config = _warehouse_config(tmp_path, conninfo_to_dict(DSN))
    namespace = "export_test_" + uuid.uuid4().hex
    with psycopg.connect(DSN, autocommit=True) as db:
        db.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
        monkeypatch.setattr("observatorio_secop.cloud_export.reader.checked", lambda *a: None)
        yield config, db, namespace
        db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))


def read(config, namespace, column, budget):
    contract = load_contract()
    relation = Relation("sample", (column,), False)
    return list(batches(config, {"read_namespace": namespace}, relation, contract, budget))


@pytest.mark.parametrize("value", ["'[0:1]={A,B}'::text[]", "ARRAY[['A'],['B']]::text[]"])
def test_array_shape_rejected_before_fetch(sql_source, value):
    config, db, namespace = sql_source
    db.execute(
        sql.SQL("CREATE VIEW {} AS SELECT {} AS items").format(
            sql.Identifier(namespace, "sample"), sql.SQL(value)
        )
    )
    with pytest.raises(ExportError, match="array dimensions"):
        read(config, namespace, Column("items", "text[]", "array-json-v1"), Budget())


def test_row_size_admission_and_combined_rows(sql_source):
    config, db, namespace = sql_source
    db.execute(
        sql.SQL(
            "CREATE VIEW {} AS SELECT repeat('a',100) AS label FROM generate_series(1,2)"
        ).format(sql.Identifier(namespace, "sample"))
    )
    column = Column("label", "text", "text")
    with pytest.raises(ExportError, match="row admission"):
        read(config, namespace, column, Budget(row_bytes=100))
    with pytest.raises(ExportError, match="row admission"):
        read(config, namespace, column, Budget(max_rows=1))
    assert len(read(config, namespace, column, Budget(max_rows=2, row_bytes=140))[0]) == 2


def test_blocked_statement_is_limited_to_remaining_deadline(sql_source):
    config, db, namespace = sql_source
    db.execute(
        sql.SQL("CREATE VIEW {} AS SELECT 'value'::text AS label FROM pg_sleep(3)").format(
            sql.Identifier(namespace, "sample")
        )
    )
    start = time.monotonic()
    with pytest.raises((PublicationError, ExportError)) as error:
        read(config, namespace, Column("label", "text", "text"), Budget(seconds=0.2))
    assert getattr(error.value, "code", "") in ("PUBLICATION_QUERY_TIMEOUT", "EXPORT_LIMIT")
    assert time.monotonic() - start < 2


def test_cursor_closes_on_partial_failure(sql_source, monkeypatch):
    config, db, namespace = sql_source
    db.execute(
        sql.SQL(
            "CREATE VIEW {} AS SELECT value::text AS label FROM generate_series(1,1001) value"
        ).format(sql.Identifier(namespace, "sample"))
    )
    calls = 0

    def expires(*args):
        nonlocal calls
        calls += 1
        if calls >= 3:
            raise PublicationError("PUBLICATION_EXPIRED", "test expiry after partial batch")

    monkeypatch.setattr("observatorio_secop.cloud_export.reader.checked", expires)
    contract = load_contract()
    relation = Relation("sample", (Column("label", "text", "text"),), False)
    stream = batches(config, {"read_namespace": namespace}, relation, contract, Budget())
    assert len(next(stream)) == 1000
    with pytest.raises(PublicationError, match="partial"):
        next(stream)
    assert (
        db.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND state='idle in transaction'"
        ).fetchone()[0]
        == 0
    )
