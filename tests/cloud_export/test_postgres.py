"""Exporter integration uses only the explicitly supplied disposable test database."""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.cloud_export.budget import Budget
from observatorio_secop.cloud_export.build import build, supervised_verify
from observatorio_secop.cloud_export.contract import load_contract
from observatorio_secop.cloud_export.errors import ExportError
from observatorio_secop.cloud_export.reader import batches
from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.readers import resolve
from warehouse.test_publication_acceptance import (
    _activate,
    _candidate,
)
from warehouse.test_publication_acceptance import (
    publication_database as _publication_database,
)

publication_database = _publication_database

DSN = os.environ.get("WAREHOUSE_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="Requires dedicated disposable PostgreSQL and dbt")


def reader_config(fixture, monkeypatch):
    config, store, root = fixture
    assert DSN and conninfo_to_dict(DSN)["dbname"].startswith("secop_ci_")
    password = uuid.uuid4().hex
    with psycopg.connect(**config.connection_kwargs, autocommit=True) as db:
        db.execute(sql.SQL("ALTER ROLE secop_bi_reader PASSWORD {}").format(sql.Literal(password)))
    reader = replace(config, user="secop_bi_reader", password=password)
    for key, value in {
        "POSTGRES_HOST": reader.host,
        "POSTGRES_PORT": str(reader.port),
        "POSTGRES_DB": reader.dbname,
        "POSTGRES_USER": reader.user,
        "POSTGRES_PASSWORD": password,
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("PGTZ", "Pacific/Auckland")
    return reader


def test_real_reader_two_builds_noop_and_conflict(publication_database, tmp_path, monkeypatch):
    config, store, _ = publication_database
    publication_id, _, _ = _candidate(publication_database, "a" * 64, "123.45", extra=True)
    _activate(store, publication_id)
    reader_config(publication_database, monkeypatch)
    config_path = Path("config/warehouse.yaml")
    outputs = [tmp_path / "left", tmp_path / "right"]
    for output in outputs:
        result = build(
            config_path=config_path,
            silver_root=config.silver_root,
            output=output,
            publication_id=publication_id,
            export_id=None,
        )
        assert result["status"] == "built"
    left, right = [p / result["export_id"] for p in outputs]
    assert (left / "manifest.json").read_bytes() == (right / "manifest.json").read_bytes()
    manifest = supervised_verify(left)
    assert len(manifest["relations"]) == 13
    assert manifest["exact_source_comparison"] == {"relations": 13, "mismatches": 0}
    assert manifest["relations"]["silver_valid"]["rows"] == 2
    assert manifest["relations"]["mart_contract_detail"]["contract_value_cop_sum"] == "173.45"
    assert (
        build(
            config_path=config_path,
            silver_root=config.silver_root,
            output=outputs[0],
            publication_id=publication_id,
            export_id=None,
        )["status"]
        == "unchanged"
    )
    before = (left / "manifest.json").read_bytes()
    (left / "extra").write_text("unexpected")
    with pytest.raises(ExportError):
        build(
            config_path=config_path,
            silver_root=config.silver_root,
            output=outputs[0],
            publication_id=publication_id,
            export_id=None,
        )
    assert (left / "manifest.json").read_bytes() == before


def test_cursor_keeps_pin_across_activation_and_expiry_rejects(publication_database, monkeypatch):
    _, store, _ = publication_database
    a, _, _ = _candidate(publication_database, "a" * 64, "123.45", extra=True)
    _activate(store, a)
    b, _, _ = _candidate(publication_database, "b" * 64, "200.00", extra=True)
    reader = reader_config(publication_database, monkeypatch)
    descriptor = resolve(reader, a)
    contract = load_contract()
    stream = batches(reader, descriptor, contract.relations["fact_contract"], contract, Budget())
    first = next(stream)
    _activate(store, b)  # Finishes while A's named read cursor is still open.
    assert all(r["publication_id"] == a for r in first)
    assert list(stream) == []
    assert all(
        r["publication_id"] == a
        for batch in batches(
            reader, descriptor, contract.relations["mart_contract_detail"], contract, Budget()
        )
        for r in batch
    )
    past = datetime.now(UTC) - timedelta(hours=2)
    expired = {
        **descriptor,
        "resolved_at": past.isoformat(),
        "operation_expires_at": (past + timedelta(hours=1)).isoformat(),
    }
    with pytest.raises(PublicationError) as failure:
        list(batches(reader, expired, contract.relations["fact_contract"], contract, Budget()))
    assert failure.value.code == "PUBLICATION_EXPIRED"


def test_reader_schema_drift_and_denial(publication_database, monkeypatch):
    config, store, _ = publication_database
    publication_id, _, _ = _candidate(publication_database, "c" * 64, "100.00")
    _activate(store, publication_id)
    reader = reader_config(publication_database, monkeypatch)
    descriptor = resolve(reader, publication_id)
    contract = load_contract()
    namespace = descriptor["read_namespace"]
    with psycopg.connect(**config.connection_kwargs, autocommit=True) as db:
        db.execute(
            sql.SQL("ALTER VIEW {} RENAME COLUMN date_sk TO changed_key").format(
                sql.Identifier(namespace, "dim_date")
            )
        )
    with pytest.raises(ExportError) as failure:
        list(batches(reader, descriptor, contract.relations["dim_date"], contract, Budget()))
    assert failure.value.code == "EXPORT_UNSUPPORTED_SCHEMA"
    with psycopg.connect(**config.connection_kwargs, autocommit=True) as db:
        db.execute(
            sql.SQL("REVOKE SELECT ON {} FROM secop_bi_reader").format(
                sql.Identifier(namespace, "dim_entity")
            )
        )
    with pytest.raises(PublicationError) as denied:
        list(batches(reader, descriptor, contract.relations["dim_entity"], contract, Budget()))
    assert denied.value.code == "PUBLICATION_DENIED"
