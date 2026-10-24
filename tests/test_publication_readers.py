"""Boundary tests for untrusted continuation descriptors and bounded reads."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime, timedelta

import pytest

from observatorio_secop.publication.build import ROOT, reviewed_metadata
from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.publication.queries import _compile_query, query_metric
from observatorio_secop.publication.readers import (
    _instant,
    _validate,
    read_publication,
    scoring_compatibility,
)

PUBLICATION = "1" * 32
OTHER = "2" * 32
NOW = datetime(2026, 10, 23, 12, tzinfo=UTC)


class CatalogCursor:
    def __init__(self, *, now=NOW):
        self.calls = []
        self.row = {
            "publication_id": PUBLICATION,
            "read_namespace": "publication_" + PUBLICATION,
            "state": "retained",
            "server_now": now,
            "built_at": NOW - timedelta(hours=2),
            "metadata": {"scope_id": "scope-test", "scoring_state": "ABSENT"},
            "is_active": True,
            "is_previous": False,
            "retain_until": None,
        }

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))

    def fetchone(self):
        return self.row


def descriptor():
    return {
        "version": 1,
        "publication_id": PUBLICATION,
        "read_namespace": "publication_" + PUBLICATION,
        "built_at": (NOW - timedelta(hours=2)).isoformat(),
        "metadata": {"scope_id": "scope-test", "scoring_state": "ABSENT"},
        "resolved_at": (NOW - timedelta(minutes=1)).isoformat(),
        "operation_expires_at": (NOW + timedelta(minutes=59)).isoformat(),
    }


@pytest.mark.parametrize(
    "value", [None, 123, "", "2026-10-23", "2026-10-23T12:00:00", "2026-10-23T12:00:00-05:00"]
)
def test_descriptor_times_require_explicit_valid_utc(value):
    with pytest.raises(PublicationError) as failure:
        _instant(value)
    assert failure.value.code == "PUBLICATION_INVALID"


def test_z_and_explicit_utc_instants_have_the_same_meaning():
    assert _instant("2026-10-23T12:00:00Z") == _instant("2026-10-23T12:00:00+00:00") == NOW


@pytest.mark.parametrize(
    "changes",
    [
        {"version": True},
        {"version": "1"},
        {"version": 2},
        {"publication_id": "candidate_" + PUBLICATION},
        {"publication_id": "../silver"},
        {"read_namespace": "silver"},
        {"read_namespace": "publication_" + OTHER},
        {"resolved_at": "not-an-instant"},
        {"operation_expires_at": (NOW + timedelta(hours=2)).isoformat()},
    ],
)
def test_malformed_descriptor_fails_before_catalog_or_relation_queries(changes):
    cursor = CatalogCursor()
    value = descriptor() | changes
    with pytest.raises(PublicationError) as failure:
        _validate(cursor, value)
    assert failure.value.code == "PUBLICATION_INVALID"
    assert cursor.calls == []


def test_deadline_boundary_is_expired_and_fresh_future_descriptor_is_invalid():
    value = descriptor()
    deadline = _instant(value["operation_expires_at"])
    with pytest.raises(PublicationError) as expired:
        _validate(CatalogCursor(now=deadline), value)
    assert expired.value.code == "PUBLICATION_EXPIRED"
    value.update(
        resolved_at=(NOW + timedelta(minutes=1)).isoformat(),
        operation_expires_at=(NOW + timedelta(hours=1, minutes=1)).isoformat(),
    )
    with pytest.raises(PublicationError) as future:
        _validate(CatalogCursor(), value)
    assert future.value.code == "PUBLICATION_INVALID"


@pytest.mark.parametrize("changes", [{"metadata": {}}, {"built_at": "2026-10-23T00:00:00Z"}])
def test_descriptor_binds_the_original_immutable_publication_metadata(changes):
    with pytest.raises(PublicationError) as failure:
        _validate(CatalogCursor(), descriptor() | changes)
    assert failure.value.code == "PUBLICATION_CONFLICT"


@pytest.mark.parametrize(
    "relation,limit,offset",
    [
        ("silver.contract_current", 10, 0),
        ("fact_contract;DROP SCHEMA published", 10, 0),
        ("fact_contract", 0, 0),
        ("fact_contract", 101, 0),
        ("fact_contract", True, 0),
        ("fact_contract", 10, -1),
        ("fact_contract", 10, False),
    ],
)
def test_unbounded_or_untrusted_read_rejected_before_any_database_connection(
    monkeypatch, relation, limit, offset
):
    monkeypatch.setattr(
        "observatorio_secop.publication.readers.psycopg.connect",
        lambda **_: pytest.fail("Invalid reader input reached PostgreSQL"),
    )
    with pytest.raises(PublicationError) as failure:
        read_publication(None, descriptor(), relation, limit=limit, offset=offset)
    assert failure.value.code == "PUBLICATION_INVALID"


def test_optional_scoring_is_explicit_and_has_no_database_side_effect(monkeypatch):
    monkeypatch.setattr(
        "observatorio_secop.publication.readers.psycopg.connect",
        lambda **_: pytest.fail("Optional compatibility check reached PostgreSQL"),
    )
    assert scoring_compatibility(PUBLICATION)["state"] == "ABSENT"
    assert scoring_compatibility(PUBLICATION, failed=True)["state"] == "FAILED"
    assert scoring_compatibility(PUBLICATION, OTHER)["state"] == "MISMATCHED"
    assert scoring_compatibility(PUBLICATION, PUBLICATION)["state"] == "COMPATIBLE"


@pytest.mark.parametrize(
    "filters",
    [
        [],
        "",
        False,
        0,
        {"unapproved": "arbitrary"},
        {"statuses": "Activo"},
        {"entity_keys": [None]},
        {"start_date": "yesterday"},
        {"missing_signatures": 1},
        {"selected_supplier": ["arbitrary"]},
    ],
)
def test_invalid_metric_filter_is_not_silently_replaced_or_sent_to_database(monkeypatch, filters):
    monkeypatch.setattr(
        "observatorio_secop.publication.queries.validate_descriptor",
        lambda *_: pytest.fail("Invalid metric filters reached publication lookup"),
    )
    with pytest.raises(PublicationError) as failure:
        query_metric(None, descriptor(), "overview", filters)
    assert failure.value.code == "PUBLICATION_INVALID_FILTER"


def test_changed_local_query_graph_cannot_reinterpret_a_retained_publication(tmp_path):
    for directory in ("dbt", "contracts", "config", "scripts"):
        shutil.copytree(
            ROOT / directory,
            tmp_path / directory,
            ignore=shutil.ignore_patterns("target", "logs", "__pycache__"),
        )
    metadata = reviewed_metadata(tmp_path)
    assert "select" in _compile_query("overview", "publication_" + PUBLICATION, metadata, tmp_path)
    macro = tmp_path / "dbt/macros/gold_queries.sql"
    before = macro.read_text()
    assert "sum(contract_value_cop)" in before
    macro.write_text(before.replace("sum(contract_value_cop)", "sum(contract_value_cop)+1"))
    with pytest.raises(PublicationError) as failure:
        _compile_query("overview", "publication_" + PUBLICATION, metadata, tmp_path)
    assert failure.value.code == "PUBLICATION_CONTRACT_UNAVAILABLE"
