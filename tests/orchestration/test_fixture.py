"""Trusted offline transport executes actual bounded acquisition without network I/O."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.errors import IngestionConfigurationError, PageValidationError
from observatorio_secop.ingestion.population_plan import member_query
from observatorio_secop.ingestion.reconcile import PopulationReconciliation
from observatorio_secop.orchestration.fixtures import (
    fixture_expected,
    fixture_rows,
    fixture_transport,
)

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def source(tmp_path):
    config = replace(
        load_ingestion_config(ROOT / "config/bronze_ingestion.yaml"),
        bronze_root=tmp_path / "bronze",
        contract_path=ROOT / "contracts/secop_source.yaml",
        page_size=1,
    )
    return config, load_bronze_contract(config.contract_path, config.projection)


@pytest.mark.parametrize("fixture_id, count, total", [("demo-a", 2, "300"), ("demo-b", 3, "400")])
def test_fixture_executes_real_reconciliation_without_network(source, fixture_id, count, total):
    config, contract = source
    transport = fixture_transport(config, contract, fixture_id)
    runner = PopulationReconciliation(
        config, contract, transport=transport, uuid_factory=lambda: "fixture-campaign"
    )
    manifest = runner.reconcile(
        "2026-01-01",
        "2026-02-01",
        total_rows=20,
        total_requests=20,
        grant_rows=20,
        grant_requests=20,
    )
    assert manifest.range_complete and not manifest.truncated
    assert manifest.counts["written"] == count
    assert transport.requests == count + 2  # metadata + one page per row + terminal page
    assert manifest.checkpoint_candidate is None
    expected = fixture_expected(fixture_id)
    assert expected.accepted_total_cop == Decimal(total)
    assert expected.accepted_rows == count and expected.rejected_rows == 0
    assert len(expected.sha256) == 64
    before = transport.requests
    again = runner.reconcile(
        "2026-01-01",
        "2026-02-01",
        total_rows=20,
        total_requests=20,
        grant_rows=20,
        grant_requests=20,
        resume=manifest.run_id,
    )
    assert again.as_dict() == manifest.as_dict() and transport.requests == before


def test_fixed_members_are_filtered_and_missing_ids_stay_missing(source):
    config, contract = source
    transport = fixture_transport(config, contract, "demo-b")
    page = transport.query(member_query(["A", "MISSING"], list(config.projection)))
    assert [row["id_contrato"] for row in page.payload] == ["A"]


def test_rejected_fixture_preserves_source_number_for_silver_rule(source):
    config, contract = source
    transport = fixture_transport(config, contract, "demo-rejected")
    page = transport.query(member_query(["A"], list(config.projection)))
    assert page.payload[0]["valor_del_contrato"] == "-1.00"
    expected = fixture_expected("demo-rejected")
    assert expected.input_rows == 2 and expected.accepted_rows == 1
    assert expected.rejected_rows == 1 and expected.accepted_total_cop == Decimal("200")


def test_fixture_id_is_not_a_path_and_does_not_mutate_rows(source):
    config, contract = source
    for value in ("../demo-a", "/tmp/demo-a", "https://example.test", "unknown"):
        with pytest.raises(IngestionConfigurationError, match="Unsupported"):
            fixture_transport(config, contract, value)
    first = fixture_rows("demo-a")
    first[0]["valor_del_contrato"] = "999"
    assert fixture_rows("demo-a")[0]["valor_del_contrato"] == "100.00"


def test_fixture_never_falls_through_to_network_or_accepts_arbitrary_query(source):
    config, contract = source
    transport = fixture_transport(config, contract, "demo-a")
    with pytest.raises(PageValidationError, match="external"):
        transport._request("https://untrusted.invalid/data")
    with pytest.raises(PageValidationError, match="governed"):
        transport.query({"$limit": 1, "$where": "1=1"})


def test_fixture_full_page_without_terminal_budget_remains_partial(source):
    config, contract = source
    transport = fixture_transport(config, contract, "demo-a")
    runner = PopulationReconciliation(
        config, contract, transport=transport, uuid_factory=lambda: "partial-fixture"
    )
    manifest = runner.reconcile(
        "2026-01-01",
        "2026-02-01",
        total_rows=2,
        total_requests=10,
        grant_rows=2,
        grant_requests=10,
    )
    assert manifest.truncated and not manifest.range_complete
    assert manifest.counts["written"] == 2
    assert manifest.population["acquisition"]["state"] == "truncated"
    assert manifest.population["terminal_reason"] == "campaign_rows_exhausted"


def test_fixture_honors_exclusive_signing_scope(source):
    config, contract = source
    transport = fixture_transport(config, contract, "demo-a")
    runner = PopulationReconciliation(
        config, contract, transport=transport, uuid_factory=lambda: "empty-scope-fixture"
    )
    manifest = runner.reconcile(
        "2025-12-01",
        "2026-01-01",
        total_rows=10,
        total_requests=10,
        grant_rows=10,
        grant_requests=10,
    )
    assert manifest.range_complete and manifest.counts["written"] == 0
    assert transport.requests == 2
