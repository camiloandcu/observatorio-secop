"""Verify multiple attributable rejections remain nested without expanding Gold facts."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict

from observatorio_secop.population import condition_link_id
from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import rewrite_manifest
from .test_dbt_postgres import (
    TEST_DSN,
    _dbt,
    _dbt_environment,
    _drop_schemas,
    _warehouse_config,
)
from .test_population_bundle import fixture, persist

pytestmark = pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN is not configured")


def _verified_multiple_links(root: Path):
    value = fixture(root)
    summary = value["condition_summary"][0]
    links = []
    for ordinal in (1, 2):
        link = {
            key: item
            for key, item in summary.items()
            if key not in {"state", "later_invalid_count", "equal_invalid_count"}
        }
        link.update(
            {
                key.replace("winner_", "rejected_", 1): item
                for key, item in list(link.items())
                if key.startswith("winner_")
            }
        )
        link.update(
            temporal_relation="EQUAL",
            rejection_codes=["AMOUNT_NULL"],
            rejected_row_ordinal=ordinal,
            rejected_payload_sha256=hashlib.sha256(f"invalid-{ordinal}".encode()).hexdigest(),
        )
        link["link_id"] = condition_link_id(link)
        links.append(link)
    summary.update(state="TEMPORAL_ORDER_INDETERMINATE", equal_invalid_count=2)
    value["condition_links"] = links
    value["manifest"]["population"]["link_rows"] = 2
    value["manifest"]["counts"].update(input_rows=3, rejected_rows=2)
    value["reconciliation"]["counts"].update(input_rows=3, rejected_rows=2)
    value["coverage"]["acquisitions"][0]["consumed"]["rows"] = 3
    persist(value)
    rewrite_manifest(value["path"], lambda _: None)
    return verify_current_bundle(root)


def test_multiple_rejection_links_do_not_expand_detail_or_money(tmp_path: Path):
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    environment = _dbt_environment(tmp_path, connection)
    config = _warehouse_config(tmp_path / "silver", connection)
    bundle = _verified_multiple_links(config.silver_root)
    assert len(bundle.condition_links) == 2
    _drop_schemas()
    try:
        assert WarehouseLoader(config).load(bundle).status == "applied"
        _dbt(environment, "build")
        with psycopg.connect(TEST_DSN) as database:
            assert database.execute(
                "select count(*),count(distinct contract_key),sum(contract_value_cop) "
                "from analytics.mart_contract_detail"
            ).fetchone() == (1, 1, Decimal("100.00"))
            condition, evidence = database.execute(
                "select condition_state,attributable_rejection_evidence "
                "from analytics.mart_contract_detail"
            ).fetchone()
            assert condition == "TEMPORAL_ORDER_INDETERMINATE"
            assert len(evidence) == 2
            assert {item["rejected_row_ordinal"] for item in evidence} == {1, 2}
            assert len({item["link_id"] for item in evidence}) == 2
            assert all(item["temporal_relation"] == "EQUAL" for item in evidence)
            assert "page_path" not in json.dumps(evidence)
            assert database.execute(
                "select sum(contract_count),sum(contract_value_cop) "
                "from analytics.mart_contract_overview"
            ).fetchone() == (1, Decimal("100.00"))
    finally:
        _drop_schemas()
