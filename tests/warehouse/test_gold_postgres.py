"""Independent acceptance scenarios over the real Silver → PostgreSQL → dbt path."""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader

from .conftest import population_row, write_identity_bundle
from .snapshot_helpers import seed_registered
from .test_dbt_postgres import (
    ROOT,
    TEST_DSN,
    _dbt,
    _dbt_environment,
    _drop_schemas,
    _warehouse_config,
)

pytestmark = pytest.mark.skipif(not TEST_DSN, reason="WAREHOUSE_TEST_DSN is not configured")


def _row(
    label: str,
    amount: str,
    supplier: str | None,
    month: int | None,
    *,
    entity: str | None = "00100",
    status: str | None = "Borrador",
) -> dict[str, Any]:
    row = population_row(
        "gold-fixture",
        label,
        contract_id=f"CO1.PCCNTR.{label}",
        amount=amount,
        supplier_code={"A": "00200", "B": "00300"}.get(supplier),
        entity_code=entity,
        supplier_name=None if supplier == "A" else "Proveedor sintético",
    )
    signing = date(2026, month, 15) if month else None
    row.update(
        signing_date=signing,
        signing_date_raw=signing.isoformat() if signing else None,
        signing_year="2026" if month else "__UNKNOWN__",
        contract_status=status,
        source_url="https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index?noticeUID=1",
    )
    return row


@pytest.fixture(scope="module")
def gold_database(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("gold-postgres")
    assert TEST_DSN
    connection = conninfo_to_dict(TEST_DSN)
    environment = _dbt_environment(tmp_path, connection)
    config = _warehouse_config(tmp_path / "silver", connection)
    _drop_schemas()
    try:
        # Unequal monthly populations distinguish pooled HHI from mean monthly HHI.
        rows = [
            _row("a-january", "10.01", "A", 1),
            _row("a-february", "20.02", "A", 2, status="En aprobación"),
            _row("b-february", "90.03", "B", 2, status=" nuevo estado "),
            _row("partial-a", "40.00", "A", 1, entity="00200"),
            _row("partial-b", "40.00", "B", 1, entity="00200"),
            _row("partial-unknown", "20.00", None, 1, entity="00200"),
            _row("zero", "0.00", "A", 1, entity="00300"),
            _row("no-date", "5.00", "A", None, entity="00400", status=None),
            _row("unknown-entity", "7.00", "A", 1, entity=None),
            _row("none-known", "15.00", None, 1, entity="00500"),
            _row("zero-known", "0.00", "A", 1, entity="00600"),
            _row("positive-unknown", "10.00", None, 1, entity="00600"),
            _row("full-a", "40.00", "A", 1, entity="00700"),
            _row("full-b", "40.00", "B", 1, entity="00700"),
        ]
        rows[8]["source_url"] = "javascript:alert(1)"
        rows[3]["source_url"] = "https://community.secop.gov.co.evil.invalid/process"
        rows[4]["source_url"] = "https://community.secop.gov.co@evil.invalid/process"
        write_identity_bundle(config.silver_root, "gold-fixture", rows)
        bundle = verify_current_bundle(config.silver_root)
        loader = WarehouseLoader(config)
        # Reproduce a supported pre-population snapshot; new legacy activation
        # is forbidden. The real loader verifies/migrates this active baseline.
        seed_registered(config, bundle)
        assert loader.load(bundle).status == "noop"
        _dbt(environment, "build")
        assert loader.load(bundle).status == "noop"
        with psycopg.connect(TEST_DSN, row_factory=dict_row) as database:
            yield database, environment, tmp_path
    finally:
        _drop_schemas()


def _project_dbt(environment, project: Path, *arguments: str, check=True):
    result = subprocess.run(
        [
            shutil.which("dbt"),
            *arguments,
            "--no-partial-parse",
            "--project-dir",
            str(project),
            "--profiles-dir",
            environment["DBT_TEST_PROFILES_DIR"],
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode:
        pytest.fail(f"dbt {' '.join(arguments)} failed:\n{result.stdout}\n{result.stderr}")
    return result


def _query(database, environment, tmp_path: Path, macro: str, filters=None, *, variables=None):
    """Compile the supported macro and execute its actual SQL against built marts."""
    project = tmp_path / "query-project"
    if not project.exists():
        shutil.copytree(ROOT / "dbt", project, ignore=shutil.ignore_patterns("target", "logs"))
    analyses = project / "analyses"
    analyses.mkdir(exist_ok=True)
    (analyses / "acceptance_query.sql").write_text(
        "{{ "
        + macro
        + "(ref('mart_contract_detail'), filters="
        + repr(filters or {})
        + ", bound=true) }}\n",
        encoding="utf-8",
    )
    arguments = ["compile", "--select", "acceptance_query"]
    if variables is not None:
        import json

        arguments.extend(["--vars", json.dumps(variables)])
    cache_key = hashlib.sha256(repr((macro, variables)).encode()).hexdigest()
    cache_path = tmp_path / (cache_key + ".sql")
    if not cache_path.exists():
        _project_dbt(environment, project, *arguments)
        cache_path.write_text(
            (
                project / "target/compiled/observatorio_secop/analyses/acceptance_query.sql"
            ).read_text()
        )
    sql = cache_path.read_text()
    parameters = dict.fromkeys(
        (
            "entity_keys",
            "municipality_keys",
            "modality_keys",
            "statuses",
            "exclude_statuses",
            "start_date",
            "end_date",
            "missing_signatures",
            "selected_supplier",
        )
    )
    parameters.update(filters or {})
    return database.execute(sql, parameters).fetchall()


def _keys(database):
    entities = {
        row["entity_code"]: row["entity_sk"]
        for row in database.execute("select entity_code,entity_sk from analytics.dim_entity")
    }
    suppliers = {
        row["supplier_code"]: row["supplier_sk"]
        for row in database.execute("select supplier_code,supplier_sk from analytics.dim_supplier")
    }
    return entities, suppliers


def test_exact_numeric_distribution_and_multi_month_recomputation(gold_database):
    database, environment, tmp_path = gold_database
    entities, _ = _keys(database)
    filters = {"entity_keys": [entities["00100"]]}
    result = _query(database, environment, tmp_path, "gold_overview_query", filters)[0]
    assert result["contract_count"] == 3
    assert result["contract_value_cop"] == Decimal("120.06")
    assert result["value_p25_cop"] == Decimal("15.015")
    assert result["value_median_cop"] == Decimal("20.02")
    assert result["value_p75_cop"] == Decimal("55.025")
    assert result["contract_count_share"] == 1
    assert result["contract_value_share"] == 1
    assert result["count_share_state"] == result["value_share_state"] == "AVAILABLE"
    monthly = _query(database, environment, tmp_path, "gold_entity_monthly_query", filters)
    assert sorted((row["contract_count"], row["contract_value_cop"]) for row in monthly) == [
        (1, Decimal("10.01")),
        (2, Decimal("110.05")),
    ]
    concentration = _query(database, environment, tmp_path, "gold_concentration_query", filters)[0]
    expected = (Decimal("30.03") / Decimal("120.06")) ** 2 + (
        Decimal("90.03") / Decimal("120.06")
    ) ** 2
    assert abs(concentration["hhi"] - expected) < Decimal("1e-15")
    assert concentration["count_identity_coverage"] == 1
    assert concentration["value_identity_coverage"] == 1
    # SQL reference groups raw detail by supplier; no stored HHI participates.
    reference = database.execute(
        "select sum((value / 120.06::numeric)^2) as hhi from ("
        "select supplier_sk,sum(contract_value_cop) as value "
        "from analytics.mart_contract_detail where entity_sk=%s group by supplier_sk) s",
        (entities["00100"],),
    ).fetchone()
    assert concentration["hhi"] == reference["hhi"]


def test_partial_identity_selection_preserves_reference_and_synthetic_formula(gold_database):
    database, environment, tmp_path = gold_database
    entities, suppliers = _keys(database)
    filters = {"entity_keys": [entities["00200"]]}
    baseline = _query(database, environment, tmp_path, "gold_concentration_query", filters)[0]
    assert baseline["hhi"] is None
    assert baseline["hhi_state"] == "INSUFFICIENT_IDENTITY_COVERAGE"
    assert baseline["value_identity_coverage"] == Decimal("0.8")
    assert abs(baseline["count_identity_coverage"] - Decimal(2) / 3) < Decimal("1e-15")
    filters["selected_supplier"] = suppliers["00200"]
    selected = _query(database, environment, tmp_path, "gold_concentration_query", filters)[0]
    for column in (
        "hhi",
        "hhi_state",
        "total_contract_count",
        "known_contract_count",
        "total_value_cop",
        "known_value_cop",
        "count_identity_coverage",
        "value_identity_coverage",
    ):
        assert selected[column] == baseline[column]
    assert selected["selected_contract_count"] == 1
    assert selected["selected_supplier_reference_share"] == Decimal("0.5")
    assert selected["selected_supplier_reference_share_state"] == "AVAILABLE"
    synthetic = _query(
        database,
        environment,
        tmp_path,
        "gold_concentration_query",
        filters,
        variables={"gold_min_count_coverage": 0.5, "gold_min_value_coverage": 0.8},
    )[0]
    assert synthetic["hhi"] == Decimal("0.5")
    assert synthetic["hhi_state"] == "AVAILABLE"  # equality at value threshold qualifies
    overview = _query(database, environment, tmp_path, "gold_overview_query", filters)[0]
    assert (overview["contract_count"], overview["contract_value_cop"]) == (1, Decimal("40.00"))


def test_empty_zero_unknown_and_absent_signature_states(gold_database):
    database, environment, tmp_path = gold_database
    entities, _ = _keys(database)
    empty_filters = {"entity_keys": [entities["00100"]], "start_date": "2027-01-01"}
    empty = _query(database, environment, tmp_path, "gold_overview_query", empty_filters)[0]
    assert (empty["contract_count"], empty["contract_value_cop"]) == (0, Decimal("0.00"))
    assert empty["value_median_cop"] is None
    assert empty["distribution_state"] == "NO_CONTRACTS"
    assert empty["count_share_state"] == empty["value_share_state"] == "ZERO_DENOMINATOR"
    assert empty["silver_version_id"] == "gold-fixture"
    zero_filters = {"entity_keys": [entities["00300"]]}
    zero = _query(database, environment, tmp_path, "gold_overview_query", zero_filters)[0]
    assert zero["contract_count"] == 1 and zero["value_median_cop"] == 0
    assert zero["contract_value_share"] is None
    assert zero["value_share_state"] == "ZERO_DENOMINATOR"
    zero_hhi = _query(database, environment, tmp_path, "gold_concentration_query", zero_filters)[0]
    assert zero_hhi["hhi"] is None and zero_hhi["hhi_state"] == "ZERO_TOTAL_VALUE"
    assert zero_hhi["count_identity_coverage"] == 1
    assert zero_hhi["value_identity_coverage"] is None
    unknown = _query(
        database,
        environment,
        tmp_path,
        "gold_concentration_query",
        {"entity_keys": [entities[None]]},
    )[0]
    assert unknown["hhi"] is None and unknown["hhi_state"] == "UNKNOWN_ENTITY"
    missing = _query(
        database, environment, tmp_path, "gold_overview_query", {"missing_signatures": True}
    )[0]
    assert (missing["contract_count"], missing["contract_value_cop"]) == (1, Decimal("5.00"))
    statuses = _query(
        database, environment, tmp_path, "gold_overview_query", {"statuses": ["__MISSING__"]}
    )[0]
    assert statuses["contract_count"] == 1
    excluded = _query(
        database,
        environment,
        tmp_path,
        "gold_overview_query",
        {"entity_keys": [entities["00100"]], "exclude_statuses": ["borrador"]},
    )[0]
    assert (excluded["contract_count"], excluded["contract_value_cop"]) == (2, Decimal("110.05"))
    interval = _query(
        database,
        environment,
        tmp_path,
        "gold_overview_query",
        {"entity_keys": [entities["00100"]], "start_date": "2026-01-15", "end_date": "2026-02-15"},
    )[0]
    assert interval["contract_count"] == 1  # start inclusive, end exclusive
    hostile = _query(
        database,
        environment,
        tmp_path,
        "gold_overview_query",
        {"statuses": ["borrador' OR true --"]},
    )[0]
    assert hostile["contract_count"] == 0


def test_invalid_bound_selections_fail_explicitly(gold_database):
    database, environment, tmp_path = gold_database
    for filters, reason in (
        ({"missing_signatures": True, "start_date": "2026-01-01"}, "MISSING_SIGNATURE"),
        ({"start_date": "2026-03-01", "end_date": "2026-01-01"}, "INTERVAL"),
        ({"entity_keys": ["not-a-key"]}, "ENTITY_KEY"),
    ):
        with pytest.raises(psycopg.Error, match=reason), database.transaction():
            _query(database, environment, tmp_path, "gold_overview_query", filters)


def test_detail_safe_lineage_metadata_and_relationships(gold_database):
    database, _, _ = gold_database
    counts = database.execute(
        "select count(*) as n,count(distinct contract_key) as unique_n,"
        "sum(contract_value_cop) as value from analytics.mart_contract_detail"
    ).fetchone()
    assert counts == {"n": 14, "unique_n": 14, "value": Decimal("337.06")}
    unsafe = database.execute(
        "select source_process_url,source_link_state from analytics.mart_contract_detail "
        "where entity_identity_status='UNKNOWN'"
    ).fetchone()
    assert unsafe == {"source_process_url": None, "source_link_state": "DISALLOWED"}
    assert (
        database.execute(
            "select count(*) as n from analytics.mart_contract_detail "
            "where contract_id in ('CO1.PCCNTR.partial-a','CO1.PCCNTR.partial-b') "
            "and source_process_url is null and source_link_state='DISALLOWED'"
        ).fetchone()["n"]
        == 2
    )
    columns = {
        row["column_name"]
        for row in database.execute(
            "select column_name from information_schema.columns "
            "where table_schema='analytics' and table_name='mart_contract_detail'"
        )
    }
    assert not {"bronze_page_path", "raw_payload", "rejected_payload"} & columns
    assert {
        "contract_id",
        "process_id",
        "contract_key",
        "entity_sk",
        "supplier_sk",
        "municipality_sk",
        "modality_sk",
        "signing_date",
        "status_raw",
        "status_bucket",
        "observation_condition",
        "condition_state",
        "bronze_run_id",
        "bronze_row_ordinal",
        "acquisition_observed_at_utc",
        "pipeline_completed_at_utc",
        "source_updated_at",
        "temporal_basis",
        "value_unit",
        "source_time_basis",
    } <= columns
    rows = database.execute(
        "select observation_condition,status_raw,status_profile_state,temporal_basis "
        "from analytics.mart_contract_detail"
    ).fetchall()
    assert all("page_path" not in str(row["observation_condition"]) for row in rows)
    assert all(row["temporal_basis"] == "LATEST_OBSERVED_BY_SIGNING_DATE" for row in rows)
    assert any(
        row["status_raw"] == " nuevo estado " and row["status_profile_state"] == "UNPROFILED"
        for row in rows
    )
    tuples = []
    for mart in (
        "mart_contract_detail",
        "mart_contract_overview",
        "mart_entity_monthly",
        "mart_supplier_concentration",
        "mart_data_quality",
    ):
        result = database.execute(
            "select distinct silver_version_id,silver_manifest_sha256,scope_id,"
            "catalog_sha256,policy_sha256 "
            f"from analytics.{mart}"
        ).fetchall()
        assert len(result) == 1, mart
        tuples.append(result[0])
    assert all(value == tuples[0] for value in tuples)
    assert (
        tuples[0]["catalog_sha256"]
        == hashlib.sha256((ROOT / "contracts/gold_metrics.yaml").read_bytes()).hexdigest()
    )
    assert (
        tuples[0]["policy_sha256"]
        == hashlib.sha256((ROOT / "config/gold_policy.yaml").read_bytes()).hexdigest()
    )
    for dimension, key in (
        ("dim_entity", "entity_sk"),
        ("dim_supplier", "supplier_sk"),
        ("dim_municipality", "municipality_sk"),
        ("dim_modality", "modality_sk"),
    ):
        result = database.execute(
            f"select count(*) as n from analytics.mart_contract_detail d "
            f"join analytics.{dimension} m using ({key})"
        ).fetchone()
        assert result["n"] == 14


def test_hhi_full_identity_and_null_reasons(gold_database):
    database, environment, tmp_path = gold_database
    entities, _ = _keys(database)
    full_filters = {"entity_keys": [entities["00700"]]}
    full = _query(database, environment, tmp_path, "gold_concentration_query", full_filters)[0]
    assert full["hhi"] == Decimal("0.5") and full["hhi_state"] == "AVAILABLE"
    assert full["selected_supplier_reference_share"] is None
    assert full["selected_supplier_reference_share_state"] == "NO_SELECTED_KNOWN_SUPPLIER"
    assert full["count_identity_coverage"] == full["value_identity_coverage"] == 1
    for entity, reason in (("00500", "NO_KNOWN_SUPPLIERS"), ("00600", "ZERO_KNOWN_VALUE")):
        row = _query(
            database,
            environment,
            tmp_path,
            "gold_concentration_query",
            {"entity_keys": [entities[entity]]},
        )[0]
        assert row["hhi"] is None and row["hhi_state"] == reason
        assert row["selected_supplier_reference_share_state"] == reason
    missing_policy = _query(
        database,
        environment,
        tmp_path,
        "gold_concentration_query",
        full_filters,
        variables={"gold_min_count_coverage": None, "gold_min_value_coverage": None},
    )[0]
    assert missing_policy["hhi"] is None
    assert missing_policy["hhi_state"] == "POLICY_UNCONFIGURED"
    assert missing_policy["total_contract_count"] == 2
    empty = _query(
        database,
        environment,
        tmp_path,
        "gold_concentration_query",
        {**full_filters, "start_date": "2027-01-01"},
    )
    assert len(empty) == 1
    assert empty[0]["hhi"] is None and empty[0]["hhi_state"] == "NO_CONTRACTS"


def test_dimension_intersection_uses_same_population(gold_database):
    database, environment, tmp_path = gold_database
    entities, _ = _keys(database)
    key = database.execute(
        "select municipality_sk,modality_sk from analytics.mart_contract_detail "
        "where entity_sk=%s limit 1",
        (entities["00100"],),
    ).fetchone()
    selected = _query(
        database,
        environment,
        tmp_path,
        "gold_overview_query",
        {
            "entity_keys": [entities["00100"]],
            "municipality_keys": [key["municipality_sk"]],
            "modality_keys": [key["modality_sk"]],
            "statuses": ["en aprobación"],
            "start_date": "2026-02-01",
            "end_date": "2026-03-01",
        },
    )[0]
    assert (selected["contract_count"], selected["contract_value_cop"]) == (1, Decimal("20.02"))


def test_unsupported_supplier_population_filter_is_rejected(gold_database):
    _, environment, tmp_path = gold_database
    project = tmp_path / "query-project"
    if not project.exists():
        shutil.copytree(ROOT / "dbt", project, ignore=shutil.ignore_patterns("target", "logs"))
    analyses = project / "analyses"
    analyses.mkdir(exist_ok=True)
    query = analyses / "unsupported_query.sql"
    query.write_text(
        "{{ gold_concentration_query(ref('mart_contract_detail'), "
        "filters={'supplier_population': ['00200']}) }}\n"
    )
    try:
        result = _project_dbt(
            environment,
            project,
            "compile",
            "--select",
            "unsupported_query",
            check=False,
        )
        assert result.returncode != 0
        assert "UNSUPPORTED_GOLD_FILTER" in result.stdout + result.stderr
    finally:
        query.unlink()


def test_quality_filter_denominators_and_unavailable_global_evidence(gold_database):
    database, environment, tmp_path = gold_database
    entities, suppliers = _keys(database)
    global_before = database.execute(
        "select control_id,numerator,denominator,control_state "
        "from analytics.mart_data_quality "
        "where filter_applicability='SNAPSHOT_SCOPE_ONLY' order by control_id"
    ).fetchall()
    assert global_before
    # This legacy envelope has no acquisition summary; unavailable is never zero.
    assert all(row["control_state"] == "UNAVAILABLE_LEGACY" for row in global_before)
    assert all(row["numerator"] is None and row["denominator"] is None for row in global_before)
    controls = _query(
        database,
        environment,
        tmp_path,
        "gold_quality_query",
        {
            "entity_keys": [entities["00200"]],
            "selected_supplier": suppliers["00200"],
        },
    )
    assert controls
    assert all(row["denominator"] == 1 for row in controls)
    unknown_supplier = next(row for row in controls if row["control_id"] == "unknown_supplier")
    assert unknown_supplier["numerator"] == 0
    assert unknown_supplier["evaluation_grain"] == "UNIQUE_CONTRACT"
    assert (
        database.execute(
            "select control_id,numerator,denominator,control_state "
            "from analytics.mart_data_quality "
            "where filter_applicability='SNAPSHOT_SCOPE_ONLY' order by control_id"
        ).fetchall()
        == global_before
    )


def test_source_drift_during_build_fails_validation(gold_database):
    database, environment, tmp_path = gold_database
    original = database.execute(
        "select evidence_sha256 from silver.contract_current_quality"
    ).fetchone()["evidence_sha256"]
    database.commit()
    project = tmp_path / "drift-project"
    shutil.copytree(ROOT / "dbt", project, ignore=shutil.ignore_patterns("target", "logs"))
    (project / "models/gold_drift_probe.sql").write_text(
        "{{ config(materialized='view', pre_hook=\"update silver.contract_current_quality "
        "set evidence_sha256=repeat('f',64)\") }}\nselect 1 as probe\n"
    )
    try:
        result = _project_dbt(
            environment,
            project,
            "run",
            "--select",
            "gold_drift_probe",
            check=False,
        )
        assert result.returncode != 0
        assert "GOLD_INPUT_DRIFT" in result.stdout + result.stderr
    finally:
        with psycopg.connect(TEST_DSN, autocommit=True) as recovery:
            recovery.execute(
                "update silver.contract_current_quality set evidence_sha256=%s", (original,)
            )
