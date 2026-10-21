from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from observatorio_secop.population import (
    links_arrow_schema,
    summary_arrow_schema,
    validate_condition_set,
)
from observatorio_secop.processing.silver.conditions import observation_conditions
from observatorio_secop.processing.silver.models import BronzePage, VerifiedBronzeRun


def observed(run: str, instant: str, *, raw_id: str = "A", rejected: bool = False):
    return {
        "contract_key": "a" * 64,
        "source_contract_id": raw_id,
        "source_dataset_id": "jbjy-vk9h",
        "bronze_run_id": run,
        "bronze_page_path": "pages/data.json",
        "bronze_row_ordinal": 0,
        "bronze_fetched_at_utc": instant,
        "payload_sha256": ("b" if rejected else "c") * 64,
        "source_contract_sha256": "d" * 64,
        "source_projection_version": "secop-contracts-v2",
        "_rejection_details": [{"code": "AMOUNT_INVALID_FORMAT"}] if rejected else [],
    }


def run_evidence(names):
    return tuple(
        VerifiedBronzeRun(
            run_id=name,
            dataset_id="jbjy-vk9h",
            contract_sha256="d" * 64,
            manifest_path=Path(name),
            manifest_sha256="e" * 64,
            pages=(
                BronzePage(
                    name,
                    Path(name),
                    "pages/data.json",
                    "scope_scan",
                    1,
                    "2026-01-01T00:00:00.000Z",
                    "f" * 64,
                    1,
                ),
            ),
        )
        for name in names
    )


def evaluate(observations, winner):
    return observation_conditions(
        observations,
        [winner],
        run_evidence({row["bronze_run_id"] for row in observations}),
        scope_id="1" * 64,
        version_id="candidate",
        validation_version="silver-v1",
    )


def test_all_reject_links_preserve_strict_acquisition_chronology(tmp_path):
    winner = observed("winner", "2026-01-02T00:00:00.000001Z")
    records = [
        winner,
        observed("before", "2026-01-01T00:00:00Z", rejected=True),
        observed("same", "2026-01-02T00:00:00.000001Z", rejected=True),
        observed("after-1", "2026-01-03T00:00:00Z", rejected=True),
        observed("after-2", "2026-01-04T00:00:00Z", rejected=True),
    ]
    summaries, links = evaluate(records, winner)
    assert summaries[0]["state"] == "LATER_INVALID_OBSERVED"
    assert summaries[0]["later_invalid_count"] == 2
    assert summaries[0]["equal_invalid_count"] == 1
    assert len(links) == 4
    for filename, rows, schema in (
        ("summary", summaries, summary_arrow_schema()),
        ("links", links, links_arrow_schema()),
    ):
        path = tmp_path / f"{filename}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        assert pq.read_table(path).schema == schema
    validate_condition_set(
        summaries, links, scope_id="1" * 64, silver_version_id="candidate", contract_keys={"a" * 64}
    )
    assert summaries[0]["winner_observed_at_utc"] == datetime(2026, 1, 2, microsecond=1, tzinfo=UTC)


def test_equal_time_is_indeterminate_and_a_new_winner_recomputes_condition():
    first = observed("first", "2026-01-02T00:00:00Z")
    reject = observed("bad", "2026-01-02T00:00:00Z", rejected=True)
    summaries, _ = evaluate([first, reject], first)
    assert summaries[0]["state"] == "TEMPORAL_ORDER_INDETERMINATE"
    later_valid = observed("later-valid", "2026-01-03T00:00:00Z")
    summaries, links = evaluate([first, reject, later_valid], later_valid)
    assert summaries[0]["state"] == "NO_LATER_INVALID_OBSERVED"
    assert links[0]["temporal_relation"] == "EARLIER"
    # If source precedence retains first, the arrival of a losing valid row
    # does not change the relative chronology of first and the rejection.
    summaries, _ = evaluate([first, reject, later_valid], first)
    assert summaries[0]["state"] == "TEMPORAL_ORDER_INDETERMINATE"


def test_normalization_collisions_do_not_attribute_a_reject():
    winner = observed("winner", "2026-01-01T00:00:00Z")
    reject = observed("bad", "2026-01-02T00:00:00Z", raw_id=" A ", rejected=True)
    summaries, links = evaluate([winner, reject], winner)
    assert links == []
    assert summaries[0]["state"] == "NO_LATER_INVALID_OBSERVED"
