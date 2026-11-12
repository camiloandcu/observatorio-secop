"""Exercise the model's actual grouped evidence SQL against bounded VALUES only."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest

ROOT = Path(__file__).resolve().parents[2]


def fixture_query():
    model = (ROOT / "dbt/models/marts/mart_contract_detail.sql").read_text()
    grouped, _ = model.split("), source as (", 1)
    grouped = grouped.removeprefix("with ")
    grouped = grouped.replace("{{ ref('stg_contract_observation_link') }}", "fixture_links") + ")"
    join = model.rsplit("from source\n", 1)[1].strip()
    fixtures = """source(fixture, silver_version_id, silver_manifest_sha256,
                contract_key, scope_id) AS (
 VALUES ('empty','v1','h1','none','scope-a'),
        ('one','v1','h1','one','scope-a'),
        ('multi','v1','h1','shared','scope-a'),
        ('version','v2','h1','shared','scope-a'),
        ('hash','v1','h2','shared','scope-a'),
        ('legacy','v1','h1','legacy',NULL),
        ('other-scope','v1','h1','one','scope-b')
), fixture_links(silver_version_id,silver_manifest_sha256,contract_key,link_id,evidence) AS (
 VALUES ('v1','h1','one','single',
         '{"marker":"only","winner_page_path":"PRIVATE_WIN","rejected_page_path":"PRIVATE_REJECT"}'::jsonb),
        ('v1','h1','shared','z',
         '{"marker":"last","winner_page_path":"PRIVATE_WIN","rejected_page_path":"PRIVATE_REJECT"}'::jsonb),
        ('v1','h1','shared','a',
         '{"marker":"first","winner_page_path":"PRIVATE_WIN","rejected_page_path":"PRIVATE_REJECT"}'::jsonb),
        ('v2','h1','shared','a','{"marker":"different-version"}'::jsonb),
        ('v1','h2','shared','a','{"marker":"different-hash"}'::jsonb),
        ('v1','h9','shared','a','{"marker":"unrelated"}'::jsonb)
)"""
    old = """old_result AS (
 SELECT source.fixture,source.scope_id,
        (SELECT coalesce(jsonb_agg(l.evidence - 'winner_page_path' - 'rejected_page_path'
                                  ORDER BY l.link_id),'[]'::jsonb)
         FROM fixture_links l WHERE l.silver_version_id=source.silver_version_id
           AND l.silver_manifest_sha256=source.silver_manifest_sha256
           AND l.contract_key=source.contract_key) AS evidence
 FROM source
)"""
    actual = (
        """actual AS (
 SELECT source.fixture,source.scope_id,
        coalesce(link_evidence.attributable_rejection_evidence,'[]'::jsonb) AS evidence
 FROM source
 """
        + join
        + ")"
    )
    return "WITH " + ",\n".join([fixtures, grouped, old, actual])


def test_model_exposes_one_materialized_aggregation():
    query = fixture_query()
    assert query.count("link_evidence as materialized") == 1
    assert "GROUP BY" in query.upper()
    assert (
        "left join link_evidence using (silver_version_id, silver_manifest_sha256, contract_key)"
        in query
    )


@pytest.mark.skipif(
    not os.environ.get("WAREHOUSE_TEST_DSN"), reason="Requires dedicated disposable PostgreSQL"
)
def test_grouped_evidence_is_bag_equivalent_ordered_and_sanitized():
    with psycopg.connect(os.environ["WAREHOUSE_TEST_DSN"]) as database:
        database.execute("SET TRANSACTION READ ONLY")
        database.execute("SET LOCAL statement_timeout='5s'")
        database.execute("SET LOCAL lock_timeout='1s'")
        prefix = fixture_query()
        difference = database.execute(
            prefix
            + """
 SELECT 'old-minus-new' AS violation,* FROM (
   SELECT * FROM old_result EXCEPT ALL SELECT * FROM actual) a
 UNION ALL
 SELECT 'new-minus-old' AS violation,* FROM (
   SELECT * FROM actual EXCEPT ALL SELECT * FROM old_result) b
 """
        ).fetchall()
        assert difference == []
        rows = database.execute(
            prefix + " SELECT fixture,scope_id,evidence FROM actual ORDER BY fixture"
        ).fetchall()
        assert len(rows) == 7  # One output per source row, including a different scope.
        evidence = {fixture: value for fixture, _, value in rows}
        assert evidence["empty"] == evidence["legacy"] == []
        assert evidence["one"] == evidence["other-scope"] == [{"marker": "only"}]
        assert evidence["multi"] == [{"marker": "first"}, {"marker": "last"}]
        assert evidence["version"] == [{"marker": "different-version"}]
        assert evidence["hash"] == [{"marker": "different-hash"}]
        assert next(scope for fixture, scope, _ in rows if fixture == "legacy") is None
        assert "PRIVATE_" not in json.dumps(rows)
        # Unreferenced old_result is pruned; the optimized path has no correlated subplan.
        plan = database.execute(
            "EXPLAIN (FORMAT JSON) " + prefix + " SELECT * FROM actual"
        ).fetchone()[0]
        assert "SubPlan" not in json.dumps(plan)
