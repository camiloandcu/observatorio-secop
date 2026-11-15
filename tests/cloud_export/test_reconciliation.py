from observatorio_secop.cloud_export.contract import Column, load_contract
from observatorio_secop.cloud_export.reconciliation import cell_expression, reconciliation_sql


def test_candidate_sql_covers_every_relation_and_column_without_lossy_conversions():
    contract = load_contract()
    sql = reconciliation_sql(contract)
    assert sql.count("FROM row_hashes;") == 13
    assert sql.count("array_agg(row_sha256)") == 13
    assert "DISTINCT" not in sql and "double" not in sql.lower() and "float" not in sql.lower()
    for relation in contract.relations.values():
        assert f'FROM "${{database}}"."{relation.name}"' in sql
        for column in relation.columns:
            assert cell_expression(column) in sql
    assert "to_utf8(" in sql and "IS NULL, 'N;'" in sql
    assert "array_join(array_sort(array_agg(row_sha256)), ''), ''" in sql


def test_temporal_and_unbounded_numeric_codecs_remain_text_and_boolean_is_canonical():
    for codec in ("utc-instant-us-v1", "wall-clock-us-v1", "decimal-text-v1"):
        expression = cell_expression(Column("value", "text", codec))
        assert 'CAST("value" AS varchar)' in expression
        assert "timestamp" not in expression and "decimal" not in expression
    assert "THEN 'true' ELSE 'false' END" in cell_expression(Column("flag", "boolean", "bool"))
