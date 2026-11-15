"""Reviewable Athena engine-3 templates; execution requires its separate cloud gate."""

from __future__ import annotations

from .contract import Column, Contract


def cell_expression(column: Column) -> str:
    name = f'"{column.name}"'
    value = (
        f"CASE WHEN {name} THEN 'true' ELSE 'false' END"
        if column.codec == "bool"
        else f"CAST({name} AS varchar)"
    )
    return (
        f"IF({name} IS NULL, 'N;', concat('V', "
        f"CAST(length(to_utf8({value})) AS varchar), ':', {value}, ';'))"
    )


def reconciliation_sql(contract: Contract) -> str:
    statements = [
        "-- Candidate Athena engine 3 SQL: logical-digest-v1.",
        "-- Replace ${database} only in the reviewed cloud preview.",
        "-- Validate exact codec fixtures in Athena before relying on these results.",
        "-- Preserve duplicates. Empty relations hash the empty byte string.",
        "-- Do not cast exact text numerics or temporal codecs to lossy native types.",
    ]
    for relation in contract.relations.values():
        cells = ",\n      ".join(cell_expression(c) for c in relation.columns)
        statements.append(
            f"WITH row_hashes AS (\n"
            f"  SELECT lower(to_hex(sha256(to_utf8(concat(\n      {cells}\n"
            f"  ))))) AS row_sha256\n"
            f'  FROM "${{database}}"."{relation.name}"\n)\n'
            f"SELECT '{relation.name}' AS relation_name, count(*) AS row_count,\n"
            f"  lower(to_hex(sha256(to_utf8(coalesce(\n"
            f"    array_join(array_sort(array_agg(row_sha256)), ''), ''\n"
            f"  ))))) AS logical_sha256\nFROM row_hashes;"
        )
    return "\n\n".join(statements) + "\n"
