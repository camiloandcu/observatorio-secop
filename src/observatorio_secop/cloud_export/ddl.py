"""DDL templates from the same reviewed portable schema; no cloud actions."""

from __future__ import annotations

from .contract import Contract


def ddl(contract: Contract) -> str:
    statements = []
    for relation in contract.relations.values():
        columns = ",\n  ".join(f"`{c.name}` {c.glue_type}" for c in relation.columns)
        partition = (
            "\nPARTITIONED BY (`signing_year_partition` string, `signing_month_partition` string)"
            if relation.partition_by_signing
            else ""
        )
        statements.append(
            f"CREATE EXTERNAL TABLE `${{database}}`.`{relation.name}` (\n  {columns}\n)"
            f"{partition}\nSTORED AS PARQUET\n"
            f"LOCATION 's3://${{bucket}}/${{prefix}}/{relation.name}/';"
        )
    return "\n\n".join(statements) + "\n"
