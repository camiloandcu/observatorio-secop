"""Execute the existing governed Gold macros against a pinned safe detail view."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
import yaml
from jinja2 import Environment, StrictUndefined
from psycopg import sql

from observatorio_secop.warehouse.config import WarehouseConfig

from .build import ROOT, _graph_digest
from .errors import PublicationError
from .readers import reader_cursor, validate_descriptor

MACROS = {
    "overview": "gold_overview_query",
    "entity_monthly": "gold_entity_monthly_query",
    "concentration": "gold_concentration_query",
    "detail": "gold_detail_query",
    "quality": "gold_quality_query",
}
ARRAY_FILTERS = (
    "entity_keys",
    "municipality_keys",
    "modality_keys",
    "statuses",
    "exclude_statuses",
)
FILTERS = (*ARRAY_FILTERS, "start_date", "end_date", "missing_signatures", "selected_supplier")


def _parameters(filters: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(filters, dict) or set(filters) - set(FILTERS):
        raise PublicationError("PUBLICATION_INVALID_FILTER", "Unsupported Gold filter")
    parameters = dict.fromkeys(FILTERS)
    parameters.update(filters)
    for name in ARRAY_FILTERS:
        value = parameters[name]
        if value is not None and (
            not isinstance(value, list | tuple) or any(not isinstance(v, str) for v in value)
        ):
            raise PublicationError(
                "PUBLICATION_INVALID_FILTER", "Gold selectors must be text arrays"
            )
    for name in ("start_date", "end_date"):
        value = parameters[name]
        if value is not None:
            try:
                parameters[name] = date.fromisoformat(value) if isinstance(value, str) else value
                if type(parameters[name]) is not date:
                    raise ValueError
            except ValueError as error:
                raise PublicationError(
                    "PUBLICATION_INVALID_FILTER", "Invalid signing date"
                ) from error
    if (
        parameters["missing_signatures"] is not None
        and type(parameters["missing_signatures"]) is not bool
    ):
        raise PublicationError(
            "PUBLICATION_INVALID_FILTER", "Missing-signature selector must be boolean"
        )
    if parameters["selected_supplier"] is not None and not isinstance(
        parameters["selected_supplier"], str
    ):
        raise PublicationError("PUBLICATION_INVALID_FILTER", "Supplier selector must be text")
    return parameters


@dataclass(frozen=True)
class _TrustedRelation:
    schema: str

    def __str__(self) -> str:
        return sql.Identifier(self.schema, "mart_contract_detail").as_string()


class _CompilerErrors:
    @staticmethod
    def raise_compiler_error(message: str) -> None:
        raise PublicationError("PUBLICATION_INVALID_FILTER", "Unsupported governed query")


def _compile_query(metric: str, namespace: str, metadata: dict[str, Any], root: Path) -> str:
    if metadata.get("dbt_graph_sha256") != _graph_digest(root):
        raise PublicationError(
            "PUBLICATION_CONTRACT_UNAVAILABLE",
            "Installed query implementation differs from publication",
        )
    variables = yaml.safe_load((root / "dbt/dbt_project.yml").read_text())["vars"]
    for name, key, relative in (
        ("gold_catalog_sha256", "catalog_sha256", "contracts/gold_metrics.yaml"),
        ("gold_policy_sha256", "policy_sha256", "config/gold_policy.yaml"),
    ):
        checksum = hashlib.sha256((root / relative).read_bytes()).hexdigest()
        if metadata.get(key) != checksum or variables[name] != checksum:
            raise PublicationError(
                "PUBLICATION_CONTRACT_UNAVAILABLE",
                "Installed metric contract differs from publication",
            )
    policy = yaml.safe_load((root / "config/gold_policy.yaml").read_text())
    for key in ("gold_policy_version", "gold_min_count_coverage", "gold_min_value_coverage"):
        if variables[key] != policy[key]:
            raise PublicationError("PUBLICATION_CONTRACT_UNAVAILABLE", "Metric policy differs")
    environment = Environment(undefined=StrictUndefined, extensions=["jinja2.ext.do"])
    module = environment.from_string(
        (root / "dbt/macros/gold_queries.sql").read_text()
    ).make_module(
        {
            "var": lambda key, default=None: variables.get(key, default),
            "return": lambda value: value,
            "exceptions": _CompilerErrors,
        }
    )
    return str(getattr(module, MACROS[metric])(_TrustedRelation(namespace), filters={}, bound=True))


def query_metric(
    config: WarehouseConfig,
    descriptor: dict[str, Any],
    metric: str,
    filters: dict[str, Any] | None = None,
    *,
    limit: int = 100,
    offset: int = 0,
    project_root: Path = ROOT,
    statement_timeout_ms: int = 60_000,
) -> list[dict[str, Any]]:
    """No formula fork or untrusted SQL: render reviewed macros, bind all filter values."""
    if metric not in MACROS or type(limit) is not int or not 1 <= limit <= 100:
        raise PublicationError("PUBLICATION_INVALID_FILTER", "Unsupported bounded metric query")
    if type(offset) is not int or offset < 0:
        raise PublicationError("PUBLICATION_INVALID_FILTER", "Invalid metric pagination")
    parameters = _parameters({} if filters is None else filters)
    checked = validate_descriptor(config, descriptor)
    with reader_cursor(config, statement_timeout_ms=statement_timeout_ms) as cursor:
        statement = _compile_query(
            metric, checked["read_namespace"], checked["metadata"], project_root
        )
        parameters.update(_publication_id=checked["publication_id"], _limit=limit, _offset=offset)
        try:
            cursor.execute(
                "SELECT %(_publication_id)s::text AS publication_id, measured.* FROM ("
                + statement.strip().removesuffix(";")
                + ') measured ORDER BY to_jsonb(measured)::text COLLATE "C" '
                + "LIMIT %(_limit)s OFFSET %(_offset)s",
                parameters,
            )
        except psycopg.errors.InvalidTextRepresentation as error:
            raise PublicationError(
                "PUBLICATION_INVALID_FILTER", "Gold selection is invalid or unsupported"
            ) from error
        return [dict(row) for row in cursor.fetchall()]
