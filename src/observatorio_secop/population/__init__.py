"""Shared bounded-population contracts; no acquisition or publication side effects."""

from observatorio_secop.population.conditions import (
    CONDITION_STATES,
    LINK_FIELDS,
    SUMMARY_FIELDS,
    condition_link_id,
    links_arrow_schema,
    summary_arrow_schema,
    validate_condition_link,
    validate_condition_set,
    validate_condition_summary,
)
from observatorio_secop.population.core import PopulationError, canonical_json, digest_json
from observatorio_secop.population.coverage import (
    ACQUISITION_SCHEMA_VERSION,
    CONDITION_SCHEMA_VERSION,
    COVERAGE_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    ROW_SCHEMA_VERSION,
    legacy_coverage,
    validate_acquisition,
    validate_coverage,
)
from observatorio_secop.population.reconciliation import (
    RECONCILIATION_SCHEMA_VERSION,
    validate_reconciliation,
)
from observatorio_secop.population.scope import SCOPE_SCHEMA_VERSION, PopulationScope

__all__ = [
    "ACQUISITION_SCHEMA_VERSION",
    "CONDITION_SCHEMA_VERSION",
    "CONDITION_STATES",
    "COVERAGE_SCHEMA_VERSION",
    "LINK_FIELDS",
    "MANIFEST_SCHEMA_VERSION",
    "PopulationError",
    "PopulationScope",
    "RECONCILIATION_SCHEMA_VERSION",
    "ROW_SCHEMA_VERSION",
    "SCOPE_SCHEMA_VERSION",
    "SUMMARY_FIELDS",
    "canonical_json",
    "condition_link_id",
    "digest_json",
    "legacy_coverage",
    "links_arrow_schema",
    "summary_arrow_schema",
    "validate_acquisition",
    "validate_condition_link",
    "validate_condition_set",
    "validate_condition_summary",
    "validate_coverage",
    "validate_reconciliation",
]
