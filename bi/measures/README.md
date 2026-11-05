# Analytical measures

`analytics.json` is the native MCP measure-definition array for table
`Indicadores`. Expressions use the supported Gold catalog and the reviewed
`gold-hhi-policy-v1` binding. They contain definitions, not imported data or
credentials. Report labels are Spanish; measure names remain stable English
identifiers for report bindings and reconciliation queries. The current layer
contains 82 measures. When creating new measures through MCP, omit the read/type
metadata `dataType`; the server infers it from the expression.

## Required model

- One `mart_contract_detail` row per publication/scope/contract key, with COP
  imported as TOM Decimal only after checking individual and aggregate bounds.
- Connected one-to-many dimensions: `dim_entity`, `dim_supplier`,
  `dim_municipality`, `dim_modality`, `dim_date`; single direction toward detail.
- Disconnected `SupplierFocus[supplier_sk,supplier_label,member_status]`,
  `DateMode[mode]` (`ALL`, `INTERVAL`, `MISSING`), `DateStart[Date]`,
  `DateEnd[Date]`, `publication_metadata`, `mart_data_quality` and
  `ImportContext[import_started_at_utc]`.
- Date mode and each active interval boundary use single selection; the end
  date is exclusive. Inactive date bounds are ignored. The date dimension can
  provide chart axes but does not substitute for the explicit interval controls.

## Filter behavior

Reference measures remove only connected `dim_supplier` filters. They preserve
entity, municipality, modality, status, connected calendar and explicit contract
row filters. The date predicate is then applied to that reference. They never
remove all detail filters, so detail-row visibility remains meaningful.

`SupplierFocus` applies only to selected supplier metrics and
`Show Selected Contract`. No focus means reference contracts; multiple focuses
are invalid. Use `Show Selected Contract = 1` on contract tables. Unknown identity
can select contracts but has no real-supplier share or ranking position.

Ranking measures capture the current connected supplier row before restoring
the full reference, then select that known supplier's contracts. The ranking
denominator and HHI remain the full known reference, including suppliers outside
the displayed top10. `Supplier Row Selected` supports both a visible selection
label and a visual treatment; color alone is insufficient.

## Monetary, concentration and quality rules

Count and total are zero for a valid empty selection; median remains blank.
Medians are recomputed over non-null values, retain four-decimal fixed precision
and preserve half-cent results for even populations. They are not summed across
stored monthly aggregates. Ratios and HHI retain full calculation precision;
compare them with governed SQL using absolute tolerance `1e-12`.

HHI requires one selected `dim_entity` key and known entity identity. It pools
supplier amounts across the selected months and sums squared known-value shares
on scale0–1. `Policy State` checks the expected version and SHA256 before applying
its approved 100% count/value coverage minima. A different policy is unavailable
until reviewed; the measures do not silently adopt new thresholds. Gold's null
reason priority is preserved after explicit UI date/entity/policy checks.

Contract quality measures apply reference filters and count unique contracts.
Global counters use only `mart_data_quality` observation controls and retain
blank legacy/missing values. Reconciliation checks the actual observation
equations with six non-null counters; a successful refresh does not establish
quality or current source validity. Acquisition lanes remain separate controls.

`Import Label` describes the loaded Import's start timestamp produced by M. It
does not use `NOW()` or imply successful completion at that instant.
Date labels use short Spanish months. The header separates observation and load
on two lines, with minutes and explicit UTC; observation details retain seconds.
Contract dates have no artificial time component. Interval ends remain exclusive.
`Median Value Display` uses Colombian number notation and explicit COP without
automatic scaling: an integer median appears as `20 COP`, while a half-cent
median retains its precision, for example `15,0150 COP`. The underlying numerical
measure continues to support reconciliation and calculations.
`Source Process URL` returns only the existing sanitized URL for one contract
whose published link state is `AVAILABLE`; it never constructs a process URL.

Detail presentation parses published JSON in M before loading derived text
columns. Only temporal relation, rejection codes and observation time from each
attributable link are displayed; raw payloads, local paths and storage locators
are excluded. Multiple links remain attached to one contract. Malformed
structures fail the Import rather than becoming a reassuring empty message.
Legacy unassessed conditions retain unavailable counts.

`Date Mode Label` defaults to all dates consistently with the numerical measures.
Compact reference and supplier labels support the header;
`Reference Context Full Label` exposes actual selected entity, municipality,
modality, status and date values in a wrapped read-only table.

Authoritative executable definitions: [metric catalog](../../contracts/gold_metrics.yaml),
[policy](../../config/gold_policy.yaml), [governed SQL](../../dbt/macros/gold_queries.sql).
See [Gold analytics](../../docs/gold-analytics.md) for population and source limits.
Native application, DAX validation, SQL reconciliation and accessibility evidence
are required separately; definitions alone establish none of those results.

Money cards and ordinary tables use backend-formatted text to avoid cent loss
when a visual serializes very large numeric values. The supplier ranking keeps
its numeric measure projected for monetary sorting; a dynamic format expression
provides a quoted exact COP label. Numeric expressions and denominators remain
unchanged. Native rendering and ordering must validate this presentation layer.
See [dynamic measure formats](https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-dynamic-format-strings).
