"""Add the versioned analytical layer to a native Power BI BIM definition.

This module builds metadata only. Desktop evaluates the calculated tables after
the pinned publication Import; it owns credentials, refresh and runtime proof.
"""

from __future__ import annotations

import copy
import json
import re
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
OWNER = {"name": "Secop.Generated", "value": "analytics-v1"}
HELPERS = {"Indicadores", "SupplierFocus", "DateMode", "DateStart", "DateEnd"}
DIMENSIONS = (
    ("entity", "dim_entity", "entity_sk", "entity_sk"),
    ("supplier", "dim_supplier", "supplier_sk", "supplier_sk"),
    ("municipality", "dim_municipality", "municipality_sk", "municipality_sk"),
    ("modality", "dim_modality", "modality_sk", "modality_sk"),
    ("signing_date", "dim_date", "date_sk", "signing_date_sk"),
)


def _lineage(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "secop-bi:analytics:" + name))


def _column(name: str, data_type: str, *, hidden: bool = False) -> dict[str, Any]:
    return {
        "type": "calculatedTableColumn",
        "name": name,
        "dataType": data_type,
        "sourceColumn": f"[{name}]",
        "isNameInferred": True,
        "isDataTypeInferred": True,
        "isHidden": hidden,
    }


def _table(name: str, columns: list[dict], expression: str) -> dict[str, Any]:
    return {
        "name": name,
        "lineageTag": _lineage("table:" + name),
        "annotations": [dict(OWNER)],
        "columns": [
            {**column, "lineageTag": _lineage(name + ":" + column["name"])} for column in columns
        ],
        "partitions": [
            {
                "name": name,
                "mode": "import",
                "source": {"type": "calculated", "expression": expression.strip()},
            }
        ],
    }


def _calendar(end_offset: int) -> str:
    # The fallback is evaluated only to satisfy CALENDAR's scalar arguments.
    # FILTER removes it entirely when the source dimension has no real dates.
    return f"""VAR _Dates = FILTER ( VALUES ( 'dim_date'[calendar_date] ),
    NOT ISBLANK ( 'dim_date'[calendar_date] ) )
VAR _HasDates = COUNTROWS ( _Dates ) > 0
VAR _First = COALESCE ( MINX ( _Dates, 'dim_date'[calendar_date] ), DATE ( 2000, 1, 1 ) )
VAR _Last = COALESCE ( MAXX ( _Dates, 'dim_date'[calendar_date] ), DATE ( 2000, 1, 1 ) )
RETURN FILTER ( CALENDAR ( _First, _Last + {end_offset} ), _HasDates )"""


def _helpers() -> list[dict[str, Any]]:
    indicators = _table(
        "Indicadores",
        [_column("_Anchor", "int64", hidden=True)],
        'DATATABLE ( "_Anchor", INTEGER, { { 1 } } )',
    )
    indicators["description"] = "Versioned reference, supplier-focus and publication measures."
    supplier = _table(
        "SupplierFocus",
        [
            _column("supplier_sk", "string", hidden=True),
            _column("supplier_label", "string"),
            _column("member_status", "string", hidden=True),
        ],
        """SELECTCOLUMNS (
    'dim_supplier',
    "supplier_sk", 'dim_supplier'[supplier_sk],
    "supplier_label", IF ( 'dim_supplier'[member_status] = "KNOWN",
        COALESCE ( 'dim_supplier'[supplier_name], "Proveedor sin nombre" )
            & " (" & COALESCE ( 'dim_supplier'[supplier_code], "Código no disponible" ) & ")",
        "Sin identidad de proveedor utilizable" ),
    "member_status", 'dim_supplier'[member_status]
)""",
    )
    supplier["description"] = (
        "Disconnected focus; never filters reference HHI or identity coverage."
    )
    modes = _table(
        "DateMode",
        [
            _column("mode", "string", hidden=True),
            {**_column("mode_label", "string"), "sortByColumn": "sort_order"},
            _column("sort_order", "int64", hidden=True),
        ],
        """DATATABLE ( "mode", STRING, "mode_label", STRING, "sort_order", INTEGER,
    { { "ALL", "Todas las fechas", 1 },
      { "INTERVAL", "Intervalo de firma", 2 },
      { "MISSING", "Solo sin firma", 3 } } )""",
    )
    dates = [
        _table(
            name,
            [{**_column("Date", "dateTime"), "formatString": "d mmm yyyy"}],
            _calendar(offset),
        )
        for name, offset in (("DateStart", 0), ("DateEnd", 1))
    ]
    dates[1]["description"] = "Exclusive signing end date; includes the day after the last date."
    return [indicators, supplier, modes, *dates]


def _measures() -> list[dict[str, Any]]:
    definitions = json.loads((ROOT / "measures/analytics.json").read_text())
    names: set[str] = set()
    measures = []
    allowed = {"name", "description", "expression", "formatString", "displayFolder", "isHidden"}
    for definition in definitions:
        if definition.get("tableName") != "Indicadores" or definition["name"] in names:
            raise ValueError("Analytical measures must have unique names on Indicadores")
        names.add(definition["name"])
        measure = {key: value for key, value in definition.items() if key in allowed}
        expression = measure["expression"].strip()
        # MCP definitions permit a leading RETURN. Native BIM requires either
        # an expression directly or a VAR block followed by RETURN.
        measure["expression"] = re.sub(r"^RETURN\b\s*", "", expression, count=1, flags=re.I)
        measure["lineageTag"] = _lineage("measure:" + measure["name"])
        measures.append(measure)
    return measures


def _presentation_columns(tables: list[dict]) -> None:
    """Own display labels only; raw keys and filter relationships stay intact."""
    labels = {
        "dim_entity": (
            "Entity Label",
            """IF ( 'dim_entity'[member_status] = "KNOWN",
    COALESCE ( 'dim_entity'[entity_name], "Entidad sin nombre" )
        & " (" & COALESCE ( 'dim_entity'[entity_code], "Código no disponible" ) & ")",
    "Entidad sin identidad utilizable" )""",
        ),
        "dim_municipality": (
            "Municipality Label",
            """IF ( ISBLANK ( 'dim_municipality'[municipality_name] )
    || 'dim_municipality'[municipality_name] = "", "Municipio de registro no disponible",
    'dim_municipality'[municipality_name] )""",
        ),
        "dim_modality": (
            "Modality Label",
            """IF ( ISBLANK ( 'dim_modality'[modality_name] )
    || 'dim_modality'[modality_name] = "", "Modalidad no disponible",
    'dim_modality'[modality_name] )""",
        ),
        "mart_contract_detail": (
            "Status Label",
            """IF ( ISBLANK ( 'mart_contract_detail'[status_raw] )
    || 'mart_contract_detail'[status_raw] = "", "Estado observado no disponible",
    'mart_contract_detail'[status_raw] )""",
        ),
    }
    found = set()
    for table in tables:
        if table["name"] not in labels:
            continue
        found.add(table["name"])
        name, expression = labels[table["name"]]
        columns = table.setdefault("columns", [])
        for column in columns:
            if column["name"] == name and OWNER not in column.get("annotations", []):
                raise ValueError(f"Unowned presentation column: {table['name']}[{name}]")
        table["columns"] = [column for column in columns if column["name"] != name]
        table["columns"].append(
            {
                "name": name,
                "type": "calculated",
                "dataType": "string",
                "expression": expression,
                "summarizeBy": "none",
                "annotations": [dict(OWNER)],
                "lineageTag": _lineage(table["name"] + ":" + name),
            }
        )
    if found != set(labels):
        raise ValueError("Missing source tables for presentation columns")


DETAIL_PRESENTATION = {
    "Evidence Presentation": "EvidenceText([attributable_rejection_evidence])",
    "Warnings Presentation": "WarningsText([quality_warning_codes])",
    "Condition Presentation": "ConditionText([observation_condition])",
    "Supplier Reason Presentation": (
        "IdentityReason([supplier_identity_reason], [supplier_identity_status])"
    ),
}
DETAIL_M = r"""IdentityReason = (reason as nullable text, status as text) as text =>
    if reason = null then
        (if status = "KNOWN" then "Identidad conocida" else "Motivo no disponible")
    else Record.FieldOrDefault([LEGACY_PROJECTION="Proyección histórica",
        MISSING_CODE="Código ausente", PLACEHOLDER_CODE="Código marcador",
        INVALID_CODE="Formato de código inválido"], reason, "Motivo publicado: " & reason),
Parse = (raw as nullable text, expected as type) as any =>
    if raw = null then null else let value = Json.Document(raw) in
    if Value.Is(value, expected) then value else
        error Error.Record("PUBLICATION_DETAIL_EVIDENCE",
            "Estructura de evidencia no válida", null),
CodeText = (code as text) as text => Record.FieldOrDefault([
    IDENTIFIER_NULL="Identificador ausente", AMOUNT_NULL="Valor ausente",
    AMOUNT_NEGATIVE="Valor negativo", AMOUNT_INVALID_FORMAT="Formato de valor inválido",
    AMOUNT_SCALE_EXCEEDED="Valor con más de dos decimales",
    AMOUNT_OVERFLOW="Valor fuera del dominio de la fuente",
    DATE_INVALID="Fecha inválida", DATE_INCONSISTENT="Fechas incoherentes",
    DEPARTMENT_OUT_OF_SCOPE="Departamento fuera del alcance",
    UNKNOWN_MUNICIPALITY="Municipio no encontrado en el catálogo versionado"
], code, "Código publicado: " & code),
Codes = (values as list) as text => Text.Combine(List.Transform(values, each
    if Value.Is(_, type text) then CodeText(_) else
    error Error.Record("PUBLICATION_DETAIL_EVIDENCE", "Código no textual", null)), "; "),
WarningsText = (raw as nullable text) as text => let values = Parse(raw, type list) in
    if values = null then "Advertencias no evaluadas" else if List.IsEmpty(values)
    then "Sin advertencias publicadas" else Codes(values),
ConditionText = (raw as nullable text) as text => let value = Parse(raw, type record) in
    if value = null then "Conteos de condición no disponibles" else
    let later = Record.Field(value, "later_invalid_count"),
        equal = Record.Field(value, "equal_invalid_count") in
    if not Value.Is(later, type number) or not Value.Is(equal, type number)
        or later < 0 or equal < 0 or Number.RoundDown(later) <> later
        or Number.RoundDown(equal) <> equal
    then error Error.Record("PUBLICATION_DETAIL_EVIDENCE", "Conteos de condición inválidos", null)
    else "Inválidas posteriores: " & Text.From(later, "es-CO")
        & "; mismo instante: " & Text.From(equal, "es-CO"),
EvidenceText = (raw as nullable text) as text => let links = Parse(raw, type list) in
    if links = null then "Evidencia atribuible no disponible" else if List.IsEmpty(links)
    then "Sin rechazos atribuibles publicados" else Text.Combine(List.Transform(links, each
    if not Value.Is(_, type record) then
        error Error.Record("PUBLICATION_DETAIL_EVIDENCE", "Enlace de evidencia inválido", null)
    else let relation = Record.Field(_, "temporal_relation"),
        codes = Record.Field(_, "rejection_codes"),
        observed = Record.Field(_, "rejected_observed_at_utc") in
    if not Value.Is(relation, type text) or not Value.Is(codes, type list)
        or (observed <> null and not Value.Is(observed, type text))
    then error Error.Record("PUBLICATION_DETAIL_EVIDENCE", "Campos de evidencia inválidos", null)
    else Record.FieldOrDefault([LATER="Posterior", EQUAL="Mismo instante", EARLIER="Anterior",
        INDETERMINATE="Orden indeterminado"], relation, "Relación publicada: " & relation)
        & "; " & Codes(codes) & "; observado: "
        & (if observed = null then "No disponible" else
            DateTimeZone.ToText(DateTimeZone.SwitchZone(DateTimeZone.FromText(observed), 0),
                "d MMM yyyy HH:mm:ss", "es-CO") & " UTC")), "#(lf)")"""


def _detail_presentation(tables: list[dict]) -> None:
    """Parse only published allowlisted evidence fields, preserving contract grain."""
    detail = next(table for table in tables if table["name"] == "mart_contract_detail")
    for name in DETAIL_PRESENTATION:
        for column in detail["columns"]:
            if column["name"] == name and OWNER not in column.get("annotations", []):
                raise ValueError(f"Unowned detail presentation column: {name}")
    detail["columns"] = [c for c in detail["columns"] if c["name"] not in DETAIL_PRESENTATION]
    for name in DETAIL_PRESENTATION:
        detail["columns"].append(
            {
                "name": name,
                "dataType": "string",
                "sourceColumn": name,
                "isHidden": True,
                "summarizeBy": "none",
                "annotations": [dict(OWNER)],
                "lineageTag": _lineage("detail:" + name),
            }
        )
    source = detail["partitions"][0]["source"]
    expression = source["expression"]
    if isinstance(expression, list):
        expression = "\n".join(expression)
    if expression.startswith("let SecopDetailPresentationSource = ("):
        return
    additions = []
    previous = "SecopDetailPresentationSource"
    for index, (name, formula) in enumerate(DETAIL_PRESENTATION.items()):
        step = f"Presentation{index}"
        additions.append(
            f'{step} = Table.AddColumn({previous}, "{name}", each {formula}, type text)'
        )
        previous = step
    source["expression"] = (
        "let SecopDetailPresentationSource = ("
        + expression
        + "), "
        + DETAIL_M
        + ", "
        + ", ".join(additions)
        + f", PresentationBuffered = Table.Buffer({previous}), "
        + "PresentationChecked = if Table.IsEmpty("
        + "Table.SelectRowsWithErrors(PresentationBuffered)) "
        + 'then PresentationBuffered else error Error.Record("PUBLICATION_DETAIL_EVIDENCE", '
        + '"Evidencia de detalle no válida; reinicia el Import completo.", null) '
        + "in PresentationChecked"
    )


def enrich_model(model: dict) -> dict:
    """Return an enriched copy of a complete BIM document without data or writes.

    The caller must invoke this after compiling the imported tables' M sources;
    calculated helper tables are not publication relations. Existing generated
    objects are replaceable on a second call; unrelated objects are preserved.
    """
    result = copy.deepcopy(model)
    semantic = result.get("model")
    if not isinstance(semantic, dict) or not isinstance(semantic.get("tables"), list):
        raise ValueError("A complete BIM document with model.tables is required")
    tables = semantic["tables"]
    names = [table["name"] for table in tables]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate source table names are not supported")
    for table in tables:
        if table["name"] in HELPERS and OWNER not in table.get("annotations", []):
            raise ValueError(f"Unowned helper table already exists: {table['name']}")
    semantic["tables"] = [table for table in tables if table["name"] not in HELPERS]
    _presentation_columns(semantic["tables"])
    _detail_presentation(semantic["tables"])
    helpers = _helpers()
    helpers[0]["measures"] = _measures()
    semantic["tables"].extend(helpers)
    columns = {
        table["name"]: {column["name"] for column in table.get("columns", [])}
        for table in semantic["tables"]
    }
    relationships = semantic.setdefault("relationships", [])
    owned_names = {"secop_detail_" + name for name, *_ in DIMENSIONS}
    for relationship in relationships:
        if relationship.get("name") in owned_names and OWNER not in relationship.get(
            "annotations", []
        ):
            raise ValueError(f"Unowned relationship already exists: {relationship['name']}")
        if relationship.get("fromTable") in HELPERS or relationship.get("toTable") in HELPERS:
            raise ValueError("Analytical helper tables must remain disconnected")
    semantic["relationships"] = [
        relationship
        for relationship in relationships
        if relationship.get("name") not in owned_names
    ]
    for name, dimension, dimension_key, fact_key in DIMENSIONS:
        if dimension_key not in columns.get(dimension, set()) or fact_key not in columns.get(
            "mart_contract_detail", set()
        ):
            raise ValueError(f"Missing analytical relationship source: {dimension}")
        for relationship in semantic["relationships"]:
            if {relationship.get("fromTable"), relationship.get("toTable")} == {
                "mart_contract_detail",
                dimension,
            }:
                raise ValueError(
                    f"An unrelated relationship already connects {dimension} to detail"
                )
        semantic["relationships"].append(
            {
                "name": "secop_detail_" + name,
                "type": "singleColumn",
                "fromTable": "mart_contract_detail",
                "fromColumn": fact_key,
                "fromCardinality": "many",
                "toTable": dimension,
                "toColumn": dimension_key,
                "toCardinality": "one",
                "crossFilteringBehavior": "oneDirection",
                "securityFilteringBehavior": "oneDirection",
                "isActive": True,
                "annotations": [dict(OWNER)],
            }
        )
    for table in semantic["tables"]:
        expressions = [
            partition["source"]["expression"]
            for partition in table.get("partitions", [])
            if partition["source"]["type"] == "calculated"
        ]
        expressions.extend(
            column["expression"]
            for column in table.get("columns", [])
            if column.get("type") == "calculated" and OWNER in column.get("annotations", [])
        )
        expressions.extend(measure["expression"] for measure in table.get("measures", []))
        for expression in expressions:
            for source, column in re.findall(r"'([^']+)'\[([^]]+)\]", expression):
                if column not in columns.get(source, set()):
                    raise ValueError(f"Missing analytical expression source: {source}[{column}]")
    return result
