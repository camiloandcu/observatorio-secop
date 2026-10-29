"""Generate the native PBIR surfaces over the governed SECOP semantic model.

Run after scaffolding a .Report directory. Model creation, data refresh, Desktop
preview and native interaction acceptance are separate integration steps.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

ROOT = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
INK, BLUE, MUTED, BACKGROUND = "#183047", "#155E75", "#4B5D6B", "#F4F6F8"
DETAIL, METRICS = "mart_contract_detail", "Indicadores"
NAMES = ("Panorama", "Entidades y proveedores", "Calidad", "Detalle contractual")


def identifier(value: str, length: int = 20) -> str:
    return hashlib.sha256(("secop-bi:" + value).encode()).hexdigest()[:length]


def literal(value: str | bool | int) -> dict:
    if isinstance(value, bool):
        encoded = str(value).lower()
    elif isinstance(value, int):
        encoded = f"{value}D"
    else:
        encoded = "'" + value.replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": encoded}}}


def color(value: str) -> dict:
    return {"solid": {"color": literal(value)}}


def field(table: str, name: str, measure: bool = False, alias: bool = False) -> dict:
    return {
        "Measure" if measure else "Column": {
            "Expression": {"SourceRef": {"Source" if alias else "Entity": table}},
            "Property": name,
        }
    }


def projection(table: str, name: str, caption: str | None = None, measure: bool = False) -> dict:
    return {
        "field": field(table, name, measure),
        "queryRef": f"{table}.{name}",
        "nativeQueryRef": name,
        **({"displayName": caption} if caption else {}),
    }


def entry(properties: dict, selected: bool = False) -> list[dict]:
    return [{"properties": properties, **({"selector": {"id": "default"}} if selected else {})}]


def predicate(table: str, name: str, value: str | int, measure: bool = False) -> dict:
    left = field("s", name, measure, alias=True)
    condition = (
        {
            "Comparison": {
                "ComparisonKind": 0,
                "Left": left,
                "Right": {"Literal": {"Value": f"{value}L"}},
            }
        }
        if isinstance(value, int)
        else {"In": {"Expressions": [left], "Values": [[literal(value)["expr"]]]}}
    )
    return {
        "Version": 2,
        "From": [{"Name": "s", "Entity": table, "Type": 0}],
        "Where": [{"Condition": condition}],
    }


class Report:
    """Small deterministic encoder; the provided scaffold owns report schemas."""

    def __init__(self, folder: Path):
        self.folder = folder
        self.definition = folder / "definition"
        metadata = self.definition / "pages/pages.json"
        existing = json.loads(metadata.read_text())
        self.pages = [identifier("page:" + name) for name in NAMES]
        # Reuse the scaffold's first page instead of leaving an orphan page file.
        if existing.get("pageOrder"):
            first = existing["pageOrder"][0]
            page = json.loads((metadata.parent / first / "page.json").read_text())
            if page["displayName"] == "Panorama":
                self.pages[0] = first
        self.page_schema = (
            page["$schema"] if existing.get("pageOrder") else f"{ROOT}/page/2.1.0/schema.json"
        )
        self.visuals: dict[int, dict[str, dict]] = {i: {} for i in range(4)}
        self.interactions: dict[int, list[dict]] = {i: [] for i in range(4)}
        self.date_controls: dict[int, dict] = {}
        self.bookmarks: dict[str, dict] = {}

    def add(
        self,
        page: int,
        key: str,
        kind: str,
        bounds: tuple[int, int, int, int],
        title: str,
        roles: dict | None = None,
        objects: dict | None = None,
        transparent: bool = False,
    ) -> dict:
        name = identifier(f"{NAMES[page]}:{key}")
        x, y, w, h = bounds
        order = (len(self.visuals[page]) + 1) * 1000
        padding = 0 if kind in ("textbox", "actionButton") else 2 if kind == "slicer" else 8
        vco = {
            "general": entry({"altText": literal(title)}),
            "background": entry({"show": literal(not transparent), "color": color("#FFFFFF")}),
            "border": entry({"show": literal(False)}),
            "padding": entry({k: literal(padding) for k in ("left", "right", "top", "bottom")}),
        }
        if kind not in ("textbox", "actionButton", "slicer", "cardVisual"):
            vco["title"] = entry(
                {
                    "show": literal(True),
                    "text": literal(title),
                    "fontColor": color(INK),
                    "fontSize": literal(12),
                    "heading": literal("Heading3"),
                }
            )
        visual = {"visualType": kind, "objects": objects or {}, "visualContainerObjects": vco}
        if roles:
            visual["query"] = {"queryState": {k: {"projections": v} for k, v in roles.items()}}
        result = {
            "$schema": f"{ROOT}/visualContainer/2.9.0/schema.json",
            "name": name,
            "position": {"x": x, "y": y, "width": w, "height": h, "z": order, "tabOrder": order},
            "visual": visual,
        }
        self.visuals[page][key] = result
        return result

    def text(
        self,
        page: int,
        key: str,
        text: str,
        bounds: tuple,
        size: int = 12,
        fill: str = INK,
        expression: dict | None = None,
    ) -> dict:
        value = (
            {
                "propertyIdentifier": {"objectName": "values", "propertyName": "expr"},
                "selector": {"id": "value"},
            }
            if expression
            else text
        )
        objects = {
            "general": entry(
                {
                    "paragraphs": [
                        {
                            "textRuns": [
                                {
                                    "value": value,
                                    "textStyle": {
                                        "fontFamily": "Segoe UI",
                                        "fontSize": f"{size}pt",
                                        "color": fill,
                                    },
                                }
                            ]
                        }
                    ]
                }
            )
        }
        if expression:
            objects["values"] = [
                {"properties": {"expr": {"expr": expression}}, "selector": {"id": "value"}}
            ]
        return self.add(page, key, "textbox", bounds, text, objects=objects, transparent=True)

    def metric(self, page: int, key: str, name: str, title: str, bounds: tuple) -> dict:
        return self.add(
            page,
            key,
            "cardVisual",
            bounds,
            title,
            {"Data": [projection(METRICS, name, title, True)]},
            {
                "value": entry(
                    {
                        "fontSize": literal(18),
                        "textWrap": literal(True),
                        "fontColor": color(INK),
                        "showBlankAs": literal("No disponible"),
                        "labelDisplayUnits": literal("1"),
                    },
                    True,
                ),
                "label": entry(
                    {
                        "show": literal(True),
                        "text": literal(title),
                        "fontSize": literal(11),
                        "position": literal("aboveValue"),
                    },
                    True,
                ),
                "accentBar": entry({"show": literal(False)}, True),
                "layout": entry({"paddingUniform": literal(0)}, True),
            },
        )

    def button(
        self,
        page: int,
        key: str,
        title: str,
        action: str,
        bounds: tuple,
        target: str | dict | None = None,
    ) -> dict:
        v = self.add(
            page,
            key,
            "actionButton",
            bounds,
            title,
            objects={
                "text": [
                    {"properties": {"show": literal(True)}},
                    {
                        "selector": {"id": "default"},
                        "properties": {
                            "text": literal(title),
                            "fontSize": literal(11),
                            "fontColor": color(BLUE),
                        },
                    },
                ],
                "fill": [
                    {"properties": {"show": literal(True)}},
                    {"selector": {"id": "default"}, "properties": {"fillColor": color("#DDECF0")}},
                ],
                "outline": entry({"show": literal(False)}),
                "shape": [
                    {"properties": {"tileShape": literal("rectangleRounded")}},
                    {
                        "selector": {"id": "default"},
                        "properties": {"tileShape": literal("rectangleRounded")},
                    },
                ],
            },
            transparent=True,
        )
        link = {"show": literal(True), "type": literal(action), "tooltip": literal(title)}
        prop = {
            "PageNavigation": "navigationSection",
            "Drillthrough": "drillthroughSection",
            "Bookmark": "bookmark",
            "WebUrl": "webUrl",
        }.get(action)
        if prop:
            link[prop] = {"expr": target} if isinstance(target, dict) else literal(target or "")
        v["visual"]["visualContainerObjects"]["visualLink"] = entry(link)
        return v

    def table(self, page: int, key: str, title: str, columns: list[tuple], bounds: tuple) -> dict:
        return self.add(
            page,
            key,
            "tableEx",
            bounds,
            title,
            {"Values": [projection(*c) for c in columns]},
            {
                "columnHeaders": entry(
                    {
                        "autoSizeColumnWidth": literal(False),
                        "wordWrap": literal(False),
                        "columnAdjustment": literal("growToFit"),
                        "fontSize": literal(11),
                        "fontColor": color(INK),
                        "backColor": color("#EAF0F4"),
                    }
                ),
                "values": entry(
                    {
                        "fontSize": literal(11),
                        "fontColor": color(INK),
                        "backColorSecondary": color("#F4F6F8"),
                        "wordWrap": literal(False),
                    }
                ),
                "total": entry({"totals": literal(False)}),
                "grid": entry({"rowPadding": literal(2)}),
            },
        )

    @staticmethod
    def column_widths(visual: dict, widths: list[int]) -> None:
        columns = visual["visual"]["query"]["queryState"]["Values"]["projections"]
        visual["visual"]["objects"]["columnWidth"] = [
            {"selector": {"metadata": column["queryRef"]}, "properties": {"value": literal(width)}}
            for column, width in zip(columns, widths, strict=True)
        ]

    def slicer(
        self,
        page: int,
        key: str,
        table: str,
        column: str,
        title: str,
        bounds: tuple,
        single: bool = False,
    ) -> dict:
        x, y, width, height = bounds
        bounds = (x, y, width, max(height, 76))
        v = self.add(
            page,
            key,
            "slicer",
            bounds,
            title,
            {"Values": [projection(table, column)]},
            {
                "data": entry({"mode": literal("Dropdown")}),
                "header": entry(
                    {"show": literal(True), "text": literal(title), "textSize": literal(10)}
                ),
                "items": entry({"textSize": literal(11), "fontColor": color(INK)}),
                "selection": entry(
                    {"strictSingleSelect": literal(single), "singleSelect": literal(single)}
                ),
            },
        )
        v["visual"]["syncGroup"] = {"groupName": key, "fieldChanges": True, "filterChanges": True}
        return v

    def contract_table(self, page: int, bounds: tuple) -> dict:
        v = self.table(
            page,
            "contracts",
            "Contratos disponibles",
            [
                (DETAIL, "contract_reference", "Contrato"),
                (DETAIL, "supplier_name", "Proveedor"),
                (DETAIL, "signing_date", "Firma"),
                (DETAIL, "Status Label", "Estado"),
                (DETAIL, "contract_value_cop", "Valor (COP)"),
                (DETAIL, "contract_key", "ID"),
            ],
            bounds,
        )
        # Keep the exact key for native drillthrough without letting its SHA dominate.
        self.column_widths(v, [125, 150, 85, 85, 105, 28])
        v["visual"]["visualContainerObjects"]["title"] = entry({"show": literal(False)})
        v["filterConfig"] = {
            "filters": [
                {
                    "name": "Filter" + identifier(f"{page}:selected-contract", 24),
                    "field": field(METRICS, "Show Selected Contract", True),
                    "type": "Advanced",
                    "filter": predicate(METRICS, "Show Selected Contract", 1, True),
                }
            ]
        }
        return v

    def common(self, page: int, *, synthetic: bool = False) -> None:
        self.text(page, "title", NAMES[page], (28, 16, 570, 40), 23)
        for index, (label, x, width) in enumerate(
            (("Panorama", 660, 110), ("Entidades y proveedores", 780, 260), ("Calidad", 1050, 170))
        ):
            self.button(
                page, f"nav-{index}", label, "PageNavigation", (x, 20, width, 34), self.pages[index]
            )
        self.text(
            page,
            "scope",
            "Población publicada",
            (28, 60, 780, 30),
            10,
            expression=field(METRICS, "Scope Label", True),
        )
        self.text(
            page,
            "coverage",
            "Cobertura de población",
            (820, 60, 432, 30),
            10,
            fill="#875A10",
            expression=field(METRICS, "Coverage Label", True),
        )
        if synthetic:
            self.text(
                page,
                "synthetic-disclosure",
                "Datos de prueba",
                (28, 91, 260, 28),
                14,
                "#875A10",
            )
        self.text(
            page,
            "import",
            "Import y observación",
            (292, 80, 960, 40) if synthetic else (28, 80, 1224, 40),
            10,
            MUTED,
            field(METRICS, "Import Label", True),
        )
        self.text(
            page,
            "footer",
            ("Registros fabricados. Los importes no son reales. " if synthetic else "")
            + "Valores nominales observados; municipio de registro y último estado conocido. "
            "Los periodos anteriores pueden revisarse.",
            (28, 686, 900, 25),
            9,
            MUTED,
        )
        if page == 3:
            self.button(page, "back", "Volver con contexto", "Back", (28, 131, 220, 38))
            self.text(
                page,
                "context",
                "Contexto de solo lectura",
                (268, 122, 970, 18),
                9,
                BLUE,
            )
            self.text(
                page,
                "reference-context",
                "Referencia recibida",
                (268, 140, 970, 22),
                9,
                MUTED,
                field(METRICS, "Reference Context Label", True),
            )
            self.text(
                page,
                "supplier-context",
                "Proveedor destacado",
                (268, 162, 970, 22),
                9,
                MUTED,
                field(METRICS, "Supplier Context Label", True),
            )
            return
        for key, table, column, label, x, width in (
            ("entity", "dim_entity", "Entity Label", "Entidad", 28, 260),
            (
                "municipality",
                "dim_municipality",
                "Municipality Label",
                "Municipio de registro",
                300,
                240,
            ),
            ("modality", "dim_modality", "Modality Label", "Modalidad", 552, 250),
            ("status", DETAIL, "Status Label", "Estado observado", 814, 260),
        ):
            self.slicer(page, key, table, column, label, (x, 123, width, 76))
        self.button(page, "reset", "Restablecer", "ClearAllSlicers", (1086, 141, 166, 36))
        hidden = self.slicer(
            page, "date-mode", "DateMode", "mode", "Modo de firma", (28, 200, 260, 66), True
        )
        hidden["isHidden"] = True
        hidden["visual"]["objects"]["general"] = entry(
            {"filter": {"filter": predicate("DateMode", "mode", "ALL")}}
        )
        start = self.slicer(
            page, "date-start", "DateStart", "Date", "Inicio inclusivo", (418, 199, 178, 72), True
        )
        end = self.slicer(
            page, "date-end", "DateEnd", "Date", "Fin exclusivo", (608, 199, 178, 72), True
        )
        start["isHidden"] = end["isHidden"] = True
        self.date_controls[page] = {"mode": hidden, "start": start, "end": end}
        for mode, title, x, width in (
            ("ALL", "Todas las fechas", 28, 120),
            ("INTERVAL", "Intervalo", 158, 105),
            ("MISSING", "Solo sin firma", 273, 133),
        ):
            self.button(
                page,
                "date-" + mode,
                title,
                "Bookmark",
                (x, 202, width, 33),
                identifier("mode:" + mode),
            )
        self.text(
            page,
            "date-label",
            "Modo de firma actual",
            (28, 243, 378, 25),
            10,
            MUTED,
            field(METRICS, "Date Mode Label", True),
        )
        focus = self.slicer(
            page,
            "supplier-focus",
            "SupplierFocus",
            "supplier_label",
            "Proveedor destacado",
            (798, 199, 454, 72),
            True,
        )
        # Focus is optional: clearing it restores all reference contracts.
        focus["visual"]["objects"]["selection"] = entry(
            {
                "strictSingleSelect": literal(False),
                "singleSelect": literal(True),
            }
        )

    def generate_modes(self) -> None:
        for mode, label in (
            ("ALL", "Todas las fechas"),
            ("INTERVAL", "Intervalo de firma"),
            ("MISSING", "Solo sin firma"),
        ):
            sections, targets = {}, []
            for page, controls in self.date_controls.items():
                state = {}
                for key, v in controls.items():
                    targets.append(v["name"])
                    content = (
                        {"display": {"mode": "hidden"}}
                        if key == "mode" or mode != "INTERVAL"
                        else {}
                    )
                    if key == "mode":
                        content["objects"] = {
                            "merge": {
                                "general": entry(
                                    {"filter": {"filter": predicate("DateMode", "mode", mode)}}
                                )
                            }
                        }
                    state[v["name"]] = {"singleVisual": content}
                sections[self.pages[page]] = {"visualContainers": state}
            name = identifier("mode:" + mode)
            self.bookmarks[name] = {
                "$schema": f"{ROOT}/bookmark/2.1.0/schema.json",
                "name": name,
                "displayName": label,
                "options": {
                    "targetVisualNames": targets,
                    "suppressData": False,
                    "suppressDisplay": False,
                    "suppressActiveSection": True,
                    "applyOnlyToTargetVisuals": True,
                },
                "explorationState": {
                    "version": "1.3",
                    "activeSection": self.pages[0],
                    "sections": sections,
                },
            }

    def display_views(
        self, page: int, labels: tuple[str, str], groups: tuple[set[str], set[str]]
    ) -> None:
        """Scope display-only bookmarks to body visuals; never capture filter data."""
        targets = [self.visuals[page][key]["name"] for group in groups for key in sorted(group)]
        for selected, label in enumerate(labels):
            state = {}
            for index, group in enumerate(groups):
                for key in sorted(group):
                    visual = self.visuals[page][key]
                    if index == 1:
                        visual["isHidden"] = True
                    state[visual["name"]] = {
                        "singleVisual": (
                            {} if index == selected else {"display": {"mode": "hidden"}}
                        )
                    }
            name = identifier(f"view:{page}:{selected}")
            self.bookmarks[name] = {
                "$schema": f"{ROOT}/bookmark/2.1.0/schema.json",
                "name": name,
                "displayName": label,
                "options": {
                    "targetVisualNames": targets,
                    "suppressData": True,
                    "suppressDisplay": False,
                    "suppressActiveSection": True,
                    "applyOnlyToTargetVisuals": True,
                },
                "explorationState": {
                    "version": "1.3",
                    "activeSection": self.pages[page],
                    "sections": {self.pages[page]: {"visualContainers": state}},
                },
            }
            self.button(
                page,
                f"view-{selected}",
                label,
                "Bookmark",
                (940 + selected * 162, 684, 150, 28),
                name,
            )

    def write(self) -> None:
        def save(path: Path, value: dict) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")

        for index, name in enumerate(NAMES):
            page = {
                "$schema": self.page_schema,
                "name": self.pages[index],
                "displayName": name,
                "displayOption": "FitToPage",
                "height": 720,
                "width": 1280,
                "objects": {
                    "background": entry({"color": color(BACKGROUND), "transparency": literal(0)})
                },
                "visualInteractions": self.interactions[index],
            }
            if index == 3:
                f = "Filter" + identifier("detail-contract", 24)
                page.update(
                    {
                        "visibility": "HiddenInViewMode",
                        "type": "Drillthrough",
                        "filterConfig": {
                            "filters": [
                                {
                                    "name": f,
                                    "field": field(DETAIL, "contract_key"),
                                    "type": "Categorical",
                                    "howCreated": "Drillthrough",
                                }
                            ]
                        },
                        "pageBinding": {
                            "name": "ContractDetail",
                            "type": "Drillthrough",
                            "acceptsFilterContext": "Default",
                            "parameters": [
                                {
                                    "name": "Param_" + f,
                                    "boundFilter": f,
                                    "fieldExpr": field(DETAIL, "contract_key"),
                                }
                            ],
                        },
                    }
                )
            directory = self.definition / "pages" / self.pages[index]
            save(directory / "page.json", page)
            # These pages are generator-owned; removed controls must not survive regeneration.
            expected = {visual["name"] for visual in self.visuals[index].values()}
            for stale in (directory / "visuals").glob("*"):
                if stale.is_dir() and stale.name not in expected:
                    shutil.rmtree(stale)
            for v in self.visuals[index].values():
                save(directory / "visuals" / v["name"] / "visual.json", v)
        save(
            self.definition / "pages/pages.json",
            {
                "$schema": f"{ROOT}/pagesMetadata/1.1.0/schema.json",
                "pageOrder": self.pages,
                "activePageName": self.pages[0],
            },
        )
        for name, value in self.bookmarks.items():
            save(self.definition / "bookmarks" / f"{name}.bookmark.json", value)
        save(
            self.definition / "bookmarks/bookmarks.json",
            {
                "$schema": f"{ROOT}/bookmarksMetadata/1.0.0/schema.json",
                "items": [{"name": n} for n in self.bookmarks],
            },
        )


def generate(folder: Path, *, synthetic: bool = False) -> None:
    r = Report(folder)
    for page in range(4):
        r.common(page, synthetic=synthetic)
    for key, name, title, x, width in (
        ("count", "Contract Count", "Contratos observados", 28, 286),
        ("value", "Contract Value COP", "Valor contratado (COP)", 326, 450),
        ("median", "Median Value Display", "Mediana contractual (COP)", 788, 464),
    ):
        r.metric(0, key, name, title, (x, 282, width, 70))
    for key, metric, title, x in (
        ("value-month", "Contract Value COP", "Valor mensual (COP)", 28),
        ("count-month", "Contract Count", "Contratos por mes de firma", 650),
    ):
        trend = r.add(
            0,
            key,
            "columnChart",
            (x, 360, 602, 170),
            title,
            {
                "Category": [projection(DETAIL, "signing_month", "Mes de firma")],
                "Y": [projection(METRICS, metric, title, True)],
            },
        )
        trend["visual"]["objects"].update(
            {
                "categoryAxis": entry(
                    {
                        "show": literal(True),
                        "axisType": literal("Categorical"),
                        "showAxisTitle": literal(False),
                        "fontSize": literal(10),
                    }
                ),
                "valueAxis": entry({"showAxisTitle": literal(False), "fontSize": literal(10)}),
            }
        )
        trend["visual"]["visualContainerObjects"]["subTitle"] = entry({"show": literal(False)})
        trend["visual"]["query"]["sortDefinition"] = {
            "sort": [{"field": field(DETAIL, "signing_month"), "direction": "Ascending"}],
            "isDefaultSort": False,
        }
        # Null signatures stay in the governed reference totals, never on a time axis.
        trend["filterConfig"] = {
            "filters": [
                {
                    "name": "Filter" + identifier(key + ":dated-only", 24),
                    "field": field(DETAIL, "signing_month"),
                    "type": "Categorical",
                    "filter": {
                        "Version": 2,
                        "From": [{"Name": "s", "Entity": DETAIL, "Type": 0}],
                        "Where": [
                            {
                                "Condition": {
                                    "Not": {
                                        "Expression": {
                                            "In": {
                                                "Expressions": [
                                                    field("s", "signing_month", alias=True)
                                                ],
                                                "Values": [[{"Literal": {"Value": "null"}}]],
                                            }
                                        }
                                    }
                                }
                            }
                        ],
                    },
                }
            ]
        }
    r.table(
        0,
        "modality-summary",
        "Distribución de la referencia",
        [
            ("dim_modality", "Modality Label", "Modalidad"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value COP", "Valor (COP)", True),
        ],
        (28, 538, 602, 134),
    )
    r.text(0, "contracts-label", "Contratos disponibles", (650, 534, 390, 24), 11)
    r.contract_table(0, (650, 564, 602, 108))
    r.button(0, "drill", "Ver contrato", "Drillthrough", (1050, 534, 202, 26), r.pages[3])
    for key, measure, title, x, width in (
        ("hhi", "HHI Display", "Concentración (HHI)", 28, 236),
        ("known-value", "Known Value COP", "Valor conocido (COP)", 276, 352),
        ("count-coverage", "Count Identity Coverage", "Cobertura por contratos", 640, 300),
        ("value-coverage", "Value Identity Coverage", "Cobertura por valor", 952, 300),
    ):
        r.metric(1, key, measure, title, (x, 282, width, 70))
    rank = r.table(
        1,
        "ranking",
        "Proveedores conocidos",
        [
            ("dim_supplier", "supplier_name", "Proveedor"),
            ("dim_supplier", "supplier_code", "Código"),
            (METRICS, "Ranking Supplier Contract Count", "Contratos", True),
            (METRICS, "Ranking Supplier Value COP", "Valor (COP)", True),
            (METRICS, "Ranking Supplier Share", "% conocido", True),
            (METRICS, "Supplier Row Selected", "Destacado", True),
        ],
        (28, 390, 756, 142),
    )
    rank["filterConfig"] = {
        "filters": [
            {
                "name": "Filter" + identifier("ranking-known", 24),
                "field": field("dim_supplier", "member_status"),
                "type": "Categorical",
                "filter": predicate("dim_supplier", "member_status", "KNOWN"),
            }
        ]
    }
    rank["visual"]["query"]["sortDefinition"] = {
        "sort": [
            {
                "field": field(METRICS, "Ranking Supplier Value COP", True),
                "direction": "Descending",
            },
            {"field": field("dim_supplier", "supplier_code"), "direction": "Ascending"},
        ],
        "isDefaultSort": False,
    }
    r.column_widths(rank, [190, 75, 85, 130, 105, 110])
    r.text(
        1,
        "hhi-state",
        "Estado de concentración",
        (28, 357, 1224, 29),
        11,
        MUTED,
        field(METRICS, "HHI State Label", True),
    )
    r.text(
        1,
        "selected-supplier",
        "Proveedor destacado",
        (796, 390, 456, 36),
        11,
        MUTED,
        field(METRICS, "Selected Supplier Label", True),
    )
    r.metric(
        1,
        "supplier-value",
        "Supplier Focus Value Display",
        "Valor destacado (COP)",
        (796, 430, 224, 72),
    )
    r.metric(
        1,
        "supplier-share",
        "Supplier Focus Share Display",
        "% de la referencia",
        (1032, 430, 220, 72),
    )
    for key in ("supplier-value", "supplier-share"):
        value = r.visuals[1][key]["visual"]["objects"]["value"][0]["properties"]
        value.update({"fontSize": literal(14), "showBlankAs": literal("Sin selección")})
    r.text(1, "unknown-label", "Contratos sin identidad utilizable:", (796, 508, 350, 24), 11)
    r.text(
        1,
        "unknown",
        "Sin identidad",
        (1150, 508, 102, 24),
        11,
        MUTED,
        field(METRICS, "Unknown Supplier Count", True),
    )
    r.text(
        1,
        "reference-rule",
        "Destaca un proveedor con el selector superior; el ranking conserva la referencia.",
        (28, 536, 1008, 27),
        11,
        MUTED,
    )
    r.contract_table(1, (28, 564, 1224, 108))
    r.button(1, "drill", "Ver contrato", "Drillthrough", (1050, 536, 202, 26), r.pages[3])
    for target in r.visuals[1].values():
        if target["name"] == rank["name"]:
            continue
        r.interactions[1].append(
            {"source": rank["name"], "target": target["name"], "type": "NoFilter"}
        )
    r.text(2, "global-label", "Observaciones de la publicación", (28, 282, 732, 28), 16)
    r.text(
        2,
        "global-quality-state",
        "Evidencia y reconciliación",
        (28, 312, 732, 32),
        10,
        MUTED,
        field(METRICS, "Global Quality Label", True),
    )
    for index, (measure, title) in enumerate(
        (
            ("Global Input Display", "Observaciones recibidas"),
            ("Global Accepted Display", "Válidas / recibidas"),
            ("Global Rejected Display", "Rechazadas / recibidas"),
            ("Global Duplicates Display", "Duplicadas / recibidas"),
            ("Global Superseded Display", "Sustituidas / recibidas"),
            ("Global Output Display", "Contratos publicados"),
        )
    ):
        card = r.metric(
            2,
            "global-" + str(index),
            measure,
            title,
            (28 + (index % 3) * 248, 350 + (index // 3) * 74, 236, 68),
        )
        card["visual"]["objects"]["value"][0]["properties"]["fontSize"] = literal(12)
    r.text(
        2,
        "acquisition-label",
        "Adquisición por carril",
        (28, 496, 732, 24),
        14,
    )
    lanes = r.table(
        2,
        "acquisition-lanes",
        "Controles de adquisición publicados",
        [
            ("mart_data_quality", "control_id", "Carril / control"),
            ("mart_data_quality", "numerator", "Valor"),
            ("mart_data_quality", "control_state", "Estado"),
            ("mart_data_quality", "reason", "Límite o motivo"),
        ],
        (28, 524, 732, 104),
    )
    lanes["visual"]["visualContainerObjects"]["title"] = entry({"show": literal(False)})
    r.column_widths(lanes, [340, 65, 120, 170])
    lanes["filterConfig"] = {
        "filters": [
            {
                "name": "Filter" + identifier("acquisition-only", 24),
                "field": field("mart_data_quality", "evaluation_grain"),
                "type": "Categorical",
                "filter": predicate("mart_data_quality", "evaluation_grain", "ACQUISITION_LANE"),
            }
        ]
    }
    r.text(
        2,
        "selection-label",
        "Contratos de referencia",
        (780, 282, 472, 28),
        15,
    )
    for index, (measure, title) in enumerate(
        (
            ("Contract Count", "Contratos de referencia"),
            ("Missing Signature Count", "Sin firma"),
            ("Missing Source Update Count", "Sin actualización fuente"),
            ("Retained Observation Count", "Válida anterior retenida"),
        )
    ):
        r.metric(
            2,
            "reference-" + str(index),
            measure,
            title,
            (780 + (index % 2) * 242, 316 + (index // 2) * 76, 230, 68),
        )
    r.text(
        2,
        "supplier-diagnostics",
        "Proveedor destacado",
        (780, 470, 472, 26),
        14,
    )
    r.text(
        2,
        "selected-supplier",
        "Proveedor destacado",
        (780, 502, 472, 40),
        11,
        MUTED,
        field(METRICS, "Selected Supplier Label", True),
    )
    r.metric(
        2,
        "supplier-count",
        "Supplier Focus Count Display",
        "Contratos destacados",
        (780, 548, 230, 78),
    )
    r.metric(
        2,
        "supplier-value",
        "Supplier Focus Value Display",
        "Valor destacado (COP)",
        (1022, 548, 230, 78),
    )
    for key in ("supplier-count", "supplier-value"):
        value = r.visuals[2][key]["visual"]["objects"]["value"][0]["properties"]
        value.update({"fontSize": literal(14), "showBlankAs": literal("Sin selección")})
    r.text(
        2,
        "coverage-summary",
        "Alcance y cobertura",
        (28, 632, 732, 40),
        9,
        MUTED,
        field(METRICS, "Coverage Summary Label", True),
    )
    for index, (name, title) in enumerate(
        (
            ("Contract", "Identificación del contrato"),
            ("Value", "Valor contratado (COP)"),
            ("Entity", "Entidad y municipio registrado"),
            ("Supplier", "Identidad del proveedor"),
            ("Status", "Estado observado"),
            ("Dates", "Fechas del contrato"),
            ("Observation", "Actualidad de los datos"),
            ("Condition", "Condición de observación"),
            ("Publication", "Población y versión publicada"),
            ("Source State", "Disponibilidad del proceso fuente"),
        )
    ):
        x, y = 28 + (index % 2) * 622, 185 + (index // 2) * 88
        r.text(3, "detail-caption-" + name, title, (x, y, 602, 22), 11, BLUE)
        r.text(
            3,
            "detail-value-" + name,
            title,
            (x, y + 24, 602, 60),
            11,
            INK,
            field(METRICS, "Detail " + name + " Label", True),
        )
    source = r.button(
        3,
        "open-source",
        "Abrir proceso en SECOP II",
        "WebUrl",
        (910, 631, 342, 41),
        field(METRICS, "Source Process URL", True),
    )
    source["visual"]["visualContainerObjects"]["visualLink"][0]["properties"].update(
        {
            "disabledTooltip": literal(
                "Enlace al proceso no disponible; consulte el estado del enlace."
            ),
            "enabledTooltip": literal("Abrir el proceso fuente publicado en SECOP II."),
        }
    )
    shared = {"contracts", "contracts-label", "drill"}
    primary = {
        key
        for key, visual in r.visuals[0].items()
        if 282 <= visual["position"]["y"] < 680 and key not in shared
    }
    monthly = r.table(
        0,
        "monthly-table",
        "Resumen mensual",
        [
            (DETAIL, "signing_month", "Mes de firma"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value COP", "Valor (COP)", True),
        ],
        (28, 282, 602, 244),
    )
    r.column_widths(monthly, [160, 140, 250])
    monthly["filterConfig"] = json.loads(json.dumps(r.visuals[0]["value-month"]["filterConfig"]))
    monthly["filterConfig"]["filters"][0]["name"] = "Filter" + identifier("monthly-table-dated", 24)
    monthly["visual"]["query"]["sortDefinition"] = r.visuals[0]["value-month"]["visual"]["query"][
        "sortDefinition"
    ]
    municipalities = r.table(
        0,
        "municipality-table",
        "Municipios de registro",
        [
            ("dim_municipality", "Municipality Label", "Municipio"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value COP", "Valor (COP)", True),
        ],
        (650, 282, 602, 244),
    )
    r.column_widths(municipalities, [250, 100, 200])
    r.metric(
        0,
        "missing-signatures",
        "Missing Signature Count",
        "Contratos sin firma",
        (28, 538, 230, 78),
    )
    modalities = r.table(
        0,
        "modality-table",
        "Modalidades de contratación",
        [
            ("dim_modality", "Modality Label", "Modalidad"),
            (METRICS, "Contract Count", "Contratos", True),
        ],
        (270, 538, 360, 134),
    )
    r.column_widths(modalities, [225, 90])
    r.display_views(
        0,
        ("Gráficos", "Tablas"),
        (primary, {"monthly-table", "municipality-table", "missing-signatures", "modality-table"}),
    )

    primary = {
        key
        for key, visual in r.visuals[1].items()
        if 282 <= visual["position"]["y"] < 680 and key not in shared | {"reference-rule"}
    }
    entities = r.table(
        1,
        "entity-summary",
        "Resumen por entidad",
        [
            ("dim_entity", "Entity Label", "Entidad"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value COP", "Valor (COP)", True),
            (METRICS, "Count Identity Coverage", "Cobertura conocida", True),
        ],
        (28, 282, 756, 250),
    )
    r.column_widths(entities, [270, 95, 180, 165])
    r.metric(
        1,
        "unknown-value",
        "Unknown Supplier Value COP",
        "Valor sin identidad (COP)",
        (796, 282, 456, 78),
    )
    reasons = r.table(
        1,
        "supplier-reasons",
        "Identidad del proveedor",
        [
            (DETAIL, "Supplier Reason Presentation", "Motivo"),
            (METRICS, "Unknown Supplier Count", "Contratos", True),
            (METRICS, "Unknown Supplier Value COP", "Valor (COP)", True),
        ],
        (796, 372, 456, 160),
    )
    r.column_widths(reasons, [210, 70, 130])
    reasons["filterConfig"] = {
        "filters": [
            {
                "name": "Filter" + identifier("unknown-reasons", 24),
                "field": field(DETAIL, "supplier_identity_status"),
                "type": "Categorical",
                "filter": predicate(DETAIL, "supplier_identity_status", "UNKNOWN"),
            }
        ]
    }
    r.display_views(
        1,
        ("Proveedores", "Entidades"),
        (primary, {"entity-summary", "unknown-value", "supplier-reasons"}),
    )

    primary = {key for key, visual in r.visuals[2].items() if 282 <= visual["position"]["y"] < 680}
    diagnostics = set()
    for index, (measure, caption) in enumerate(
        (
            ("Unknown Entity Count", "Entidad sin identidad"),
            ("Unknown Supplier Count", "Proveedor sin identidad"),
            ("Missing Status Count", "Estado ausente"),
            ("Unprofiled Status Count", "Estado no catalogado"),
            ("Missing Value Count", "Valor ausente"),
            ("Missing Signature Count", "Firma ausente"),
        )
    ):
        key = "diagnostic-" + str(index)
        diagnostics.add(key)
        r.metric(
            2, key, measure, caption, (28 + (index % 3) * 414, 282 + (index // 3) * 86, 402, 76)
        )
    status = r.table(
        2,
        "status-diagnostics",
        "Estados observados",
        [
            (DETAIL, "Status Label", "Estado fuente"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value COP", "Valor (COP)", True),
        ],
        (28, 466, 602, 206),
    )
    r.column_widths(status, [280, 100, 175])
    identity = r.table(
        2,
        "identity-diagnostics",
        "Motivos de identidad",
        [
            (DETAIL, "Supplier Reason Presentation", "Motivo del proveedor"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value COP", "Valor (COP)", True),
        ],
        (650, 466, 602, 206),
    )
    r.column_widths(identity, [280, 100, 175])
    diagnostics.update({"status-diagnostics", "identity-diagnostics"})
    r.display_views(2, ("Publicación", "Diagnóstico"), (primary, diagnostics))

    primary = {key for key in r.visuals[3] if key.startswith("detail-")}
    evidence_view = set()
    for key, measure, title, x in (
        ("condition-evidence", "Detail Condition Label", "Condición de observación", 28),
        ("identity-reasons", "Detail Identity Reasons Label", "Motivos de identidad", 650),
    ):
        evidence_view.update({key + "-caption", key})
        r.text(3, key + "-caption", title, (x, 185, 602, 22), 11, BLUE)
        r.text(3, key, title, (x, 209, 602, 72), 10, INK, field(METRICS, measure, True))
    context = r.table(
        3,
        "context-table",
        "Filtros recibidos",
        [
            (METRICS, "Reference Context Full Label", "Referencia de contratos", True),
            (METRICS, "Supplier Context Label", "Proveedor destacado", True),
        ],
        (28, 293, 1224, 124),
    )
    r.column_widths(context, [738, 440])
    context["visual"]["objects"]["values"][0]["properties"].update(
        {"wordWrap": literal(True), "fontSize": literal(10)}
    )
    evidence_view.add("context-table")
    for key, measure, title, bounds in (
        ("evidence-table", "Detail Evidence Label", "Evidencia atribuible", (28, 429, 756, 197)),
        (
            "warnings-table",
            "Detail Warnings Label",
            "Advertencias publicadas",
            (796, 429, 456, 197),
        ),
    ):
        evidence_view.add(key)
        visual = r.table(3, key, title, [(METRICS, measure, title, True)], bounds)
        r.column_widths(visual, [bounds[2] - 40])
        visual["visual"]["objects"]["values"][0]["properties"].update(
            {"wordWrap": literal(True), "fontSize": literal(10)}
        )
    r.display_views(3, ("Contrato", "Evidencia"), (primary, evidence_view))
    # Read-only comparison tables must not silently redefine the reference context.
    for page, comparison in ((1, entities), (2, status), (2, identity), (3, context)):
        for target in r.visuals[page].values():
            if target["name"] != comparison["name"]:
                r.interactions[page].append(
                    {"source": comparison["name"], "target": target["name"], "type": "NoFilter"}
                )

    r.generate_modes()
    r.write()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_dir", type=Path, help="Existing scaffolded .Report folder")
    parser.add_argument(
        "--synthetic", action="store_true", help="Visibly disclose synthetic test data"
    )
    args = parser.parse_args()
    generate(args.report_dir, synthetic=args.synthetic)
