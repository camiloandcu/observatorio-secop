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
AMBER, BORDER, TINT, DISABLED = "#875A10", "#DCE3E8", "#DDECF0", "#E6EAED"
DETAIL, METRICS = "mart_contract_detail", "Indicadores"
# Shared 1280×720 grid: header, two filter rows, an action toolbar, then the body.
FILTER_Y, FILTER_H, DATE_Y, TOOLBAR_Y, BODY_Y, BODY_END = 100, 62, 166, 234, 274, 680
# Evidence actions are reached after the content they act on.
ACTIONS = ("drill", "month-drill", "open-source", "back", "back-month")
# Desktop 2.159 gave first keyboard focus to the highest tabOrder on every page.
TAB_DESCENDING = True
NAMES = (
    "Panorama",
    "Entidades y proveedores",
    "Calidad",
    "Detalle contractual",
    "Contratos",
    "Contratos del mes",
)


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
        self.visuals: dict[int, dict[str, dict]] = {i: {} for i in range(len(NAMES))}
        self.interactions: dict[int, list[dict]] = {i: [] for i in range(len(NAMES))}
        self.date_controls: dict[int, dict] = {}
        self.view_targets: dict[int, set[str]] = {i: set() for i in range(len(NAMES))}
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
        framed = kind not in ("textbox", "actionButton")
        vco = {
            "general": entry({"altText": literal(title)}),
            "background": entry({"show": literal(not transparent), "color": color("#FFFFFF")}),
            "border": entry(
                {
                    "show": literal(framed),
                    "color": color(BORDER),
                    "radius": literal(6),
                    "width": literal(1),
                }
                if framed
                else {"show": literal(False)}
            ),
            "padding": entry({k: literal(padding) for k in ("left", "right", "top", "bottom")}),
        }
        if not framed:
            # Text and buttons need no "More options" header in reading view; data
            # visuals keep it for table access. Desktop edit mode still shows it.
            vco["visualHeader"] = entry({"show": literal(False)})
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
        style: str = "secondary",
    ) -> dict:
        # Primary marks the page's evidence action; current marks the active page.
        ink, fill = {"primary": ("#FFFFFF", BLUE), "current": ("#FFFFFF", INK)}.get(
            style, (BLUE, TINT)
        )
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
                            "fontColor": color(ink),
                        },
                    },
                    {"selector": {"id": "disabled"}, "properties": {"fontColor": color(MUTED)}},
                ],
                "fill": [
                    {"properties": {"show": literal(True)}},
                    {"selector": {"id": "default"}, "properties": {"fillColor": color(fill)}},
                    {"selector": {"id": "disabled"}, "properties": {"fillColor": color(DISABLED)}},
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
        bounds = (x, y, width, max(height, FILTER_H))
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
                (METRICS, "Contract Row Value Exact COP", "Valor (COP)", True),
                (DETAIL, "contract_key", "ID"),
            ],
            bounds,
        )
        # Keep the exact key for native drillthrough without letting its SHA dominate.
        self.column_widths(v, [180, 330, 120, 150, 230, 160])
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
        self.text(page, "brand", "Observatorio SECOP", (28, 6, 150, 20), 9, MUTED)
        if synthetic:
            self.text(page, "synthetic-disclosure", "Datos de prueba", (178, 4, 200, 24), 11, AMBER)
        self.text(page, "title", NAMES[page], (28, 26, 640, 42), 20)
        x = 712
        for index, (label, width) in enumerate(
            (
                ("Panorama", 110),
                ("Contratos", 110),
                ("Entidades y proveedores", 200),
                ("Calidad", 96),
            )
        ):
            target = (0, 4, 1, 2)[index]
            if page != 3:
                self.button(
                    page,
                    f"nav-{index}",
                    label,
                    "PageNavigation",
                    (x, 26, width, 32),
                    self.pages[target],
                    style="current" if page == target else "secondary",
                )
            x += width + 8
        self.text(
            page,
            "scope",
            "Población publicada",
            (28, 70, 680, 22),
            10,
            expression=field(METRICS, "Scope Label", True),
        )
        self.text(
            page,
            "coverage",
            "Cobertura de población",
            (716, 70, 250, 22),
            10,
            fill=AMBER,
            expression=field(METRICS, "Coverage Label", True),
        )
        self.text(
            page,
            "import",
            "Import y observación",
            (980, 60, 272, 38),
            9,
            MUTED,
            field(METRICS, "Import Label", True),
        )
        footer = self.text(
            page,
            "footer",
            ("Datos e importes de prueba. " if synthetic else "")
            + "COP observados, sin ajuste por inflación. Municipio de registro. "
            "Los periodos pueden revisarse.",
            (28, 692, 1224, 22),
            9,
            MUTED,
        )
        if page == 1:
            paragraph = footer["visual"]["objects"]["general"][0]["properties"]["paragraphs"][0]
            paragraph["textRuns"][0]["value"] = (
                ("Datos e importes de prueba. " if synthetic else "")
                + "Cobertura de referencia: contratos/COP con proveedor identificable. "
                "COP sin ajuste por inflación; municipio de registro; periodos revisables."
            )
        if page == 3:
            self.button(page, "back", "Volver a la lista", "Back", (28, 108, 170, 34))
            self.text(
                page,
                "context",
                "Contexto de solo lectura",
                (214, 102, 760, 20),
                9,
                BLUE,
            )
            self.text(
                page,
                "reference-context",
                "Referencia recibida",
                (214, 122, 760, 20),
                9,
                MUTED,
                field(METRICS, "Reference Context Label", True),
            )
            self.text(
                page,
                "supplier-context",
                "Proveedor destacado",
                (214, 142, 760, 20),
                9,
                MUTED,
                field(METRICS, "Supplier Context Label", True),
            )
            return
        for key, table, column, label, x, width in (
            ("entity", "dim_entity", "Entity Label", "Entidad", 28, 250),
            (
                "municipality",
                "dim_municipality",
                "Municipality Label",
                "Municipio de registro",
                288,
                190,
            ),
            ("modality", "dim_modality", "Modality Label", "Modalidad", 488, 200),
            ("status", DETAIL, "Status Label", "Estado observado", 698, 170),
            ("signing-month", DETAIL, "signing_month", "Mes de firma (intersecta modo)", 878, 208),
        ):
            slicer = self.slicer(page, key, table, column, label, (x, FILTER_Y, width, FILTER_H))
            if key == "signing-month":
                # Restrict offered months only. Leaving this slicer clear must
                # still include contracts without a signing date.
                slicer["filterConfig"] = {
                    "filters": [
                        {
                            "name": "Filter" + identifier("signing-month:dated-only", 24),
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
                                                        "Values": [
                                                            [{"Literal": {"Value": "null"}}]
                                                        ],
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
        if page == 5:
            self.text(
                page,
                "month-return-hint",
                "Vuelve para cambiar el mes",
                (1096, 108, 156, 46),
                10,
                MUTED,
            )
        else:
            self.button(
                page,
                "reset",
                "Restablecer",
                "Bookmark",
                (1096, 114, 156, 34),
                identifier("reset:filters"),
            )
        hidden = self.slicer(
            page, "date-mode", "DateMode", "mode", "Modo de firma", (28, DATE_Y, 260, 62), True
        )
        hidden["isHidden"] = True
        hidden["visual"]["objects"]["general"] = entry(
            {"filter": {"filter": predicate("DateMode", "mode", "ALL")}}
        )
        start = self.slicer(
            page,
            "date-start",
            "DateStart",
            "Date",
            "Inicio inclusivo",
            (392, DATE_Y, 170, 62),
            True,
        )
        end = self.slicer(
            page, "date-end", "DateEnd", "Date", "Fin exclusivo", (572, DATE_Y, 170, 62), True
        )
        start["isHidden"] = end["isHidden"] = True
        self.date_controls[page] = {"mode": hidden, "start": start, "end": end}
        for mode, title, x, width in (
            ("ALL", "Todo rango", 28, 110),
            ("INTERVAL", "Intervalo", 146, 96),
            ("MISSING", "Solo sin firma", 250, 130),
        ):
            self.button(
                page,
                "date-" + mode,
                title,
                "Bookmark",
                (x, DATE_Y + 4, width, 30),
                identifier("mode:" + mode),
            )
        self.text(
            page,
            "date-label",
            "Modo de firma actual",
            (28, DATE_Y + 38, 360, 22),
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
            (798, DATE_Y, 454, 62),
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

    def generate_filter_reset(self) -> None:
        """Restore the controls' initial state without changing the current page."""

        def visual_state(visual: dict, content: dict) -> dict:
            result = {"singleVisual": content}
            filters = visual.get("filterConfig", {}).get("filters", [])
            if filters:
                for item in filters:
                    item.setdefault("howCreated", "User")
                # Bookmark data replaces visual-filter state, so preserve the
                # authored restrictions while clearing transient selections.
                result["filters"] = {
                    "byExpr": [
                        {
                            "name": item["name"],
                            "type": item["type"],
                            "expression": item["field"],
                            "filter": item["filter"],
                            "howCreated": 1,
                        }
                        for item in filters
                    ]
                }
            return result

        sections, targets = {}, []
        for page in self.date_controls:
            state = {}
            for key in (
                "entity",
                "municipality",
                "modality",
                "status",
                "signing-month",
                "supplier-focus",
                "date-start",
                "date-end",
                "date-mode",
            ):
                visual = self.visuals[page][key]
                targets.append(visual["name"])
                content = {}
                if key in ("date-start", "date-end", "date-mode"):
                    content["display"] = {"mode": "hidden"}
                if key == "date-mode":
                    content["objects"] = {
                        "merge": {
                            "general": entry(
                                {"filter": {"filter": predicate("DateMode", "mode", "ALL")}}
                            )
                        }
                    }
                state[visual["name"]] = visual_state(visual, content)
            # Restore the initial charts/tables view while clearing transient
            # row/bar selections; an empty state makes a hidden visual visible.
            for visual in self.visuals[page].values():
                if (
                    visual["visual"]["visualType"] in ("columnChart", "tableEx")
                    or visual["name"] in self.view_targets[page]
                ):
                    targets.append(visual["name"])
                    content = {"display": {"mode": "hidden"}} if visual.get("isHidden") else {}
                    state[visual["name"]] = visual_state(visual, content)
            sections[self.pages[page]] = {"visualContainers": state}
        name = identifier("reset:filters")
        self.bookmarks[name] = {
            "$schema": f"{ROOT}/bookmark/2.1.0/schema.json",
            "name": name,
            "displayName": "Restablecer filtros",
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
        self,
        page: int,
        labels: tuple[str, str],
        groups: tuple[set[str], set[str]],
        toolbar_y: int = TOOLBAR_Y,
    ) -> None:
        """Scope display-only bookmarks to body visuals; never capture filter data."""
        targets = [self.visuals[page][key]["name"] for group in groups for key in sorted(group)]
        self.view_targets[page].update(targets)
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
                (996 + selected * 132, toolbar_y, 124, 30),
                name,
            )

    def keyboard_sequence(self, page: int) -> list[str]:
        """Approved focus order: header, filters, toolbar, content, actions, footnote."""

        def band(key: str) -> int:
            y = self.visuals[page][key]["position"]["y"]
            if key == "footer":
                return 5
            if key in ACTIONS:
                return 4
            return 0 if y < FILTER_Y else 1 if y < TOOLBAR_Y else 2 if y < BODY_Y else 3

        # Within a band, creation order keeps each column's reading order.
        return sorted(self.visuals[page], key=band)

    def apply_keyboard_order(self) -> None:
        for page in self.visuals:
            sequence = self.keyboard_sequence(page)
            for index, key in enumerate(sequence):
                rank = len(sequence) - index if TAB_DESCENDING else index + 1
                self.visuals[page][key]["position"]["tabOrder"] = rank * 1000

    def write(self) -> None:
        def save(path: Path, value: dict) -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")

        self.apply_keyboard_order()

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
            if index in (3, 5):
                target_field = "contract_key" if index == 3 else "signing_month"
                f = "Filter" + identifier("detail-contract" if index == 3 else "detail-month", 24)
                page.update(
                    {
                        "visibility": "HiddenInViewMode",
                        "type": "Drillthrough",
                        "filterConfig": {
                            "filters": [
                                {
                                    "name": f,
                                    "field": field(DETAIL, target_field),
                                    "type": "Categorical",
                                    "howCreated": "Drillthrough",
                                }
                            ]
                        },
                        "pageBinding": {
                            "name": "ContractDetail" if index == 3 else "MonthContracts",
                            "type": "Drillthrough",
                            "acceptsFilterContext": "Default",
                            "parameters": [
                                {
                                    "name": "Param_" + f,
                                    "boundFilter": f,
                                    "fieldExpr": field(DETAIL, target_field),
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
                "pageOrder": [self.pages[i] for i in (0, 4, 1, 2, 3, 5)],
                "activePageName": self.pages[0],
            },
        )
        # Bookmarks are generator-owned too; stale ones would target removed pages.
        for stale in (self.definition / "bookmarks").glob("*.bookmark.json"):
            if stale.name.removesuffix(".bookmark.json") not in self.bookmarks:
                stale.unlink()
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
    for page in range(len(NAMES)):
        r.common(page, synthetic=synthetic)
    for key, name, title, x, width in (
        ("count", "Contract Count", "Contratos observados", 28, 286),
        ("value", "Contract Value Display", "Valor contratado (COP)", 326, 450),
        ("median", "Median Value Display", "Mediana contractual (COP)", 788, 464),
    ):
        r.metric(0, key, name, title, (x, BODY_Y, width, 70))
    for key, metric, title, x in (
        ("value-month", "Contract Value COP", "Valor mensual (COP)", 28),
        ("count-month", "Contract Count", "Contratos por mes de firma", 650),
    ):
        trend = r.add(
            0,
            key,
            "columnChart",
            (x, BODY_Y + 80, 602, BODY_END - BODY_Y - 80),
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
                "dataPoint": entry({"defaultColor": color(BLUE)}),
                # Exact counts are readable labels; COP values stay exact in Tablas.
                "labels": entry(
                    {
                        "show": literal(key == "count-month"),
                        "fontSize": literal(10),
                        "color": color(INK),
                        "labelDisplayUnits": literal("1"),
                    }
                ),
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
    r.text(
        0,
        "month-hint",
        "Selecciona un mes para ver sus contratos.",
        (28, TOOLBAR_Y + 4, 600, 24),
        10,
        MUTED,
    )
    r.button(
        0,
        "month-drill",
        "Ver contratos del mes",
        "Drillthrough",
        (796, TOOLBAR_Y, 188, 30),
        r.pages[5],
        style="primary",
    )
    table_bounds = (28, BODY_Y, 1224, BODY_END - BODY_Y)
    table_widths = [140, 290, 250, 100, 120, 180, 90]
    r.text(
        4,
        "contracts-label",
        "Selecciona un contrato para ver detalle",
        (28, TOOLBAR_Y + 2, 700, 28),
        12,
    )
    contracts = r.contract_table(4, table_bounds)
    contracts["visual"]["query"]["queryState"]["Values"]["projections"].insert(
        1, projection("dim_entity", "Entity Label", "Entidad")
    )
    r.column_widths(contracts, table_widths)
    r.button(
        4,
        "drill",
        "Ver contrato",
        "Drillthrough",
        (1072, TOOLBAR_Y, 180, 32),
        r.pages[3],
        style="primary",
    )
    r.text(
        5, "contracts-label", "Contratos del mes seleccionado", (160, TOOLBAR_Y + 2, 650, 28), 12
    )
    month_contracts = r.contract_table(5, table_bounds)
    month_contracts["visual"]["query"]["queryState"]["Values"]["projections"].insert(
        1, projection("dim_entity", "Entity Label", "Entidad")
    )
    r.column_widths(month_contracts, table_widths)
    r.button(
        5,
        "drill",
        "Ver contrato",
        "Drillthrough",
        (1072, TOOLBAR_Y, 180, 32),
        r.pages[3],
        style="primary",
    )
    r.button(5, "back-month", "Volver", "Back", (28, TOOLBAR_Y, 120, 32))

    for key, measure, title, x, width in (
        ("hhi", "HHI Display", "Concentración (HHI)", 28, 236),
        ("known-value", "Known Value Display", "Valor con proveedor identificado (COP)", 276, 352),
        (
            "count-coverage",
            "Count Identity Coverage",
            "% contratos con proveedor conocido",
            640,
            300,
        ),
        (
            "value-coverage",
            "Value Identity Coverage",
            "% valor con proveedor conocido",
            952,
            300,
        ),
    ):
        r.metric(1, key, measure, title, (x, BODY_Y, width, 70))
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
        (28, 354, 756, 152),
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
    r.column_widths(rank, [200, 90, 78, 170, 88, 84])
    r.text(
        1,
        "hhi-state",
        "Estado de concentración",
        (28, TOOLBAR_Y + 4, 940, 24),
        10,
        MUTED,
        field(METRICS, "HHI State Label", True),
    )
    r.text(
        1,
        "selected-supplier",
        "Proveedor destacado",
        (796, 354, 456, 24),
        11,
        MUTED,
        field(METRICS, "Selected Supplier Label", True),
    )
    r.metric(
        1,
        "supplier-value",
        "Supplier Focus Value Display",
        "Valor destacado (COP)",
        (796, 382, 270, 64),
    )
    r.metric(
        1,
        "supplier-share",
        "Supplier Focus Share Display",
        "% de la referencia",
        (1076, 382, 176, 64),
    )
    for key in ("supplier-value", "supplier-share"):
        value = r.visuals[1][key]["visual"]["objects"]["value"][0]["properties"]
        value.update({"fontSize": literal(14), "showBlankAs": literal("Sin selección")})
    r.text(1, "unknown-label", "Contratos sin identidad utilizable:", (796, 454, 300, 24), 11)
    r.text(
        1,
        "unknown",
        "Sin identidad",
        (1100, 454, 152, 24),
        11,
        MUTED,
        field(METRICS, "Unknown Supplier Count", True),
    )
    r.text(
        1,
        "reference-rule",
        "Destaca un proveedor con el selector superior; el ranking conserva la referencia.",
        (28, 516, 900, 24),
        11,
        MUTED,
    )
    r.contract_table(1, (28, 548, 1224, BODY_END - 548))
    r.button(
        1,
        "drill",
        "Ver contrato",
        "Drillthrough",
        (1072, 512, 180, 30),
        r.pages[3],
        style="primary",
    )
    r.text(2, "global-label", "Observaciones de la publicación", (28, BODY_Y, 732, 30), 14)
    r.text(
        2,
        "global-quality-state",
        "Evidencia y reconciliación",
        (28, BODY_Y + 30, 732, 22),
        9,
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
            (28 + (index % 3) * 248, 330 + (index // 3) * 68, 236, 60),
        )
        card["visual"]["objects"]["value"][0]["properties"]["fontSize"] = literal(12)
    r.text(
        2,
        "acquisition-label",
        "Adquisición por carril",
        (28, 460, 732, 28),
        13,
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
        (28, 490, 732, 128),
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
        (780, BODY_Y, 472, 30),
        14,
    )
    for index, (measure, title) in enumerate(
        (
            ("Contract Count", "Contratos de referencia"),
            ("Missing Signature Count", "Sin firma"),
            ("Missing Source Update Count", "Sin actualización fuente"),
            ("Retained Observation Display", "Válida anterior retenida"),
        )
    ):
        r.metric(
            2,
            "reference-" + str(index),
            measure,
            title,
            (780 + (index % 2) * 242, 306 + (index // 2) * 72, 230, 66),
        )
    r.visuals[2]["reference-3"]["visual"]["objects"]["value"][0]["properties"]["fontSize"] = (
        literal(12)
    )
    r.text(
        2,
        "supplier-diagnostics",
        "Proveedor destacado",
        (780, 450, 472, 30),
        14,
    )
    r.text(
        2,
        "selected-supplier",
        "Proveedor destacado",
        (780, 480, 472, 24),
        11,
        MUTED,
        field(METRICS, "Selected Supplier Label", True),
    )
    r.metric(
        2,
        "supplier-count",
        "Supplier Focus Count Display",
        "Contratos destacados",
        (780, 508, 200, 64),
    )
    r.metric(
        2,
        "supplier-value",
        "Supplier Focus Value Display",
        "Valor destacado (COP)",
        (990, 508, 262, 64),
    )
    for key in ("supplier-count", "supplier-value"):
        value = r.visuals[2][key]["visual"]["objects"]["value"][0]["properties"]
        value.update({"fontSize": literal(14), "showBlankAs": literal("Sin selección")})
    r.text(
        2,
        "coverage-summary",
        "Alcance y cobertura",
        (28, 624, 732, 44),
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
        x, y = 28 + (index % 2) * 622, 208 + (index // 2) * 94
        r.text(3, "detail-caption-" + name, title, (x, y, 602, 22), 11, BLUE)
        r.text(
            3,
            "detail-value-" + name,
            title,
            (x, y + 24, 602, 62),
            11,
            INK,
            field(METRICS, "Detail " + name + " Label", True),
        )
    source = r.button(
        3,
        "open-source",
        "Abrir proceso en SECOP II",
        "WebUrl",
        (1012, 108, 240, 34),
        field(METRICS, "Source Process URL", True),
        style="primary",
    )
    source["visual"]["visualContainerObjects"]["visualLink"][0]["properties"].update(
        {
            "disabledTooltip": literal(
                "Enlace al proceso no disponible; consulte el estado del enlace."
            ),
            "enabledTooltip": literal("Abrir el proceso fuente publicado en SECOP II."),
        }
    )
    # A web-link button never disables itself, so a missing or disallowed URL
    # must also change its label and colors rather than look actionable.
    objects = source["visual"]["objects"]
    objects["text"][1]["properties"].update(
        {
            "text": {"expr": field(METRICS, "Source Action Label", True)},
            "fontColor": {
                "solid": {"color": {"expr": field(METRICS, "Source Action Text Color", True)}}
            },
        }
    )
    objects["fill"][1]["properties"]["fillColor"] = {
        "solid": {"color": {"expr": field(METRICS, "Source Action Fill", True)}}
    }
    shared = {"contracts", "contracts-label", "drill"}

    def body(page: int, excluded: set[str] = frozenset()) -> set[str]:
        return {
            key
            for key, visual in r.visuals[page].items()
            if BODY_Y <= visual["position"]["y"] < BODY_END and key not in shared | excluded
        }

    primary = body(0)
    half = (BODY_END - BODY_Y - 10) // 2
    monthly = r.table(
        0,
        "monthly-table",
        "Resumen mensual",
        [
            (DETAIL, "signing_month", "Mes de firma"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value Exact COP", "Valor (COP)", True),
        ],
        (28, BODY_Y, 602, half),
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
            (METRICS, "Contract Value Exact COP", "Valor (COP)", True),
        ],
        (650, BODY_Y, 602, half),
    )
    r.column_widths(municipalities, [250, 100, 200])
    r.metric(
        0,
        "missing-signatures",
        "Missing Signature Count",
        "Contratos sin firma",
        (650, BODY_END - half, 230, 66),
    )
    modalities = r.table(
        0,
        "modality-table",
        "Modalidades de contratación",
        [
            ("dim_modality", "Modality Label", "Modalidad"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value Exact COP", "Valor (COP)", True),
        ],
        (28, BODY_END - half, 602, half),
    )
    r.column_widths(modalities, [260, 100, 200])
    r.display_views(
        0,
        ("Gráficos", "Tablas"),
        (primary, {"monthly-table", "municipality-table", "missing-signatures", "modality-table"}),
    )

    primary = body(1, {"reference-rule"})
    entities = r.table(
        1,
        "entity-summary",
        "Resumen por entidad",
        [
            ("dim_entity", "Entity Label", "Entidad"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value Exact COP", "Valor (COP)", True),
            (METRICS, "Count Identity Coverage", "Cobertura conocida", True),
        ],
        (28, BODY_Y, 756, 506 - BODY_Y),
    )
    r.column_widths(entities, [300, 95, 180, 140])
    r.metric(
        1,
        "unknown-value",
        "Unknown Supplier Value Display",
        "Valor sin identidad (COP)",
        (796, BODY_Y, 456, 66),
    )
    reasons = r.table(
        1,
        "supplier-reasons",
        "Identidad del proveedor",
        [
            (DETAIL, "Supplier Reason Presentation", "Motivo"),
            (METRICS, "Unknown Supplier Count", "Contratos", True),
            (METRICS, "Unknown Supplier Value Exact COP", "Valor (COP)", True),
        ],
        (796, BODY_Y + 76, 456, 506 - BODY_Y - 76),
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

    primary = body(2)
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
            2, key, measure, caption, (28 + (index % 3) * 414, BODY_Y + (index // 3) * 74, 402, 66)
        )
    status = r.table(
        2,
        "status-diagnostics",
        "Estados observados",
        [
            (DETAIL, "Status Label", "Estado fuente"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value Exact COP", "Valor (COP)", True),
        ],
        (28, 428, 602, BODY_END - 428),
    )
    r.column_widths(status, [280, 100, 175])
    identity = r.table(
        2,
        "identity-diagnostics",
        "Motivos de identidad",
        [
            (DETAIL, "Supplier Reason Presentation", "Motivo del proveedor"),
            (METRICS, "Contract Count", "Contratos", True),
            (METRICS, "Contract Value Exact COP", "Valor (COP)", True),
        ],
        (650, 428, 602, BODY_END - 428),
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
        r.text(3, key + "-caption", title, (x, 208, 602, 22), 11, BLUE)
        r.text(3, key, title, (x, 232, 602, 72), 10, INK, field(METRICS, measure, True))
    context = r.table(
        3,
        "context-table",
        "Filtros recibidos",
        [
            (METRICS, "Reference Context Full Label", "Referencia de contratos", True),
            (METRICS, "Supplier Context Label", "Proveedor destacado", True),
        ],
        (28, 314, 1224, 120),
    )
    r.column_widths(context, [738, 440])
    context["visual"]["objects"]["values"][0]["properties"].update(
        {"wordWrap": literal(True), "fontSize": literal(10)}
    )
    evidence_view.add("context-table")
    for key, measure, title, bounds in (
        (
            "evidence-table",
            "Detail Evidence Label",
            "Evidencia atribuible",
            (28, 444, 756, BODY_END - 444),
        ),
        (
            "warnings-table",
            "Detail Warnings Label",
            "Advertencias publicadas",
            (796, 444, 456, BODY_END - 444),
        ),
    ):
        evidence_view.add(key)
        visual = r.table(3, key, title, [(METRICS, measure, title, True)], bounds)
        r.column_widths(visual, [bounds[2] - 40])
        visual["visual"]["objects"]["values"][0]["properties"].update(
            {"wordWrap": literal(True), "fontSize": literal(10)}
        )
    r.display_views(3, ("Contrato", "Evidencia"), (primary, evidence_view), toolbar_y=170)
    # Read-only comparison tables must not silently redefine the reference context.
    for page, comparison in ((1, rank), (1, entities), (2, status), (2, identity), (3, context)):
        for target in r.visuals[page].values():
            if target["name"] != comparison["name"]:
                r.interactions[page].append(
                    {"source": comparison["name"], "target": target["name"], "type": "NoFilter"}
                )

    r.generate_modes()
    r.generate_filter_reset()
    r.write()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_dir", type=Path, help="Existing scaffolded .Report folder")
    parser.add_argument(
        "--synthetic", action="store_true", help="Visibly disclose synthetic test data"
    )
    args = parser.parse_args()
    generate(args.report_dir, synthetic=args.synthetic)
