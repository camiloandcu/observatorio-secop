"""The signing-month reference filter is available on every navigation page."""

import json

from bi.report import NAMES, generate, identifier


def test_signing_month_slicer_is_synchronized_without_date_bookmark_reset(tmp_path):
    pages = tmp_path / "definition" / "pages"
    pages.mkdir(parents=True)
    (pages / "pages.json").write_text('{"pageOrder": []}', encoding="utf-8")
    generate(tmp_path)

    slicer_ids = []
    for page in NAMES:
        if page == "Detalle contractual":
            continue  # Drill-through detail is deliberately read-only.
        visual_id = identifier(f"{page}:signing-month")
        slicer_ids.append(visual_id)
        path = pages / identifier(f"page:{page}") / "visuals" / visual_id / "visual.json"
        visual = json.loads(path.read_text(encoding="utf-8"))
        config = visual["visual"]
        assert config["visualType"] == "slicer"
        assert config["syncGroup"] == {
            "groupName": "signing-month",
            "fieldChanges": True,
            "filterChanges": True,
        }
        column = config["query"]["queryState"]["Values"]["projections"][0]["field"]["Column"]
        assert column["Expression"]["SourceRef"]["Entity"] == "mart_contract_detail"
        assert column["Property"] == "signing_month"
        assert visual["position"]["x"] + visual["position"]["width"] <= 1086
        condition = visual["filterConfig"]["filters"][0]["filter"]["Where"][0]["Condition"]
        assert condition["Not"]["Expression"]["In"]["Values"] == [[{"Literal": {"Value": "null"}}]]

    for mode in ("ALL", "INTERVAL", "MISSING"):
        path = tmp_path / "definition" / "bookmarks" / f"{identifier('mode:' + mode)}.bookmark.json"
        bookmark = json.loads(path.read_text(encoding="utf-8"))
        assert not set(slicer_ids) & set(bookmark["options"]["targetVisualNames"])


def test_reset_captures_slicers_and_initial_date_mode(tmp_path):
    pages = tmp_path / "definition" / "pages"
    pages.mkdir(parents=True)
    (pages / "pages.json").write_text('{"pageOrder": []}', encoding="utf-8")
    generate(tmp_path)
    name = identifier("reset:filters")
    bookmark = json.loads(
        (tmp_path / "definition" / "bookmarks" / f"{name}.bookmark.json").read_text()
    )
    assert bookmark["options"]["suppressData"] is False
    assert bookmark["options"]["suppressDisplay"] is False
    assert bookmark["options"]["suppressActiveSection"] is True
    for page in NAMES:
        if page == "Detalle contractual":
            continue
        visual_path = pages / identifier(f"page:{page}") / "visuals"
        reset_path = visual_path / identifier(f"{page}:reset") / "visual.json"
        if page == "Contratos del mes":
            assert not reset_path.exists()
            hint_path = visual_path / identifier(f"{page}:month-return-hint") / "visual.json"
            assert "Vuelve para cambiar el mes" in hint_path.read_text(encoding="utf-8")
            assert (visual_path / identifier(f"{page}:back-month") / "visual.json").exists()
        else:
            button = json.loads(reset_path.read_text())
            link = button["visual"]["visualContainerObjects"]["visualLink"][0]["properties"]
            assert link["bookmark"]["expr"]["Literal"]["Value"] == f"'{name}'"
        state = bookmark["explorationState"]["sections"][identifier(f"page:{page}")][
            "visualContainers"
        ]
        for file in visual_path.glob("*/visual.json"):
            visual = json.loads(file.read_text(encoding="utf-8"))
            if visual["visual"]["visualType"] in ("columnChart", "tableEx"):
                assert visual["name"] in bookmark["options"]["targetVisualNames"]
                expected = {"display": {"mode": "hidden"}} if visual.get("isHidden") else {}
                assert state[visual["name"]]["singleVisual"] == expected
        page_index = NAMES.index(page)
        view_path = (
            tmp_path
            / "definition"
            / "bookmarks"
            / f"{identifier(f'view:{page_index}:0')}.bookmark.json"
        )
        if view_path.exists():
            default_view = json.loads(view_path.read_text(encoding="utf-8"))
            view_state = default_view["explorationState"]["sections"][identifier(f"page:{page}")][
                "visualContainers"
            ]
            for visual_id, expected in view_state.items():
                assert state[visual_id] == expected
        for key in (
            "entity",
            "municipality",
            "modality",
            "status",
            "signing-month",
            "supplier-focus",
        ):
            assert state[identifier(f"{page}:{key}")]["singleVisual"] == {}
        for key in ("date-start", "date-end", "date-mode"):
            assert state[identifier(f"{page}:{key}")]["singleVisual"]["display"]["mode"] == "hidden"
        mode = state[identifier(f"{page}:date-mode")]["singleVisual"]["objects"]["merge"]
        filter_value = mode["general"][0]["properties"]["filter"]["filter"]
        assert filter_value["From"][0]["Entity"] == "DateMode"
        assert filter_value["Where"][0]["Condition"]["In"]["Values"] == [
            [{"Literal": {"Value": "'ALL'"}}]
        ]
