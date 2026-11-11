"""Keyboard focus follows header, filters, toolbar, content, actions, footnote."""

import json

import pytest

from bi.report import ACTIONS, NAMES, TAB_DESCENDING, generate, identifier


@pytest.fixture(scope="module")
def focus(tmp_path_factory):
    root = tmp_path_factory.mktemp("report")
    folder = root / "definition" / "pages"
    folder.mkdir(parents=True)
    (folder / "pages.json").write_text('{"pageOrder": []}', encoding="utf-8")
    generate(root)
    result = {}
    for name in NAMES:
        directory = folder / identifier(f"page:{name}") / "visuals"
        visuals = [json.loads(p.read_text()) for p in directory.glob("*/visual.json")]
        ordered = sorted(
            (v for v in visuals if not v.get("isHidden")),
            key=lambda v: v["position"]["tabOrder"],
            reverse=TAB_DESCENDING,
        )
        result[name] = [v["name"] for v in ordered]
    return result


def key(page, name):
    return identifier(f"{page}:{name}")


@pytest.mark.parametrize("page", NAMES)
def test_focus_starts_in_header_and_ends_with_actions_then_footnote(focus, page):
    order = focus[page]
    assert order[:2] == [key(page, "brand"), key(page, "title")]
    assert order[-1] == key(page, "footer")
    actions = {key(page, a) for a in ACTIONS if key(page, a) in order}
    assert set(order[-1 - len(actions) : -1]) == actions


@pytest.mark.parametrize("page", [n for n in NAMES if n != "Detalle contractual"])
def test_navigation_and_filters_precede_content(focus, page):
    order = focus[page]
    for earlier, later in (
        ("nav-0", "entity"),
        ("entity", "supplier-focus"),
        ("signing-month", "date-ALL"),
        ("supplier-focus", "contracts" if key(page, "contracts") in order else "footer"),
    ):
        assert order.index(key(page, earlier)) < order.index(key(page, later))


def test_text_and_buttons_have_no_header_trap(tmp_path):
    folder = tmp_path / "definition" / "pages"
    folder.mkdir(parents=True)
    (folder / "pages.json").write_text('{"pageOrder": []}', encoding="utf-8")
    generate(tmp_path)
    for path in folder.glob("*/visuals/*/visual.json"):
        visual = json.loads(path.read_text())["visual"]
        header = visual["visualContainerObjects"].get("visualHeader")
        if visual["visualType"] in ("textbox", "actionButton"):
            assert header[0]["properties"]["show"]["expr"]["Literal"]["Value"] == "false"
        else:
            assert header is None
