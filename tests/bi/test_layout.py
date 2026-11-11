"""Generated pages keep a readable, non-overlapping layout on the 1280×720 canvas."""

import json
from itertools import combinations

import pytest

from bi.report import NAMES, generate, identifier


@pytest.fixture(scope="module")
def pages(tmp_path_factory):
    root = tmp_path_factory.mktemp("report")
    folder = root / "definition" / "pages"
    folder.mkdir(parents=True)
    (folder / "pages.json").write_text('{"pageOrder": []}', encoding="utf-8")
    generate(root)
    result = {}
    for name in NAMES:
        directory = folder / identifier(f"page:{name}") / "visuals"
        result[name] = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in sorted(directory.glob("*/visual.json"))
        ]
    return result


def box(visual):
    p = visual["position"]
    return p["x"], p["y"], p["x"] + p["width"], p["y"] + p["height"]


def overlaps(a, b):
    ax0, ay0, ax1, ay1 = box(a)
    bx0, by0, bx1, by1 = box(b)
    return ax0 < bx1 and bx0 < ax1 and ay0 < by1 and by0 < ay1


def font_size(visual):
    run = visual["visual"]["objects"]["general"][0]["properties"]["paragraphs"][0]["textRuns"][0]
    return float(run["textStyle"]["fontSize"].removesuffix("pt"))


@pytest.mark.parametrize("name", NAMES)
def test_visuals_fit_the_canvas(pages, name):
    for visual in pages[name]:
        x0, y0, x1, y1 = box(visual)
        assert x0 >= 0 and y0 >= 0 and x1 <= 1280 and y1 <= 720, visual["name"]


@pytest.mark.parametrize("name", NAMES)
def test_text_boxes_leave_room_for_their_font(pages, name):
    # Shorter boxes render a scrollbar beside the text in Desktop.
    for visual in pages[name]:
        if visual["visual"]["visualType"] == "textbox":
            assert visual["position"]["height"] >= 2 * font_size(visual), visual["name"]


@pytest.mark.parametrize("name", NAMES)
def test_simultaneously_visible_visuals_do_not_overlap(pages, name):
    hidden_mode = identifier(f"{name}:date-mode")
    interval = {identifier(f"{name}:date-start"), identifier(f"{name}:date-end")}
    visuals = [v for v in pages[name] if v["name"] != hidden_mode]
    always = [v for v in visuals if not v.get("isHidden")]
    alternate = [v for v in visuals if v.get("isHidden") and v["name"] not in interval]
    toggled = {
        v["name"]
        for v in always
        if any(overlaps(v, other) for other in alternate)  # replaced by the alternate view
    }
    states = {
        "default": always,
        "alternate": [v for v in always if v["name"] not in toggled] + alternate,
        "interval": always + [v for v in visuals if v["name"] in interval],
    }
    for state, shown in states.items():
        for a, b in combinations(shown, 2):
            assert not overlaps(a, b), (state, a["name"], b["name"])
