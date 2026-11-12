"""Table COP columns sort by amount, not by their formatted text."""

import json
from pathlib import Path

from bi.report import NAMES, generate, identifier

MEASURES = {
    m["name"]: m
    for m in json.loads(
        (Path(__file__).resolve().parents[2] / "bi/measures/analytics.json").read_text()
    )
}


def test_table_money_columns_project_numeric_measures_with_exact_labels(tmp_path):
    pages = tmp_path / "definition" / "pages"
    pages.mkdir(parents=True)
    (pages / "pages.json").write_text('{"pageOrder": []}', encoding="utf-8")
    generate(tmp_path)
    checked = 0
    for name in NAMES:
        folder = pages / identifier(f"page:{name}") / "visuals"
        for path in folder.glob("*/visual.json"):
            visual = json.loads(path.read_text(encoding="utf-8"))["visual"]
            if visual["visualType"] != "tableEx":
                continue
            for column in visual["query"]["queryState"]["Values"]["projections"]:
                if "COP" not in column.get("displayName", ""):
                    continue
                measure = MEASURES[column["field"]["Measure"]["Property"]]
                assert measure["dataType"] != "String", (name, measure["name"])
                assert "FORMAT ( _Value" in measure["formatStringDefinition"]["expression"]
                checked += 1
    assert checked >= 9


def test_identity_coverage_never_rounds_up_to_the_full_policy():
    for name in ("Count Identity Coverage", "Value Identity Coverage"):
        definition = MEASURES[name]
        assert "formatString" not in definition
        expression = definition["formatStringDefinition"]["expression"]
        assert "_Value < 1 && _Value >= 0.99995" in expression
        assert '">99,99%"' in expression
