"""Check descriptor identity, lifetime and native model compilation."""

import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parents[2] / "bi" / "build.py"
SPEC = importlib.util.spec_from_file_location("bi_build", MODULE)
bi_build = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bi_build)


def descriptor():
    now = datetime.now(UTC) - timedelta(seconds=5)
    identity = "a" * 32
    return {
        "version": 1,
        "publication_id": identity,
        "read_namespace": "publication_" + identity,
        "resolved_at": now.isoformat(),
        "operation_expires_at": (now + timedelta(hours=1)).isoformat(),
    }


@pytest.mark.parametrize("version", [True, "1", 0, None])
def test_descriptor_requires_integer_version_one(version):
    value = descriptor()
    value["version"] = version
    with pytest.raises(ValueError):
        bi_build.publication_literal(value)


@pytest.mark.parametrize("identity", ["a" * 31, "A" * 32, "';SELECT 1", 1])
def test_descriptor_rejects_unsafe_identity(identity):
    value = descriptor()
    value["publication_id"] = identity
    with pytest.raises(ValueError):
        bi_build.publication_literal(value)


def test_namespace_must_match_identity():
    value = descriptor()
    value["read_namespace"] = "published"
    with pytest.raises(ValueError):
        bi_build.publication_literal(value)


def test_deadline_cannot_be_extended():
    value = descriptor()
    value["operation_expires_at"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    with pytest.raises(ValueError):
        bi_build.publication_literal(value)


def test_expired_descriptor_creates_no_project(tmp_path):
    value = descriptor()
    start = datetime.now(UTC) - timedelta(hours=2)
    value.update(
        resolved_at=start.isoformat(), operation_expires_at=(start + timedelta(hours=1)).isoformat()
    )
    output = tmp_path / "project"
    with pytest.raises(ValueError):
        bi_build.build(output, value, "localhost:5432", "test")
    assert not output.exists()


def test_publication_changes_only_shared_literal():
    first = descriptor()
    second = {**first, "publication_id": "b" * 32, "read_namespace": "publication_" + "b" * 32}
    left = bi_build.compile_model(first, "localhost:5432", "test")
    right = bi_build.compile_model(second, "localhost:5432", "test")
    assert left["model"]["tables"] == right["model"]["tables"]
    assert left["model"]["expressions"] != right["model"]["expressions"]
    assert len(left["model"]["tables"]) == 14


def test_native_array_is_serialized_before_text_import():
    model = bi_build.compile_model(descriptor(), "localhost:5432", "test")
    detail = next(t for t in model["model"]["tables"] if t["name"] == "mart_contract_detail")
    query = detail["partitions"][0]["source"]["expression"]
    assert '[name = "quality_warning_codes", kind = "json"]' in query
    assert "CAST(to_json(" in query


def test_rejects_conflicting_native_measure_formats(tmp_path, monkeypatch):
    import json

    from bi import analytics

    definitions = tmp_path / "measures"
    definitions.mkdir()
    (definitions / "analytics.json").write_text(
        json.dumps(
            [
                {
                    "tableName": "Indicadores",
                    "name": "Money",
                    "expression": "1",
                    "formatString": "0.00",
                    "formatStringDefinition": {"expression": '"0.00"'},
                }
            ]
        )
    )
    monkeypatch.setattr(analytics, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="mutually exclusive"):
        analytics._measures()
