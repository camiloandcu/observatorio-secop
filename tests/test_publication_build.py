"""Publications require complete fresh dbt evidence, not a process exit code."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from observatorio_secop.publication.build import validate_artifacts

PUBLICATION = "a" * 32


@pytest.mark.parametrize("relative", [True, False], ids=["relative-root", "absolute-root"])
def test_dbt_and_artifact_reader_share_absolute_invocation_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative: bool
) -> None:
    from observatorio_secop.publication import build, store

    class ManifestRegistered(Exception):
        """Stop after the real reader has consumed the simulated dbt manifest."""

    class FakeStore:
        def __init__(self, config):
            pass

        def update_metadata(self, publication_id, metadata):
            return {"metadata": metadata}

        def register_expected_nodes(self, publication_id, nodes):
            assert nodes == ["model.project.one"]
            raise ManifestRegistered

    project = tmp_path / "project"
    models = project / "dbt/models"
    models.mkdir(parents=True)
    (models / "one.sql").write_text("select 1")
    caller = tmp_path / "caller"
    caller.mkdir()
    monkeypatch.chdir(caller)
    artifact_root = Path("reports/publication") if relative else tmp_path / "artifacts"
    expected_root = artifact_root.resolve()
    calls = []

    def dbt_parse(arguments, **kwargs):
        assert arguments[1] == "parse"
        assert kwargs["cwd"] == project
        paths = {
            flag: Path(arguments[arguments.index(flag) + 1])
            for flag in ("--target-path", "--log-path", "--profiles-dir")
        }
        calls.append(paths)
        # dbt resolves a relative target against its project directory, which
        # differs from the caller's working directory in this regression case.
        target = paths["--target-path"]
        if not target.is_absolute():
            target = project / "dbt" / target
        target.mkdir(parents=True)
        manifest = {
            "nodes": {
                "model.project.one": {
                    "resource_type": "model",
                    "name": "one",
                    "config": {"enabled": True},
                }
            }
        }
        (target / "manifest.json").write_text(json.dumps(manifest))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(store, "PublicationStore", FakeStore)
    monkeypatch.setattr(build, "reviewed_metadata", lambda _: {})
    monkeypatch.setattr(build, "_prepare_schemas", lambda *args, **kwargs: None)
    monkeypatch.setattr(build.subprocess, "run", dbt_parse)
    config = SimpleNamespace(host="unused", port=1, dbname="unused", user="unused", password="")
    with pytest.raises(ManifestRegistered):
        build._run_candidate_build(
            config,
            PUBLICATION,
            artifact_root=artifact_root,
            timeout_seconds=30,
            storage_budget_bytes=1024,
            project_root=project,
        )
    assert len(calls) == 1
    for path in calls[0].values():
        assert path.is_absolute()
        assert path.parent.parent == expected_root / PUBLICATION
    target = calls[0]["--target-path"]
    assert (target / "manifest.json").is_file()
    assert (target.parent / "parse.stdout.log").is_file()
    assert (calls[0]["--profiles-dir"] / "profiles.yml").is_file()


def _evidence(target: Path) -> dict:
    nodes = {
        "model.project.fact_contract": {
            "resource_type": "model",
            "name": "fact_contract",
            "schema": f"candidate_{PUBLICATION}_analytics",
            "checksum": {"checksum": "model-bytes"},
        },
        "test.project.unique_fact_contract": {
            "resource_type": "test",
            "name": "unique_fact_contract",
            "schema": f"candidate_{PUBLICATION}_dbt_test__audit",
            "checksum": {"checksum": "test-bytes"},
        },
    }
    manifest = {
        "metadata": {"invocation_id": "same-invocation"},
        "nodes": nodes,
        "sources": {str(index): {"schema": f"candidate_{PUBLICATION}_input"} for index in range(6)},
    }
    results = {
        "metadata": {"invocation_id": "same-invocation"},
        "results": [
            {"unique_id": key, "status": "pass" if key.startswith("test.") else "success"}
            for key in nodes
        ],
    }
    (target / "manifest.json").write_text(json.dumps(manifest))
    (target / "run_results.json").write_text(json.dumps(results))
    return nodes


def test_required_inventory_and_fresh_invocation_form_sealable_receipt(tmp_path: Path) -> None:
    expected = _evidence(tmp_path)
    receipt = validate_artifacts(tmp_path, PUBLICATION, expected)
    assert receipt["required_nodes"] == sorted(expected)
    assert len(receipt["manifest_sha256"]) == len(receipt["run_results_sha256"]) == 64
    assert receipt["dbt_invocation_id"] == "same-invocation"


@pytest.mark.parametrize("status", ["skipped", "fail", "warn", "error"])
def test_any_unexecuted_or_unsuccessful_required_test_blocks_sealing(
    tmp_path: Path, status: str
) -> None:
    expected = _evidence(tmp_path)
    path = tmp_path / "run_results.json"
    result = json.loads(path.read_text())
    result["results"][1]["status"] = status
    path.write_text(json.dumps(result))
    with pytest.raises(ValueError, match="VALIDATION_FAILED"):
        validate_artifacts(tmp_path, PUBLICATION, expected)


@pytest.mark.parametrize("change", ["missing", "duplicate", "stale"])
def test_missing_duplicate_or_stale_success_cannot_authorize_candidate(
    tmp_path: Path, change: str
) -> None:
    expected = _evidence(tmp_path)
    path = tmp_path / "run_results.json"
    result = json.loads(path.read_text())
    if change == "missing":
        result["results"].pop()
    elif change == "duplicate":
        result["results"].append(result["results"][0])
    else:
        result["metadata"]["invocation_id"] = "another-build"
    path.write_text(json.dumps(result))
    with pytest.raises(ValueError, match="INCOMPLETE_RESULTS|DUPLICATE_RESULT|STALE_RESULTS"):
        validate_artifacts(tmp_path, PUBLICATION, expected)


@pytest.mark.parametrize("escape", ["model", "source"])
def test_complete_success_cannot_reference_global_mutable_relations(
    tmp_path: Path, escape: str
) -> None:
    expected = _evidence(tmp_path)
    path = tmp_path / "manifest.json"
    manifest = json.loads(path.read_text())
    if escape == "model":
        manifest["nodes"]["model.project.fact_contract"]["schema"] = "analytics"
    else:
        manifest["sources"]["0"]["schema"] = "silver"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="NAMESPACE_ESCAPE|SOURCE_ESCAPE"):
        validate_artifacts(tmp_path, PUBLICATION, expected)
