from __future__ import annotations

import shutil
import threading
from pathlib import Path
from unittest.mock import Mock

import pytest

from observatorio_secop.cloud_export.build import build
from observatorio_secop.cloud_export.contract import load_contract
from observatorio_secop.cloud_export.errors import ExportError
from observatorio_secop.cloud_export.identity import writer_recipe
from observatorio_secop.cloud_export.package import verify

from .test_package import fixture, identity_fields, package_root


@pytest.fixture
def local_builder(tmp_path, monkeypatch):
    fields = identity_fields()
    descriptor = {"publication_id": fields["publication_id"], "metadata": fields["binding"]}
    monkeypatch.setattr(
        "observatorio_secop.cloud_export.build.load_warehouse_config", lambda _: None
    )
    monkeypatch.setattr("observatorio_secop.cloud_export.build.resolve", lambda *a: descriptor)
    monkeypatch.setattr("observatorio_secop.cloud_export.build.checked", lambda *a: None)

    def phase(mode, request, scratch, budget):
        if mode == "validate":
            return {"result": {"silver_valid": {"rows": 2}}, "peak_increment": 0}
        base = tmp_path / "fixtures"
        base.mkdir(exist_ok=True)
        source = package_root(base)
        if not source.exists():
            fixture(source)
        manifest = verify(source, load_contract())
        shutil.copytree(source, Path(request["stage"]), dirs_exist_ok=True)
        return {"result": manifest, "peak_increment": 0}

    monkeypatch.setattr("observatorio_secop.cloud_export.build.phase", phase)
    kwargs = {
        "config_path": tmp_path / "unused",
        "silver_root": tmp_path / "unused",
        "output": tmp_path / "output",
        "publication_id": fields["publication_id"],
    }
    return kwargs, phase


def test_writer_change_between_phases_prevents_seal(local_builder, monkeypatch):
    kwargs, _ = local_builder
    before = writer_recipe()
    monkeypatch.setattr(
        "observatorio_secop.cloud_export.build.writer_recipe",
        Mock(side_effect=[before, {**before, "revision": "f" * 64}]),
    )
    with pytest.raises(ExportError, match="recipe changed"):
        build(**kwargs)
    output = kwargs["output"]
    assert all(p.name.startswith(".") for p in output.iterdir())


def test_failure_immediately_before_rename_leaves_only_incomplete_stage(local_builder, monkeypatch):
    kwargs, _ = local_builder

    def fail(*args):
        raise OSError("simulated failure before rename")

    monkeypatch.setattr("observatorio_secop.cloud_export.build._rename_no_replace", fail)
    with pytest.raises(OSError):
        build(**kwargs)
    stages = list(kwargs["output"].glob(".staging-*"))
    assert len(stages) == 1
    with pytest.raises(ExportError):
        verify(stages[0], load_contract())


def test_concurrent_builders_cannot_share_destination(local_builder, monkeypatch):
    kwargs, original_phase = local_builder
    entered, release = threading.Event(), threading.Event()
    errors = []

    def slow(mode, *args):
        if mode == "validate":
            entered.set()
            assert release.wait(10)
        return original_phase(mode, *args)

    monkeypatch.setattr("observatorio_secop.cloud_export.build.phase", slow)

    def first():
        try:
            build(**kwargs)
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=first)
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(ExportError, match="Another builder"):
            build(**kwargs)
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive() and errors == []
    assert len([p for p in kwargs["output"].iterdir() if not p.name.startswith(".")]) == 1


def test_deadline_expires_after_output_before_seal(local_builder, monkeypatch):
    kwargs, original_phase = local_builder

    def expired(mode, request, scratch, budget):
        result = original_phase(mode, request, scratch, budget)
        if mode == "export":
            budget.started -= 301
        return result

    monkeypatch.setattr("observatorio_secop.cloud_export.build.phase", expired)
    with pytest.raises(ExportError, match="deadline"):
        build(**kwargs)
    assert all(p.name.startswith(".") for p in kwargs["output"].iterdir())


def test_compression_failure_cannot_create_manifest(tmp_path, monkeypatch):
    from observatorio_secop.cloud_export.budget import Budget
    from observatorio_secop.cloud_export.package import RelationWriter

    relation = load_contract().relations["dim_entity"]

    class FailedWriter:
        def __init__(self, path, *args, **kwargs):
            Path(path).write_bytes(b"incomplete")

        def write_table(self, *args, **kwargs):
            raise OSError("compression failure")

        def close(self):
            pass

    monkeypatch.setattr("observatorio_secop.cloud_export.package.pq.ParquetWriter", FailedWriter)
    writer = RelationWriter(tmp_path, relation, Budget())
    with pytest.raises(OSError, match="compression"):
        writer.add([{c.name: None for c in relation.columns}])
    writer.abort()
    assert not (tmp_path / "manifest.json").exists()
    with pytest.raises(ExportError):
        verify(tmp_path, load_contract())
