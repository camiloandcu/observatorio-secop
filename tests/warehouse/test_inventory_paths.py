from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.cli import main
from observatorio_secop.warehouse.errors import SilverBundleError

from .conftest import rewrite_manifest
from .inventory_helpers import INVALID_CASES, cli_environment, corrupt_inventory, declare


@pytest.mark.parametrize("case", INVALID_CASES)
def test_invalid_inventory_fails_before_cli_connection(
    silver_root: Path, monkeypatch: pytest.MonkeyPatch, capsys, case: str
) -> None:
    message = corrupt_inventory(silver_root / "versions/version-1/manifest.json", case)
    cli_environment(silver_root, monkeypatch)
    connect = Mock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr("psycopg.connect", connect)
    for command in ["verify", "load"]:
        assert main([command]) == 2
        assert message in capsys.readouterr().err
    connect.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        None,
        "",
        " ",
        12,
        "/tmp/outside.parquet",
        "../outside.parquet",
        "./data/part.parquet",
        "data/../data/part.parquet",
        "data//part.parquet",
        "data/part.parquet/",
        "data\\part.parquet",
        "data/part\0.parquet",
    ],
)
def test_noncanonical_paths_are_rejected(silver_root: Path, path: object) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    rewrite_manifest(manifest, lambda document: document["files"][0].update(path=path))
    with pytest.raises(SilverBundleError, match="canonical"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize("kind", ["internal", "external", "dangling", "directory", "fifo"])
@pytest.mark.parametrize("declared", [False, True])
def test_links_and_special_entries_cannot_bypass_inventory(
    silver_root: Path, kind: str, declared: bool
) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    data = manifest.parent / "data"
    parquet = next(data.rglob("*.parquet"))
    path = data / "entry.parquet"
    if kind == "internal":
        path.symlink_to(parquet)
    elif kind == "external":
        target = silver_root.parent / "outside.parquet"
        target.write_bytes(parquet.read_bytes())
        path.symlink_to(target)
    elif kind == "dangling":
        path.symlink_to(data / "missing")
    elif kind == "directory":
        path.symlink_to(parquet.parent, target_is_directory=True)
    else:
        os.mkfifo(path)
    if declared:
        # Avoid opening a FIFO or following a link while preparing evidence.
        rewrite_manifest(
            manifest,
            lambda document: document["files"].append(
                {
                    "path": path.relative_to(manifest.parent).as_posix(),
                    "size_bytes": 0,
                    "sha256": "",
                }
            ),
        )
    with pytest.raises(SilverBundleError, match="symlink|regular"):
        verify_current_bundle(silver_root)


def test_inventory_directory_is_not_a_regular_file(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    rewrite_manifest(manifest, lambda document: document["files"][0].update(path="data"))
    with pytest.raises(SilverBundleError, match="regular"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize("kind", ["symlink", "file", "missing"])
def test_data_root_must_be_a_real_directory(silver_root: Path, kind: str) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    data = manifest.parent / "data"
    data.rename(manifest.parent / "retained")
    # Retain verified evidence outside data while replacing its root.
    rewrite_manifest(
        manifest,
        lambda document: [
            entry.update(path=entry["path"].replace("data/", "retained/", 1))
            for entry in document["files"]
        ],
    )
    if kind == "symlink":
        data.symlink_to("retained", target_is_directory=True)
    elif kind == "file":
        data.write_bytes(b"not a directory")
    with pytest.raises(SilverBundleError, match="symlink|regular|missing"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize("declared", [False, True])
@pytest.mark.parametrize(
    "name", ["_metadata", "_common_metadata", ".arbitrary", "README", ".orphan.parquet.crc", ".crc"]
)
def test_unsupported_auxiliary_is_rejected(silver_root: Path, name: str, declared: bool) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    artifact = manifest.parent / "data" / name
    artifact.write_bytes(b"auxiliary")
    if declared:
        declare(manifest, artifact)
    with pytest.raises(SilverBundleError, match="unsupported data artifact"):
        verify_current_bundle(silver_root)


def test_success_marker_requires_inventory_and_integrity(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    marker = manifest.parent / "data/_SUCCESS"
    marker.touch()
    with pytest.raises(SilverBundleError, match="undeclared auxiliary"):
        verify_current_bundle(silver_root)
    declare(manifest, marker)
    assert verify_current_bundle(silver_root).row_count == 1
    marker.write_bytes(b"changed")
    with pytest.raises(SilverBundleError, match="size differs"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize("declared", [False, True])
def test_valid_checksum_is_optional_evidence(silver_root: Path, declared: bool) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    parquet = next((manifest.parent / "data").rglob("*.parquet"))
    checksum = parquet.with_name(f".{parquet.name}.crc")
    checksum.write_bytes(b"not used as a manifest hash")
    if declared:
        declare(manifest, checksum)
    assert verify_current_bundle(silver_root).row_count == 1
    checksum.write_bytes(b"modified")
    if declared:
        with pytest.raises(SilverBundleError, match="size differs"):
            verify_current_bundle(silver_root)
    else:
        assert verify_current_bundle(silver_root).row_count == 1


def test_quality_evidence_remains_hash_verified(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    quality = manifest.parent / "quality/results.json"
    quality.parent.mkdir()
    quality.write_text("[]")
    declare(manifest, quality)
    quality.write_text("{}")
    with pytest.raises(SilverBundleError, match="hash differs"):
        verify_current_bundle(silver_root)


def test_inventory_without_contract_data_is_rejected(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    for path in (manifest.parent / "data").rglob("*.parquet"):
        path.unlink()
    rewrite_manifest(manifest, lambda document: document.update(files=[]))
    marker = manifest.parent / "data/_SUCCESS"
    marker.touch()
    declare(manifest, marker)
    with pytest.raises(SilverBundleError, match="no Parquet"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize("files", [None, [], ["bad entry"]])
def test_inventory_structure_is_required(silver_root: Path, files: object) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    rewrite_manifest(manifest, lambda document: document.update(files=files))
    with pytest.raises(SilverBundleError, match="inventory|object"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize("size", [True, -1, None, "12"])
def test_inventory_size_must_be_nonnegative_integer(silver_root: Path, size: object) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    rewrite_manifest(manifest, lambda document: document["files"][0].update(size_bytes=size))
    with pytest.raises(SilverBundleError, match="non-negative integer"):
        verify_current_bundle(silver_root)


def test_traversal_errors_are_diagnostic(
    silver_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.iterdir

    def denied(path: Path):
        if path.name.startswith("signing_year="):
            raise PermissionError("test traversal denied")
        return original(path)

    monkeypatch.setattr(Path, "iterdir", denied)
    with pytest.raises(SilverBundleError, match="Could not verify Silver inventory"):
        verify_current_bundle(silver_root)


def test_root_alias_is_supported(silver_root: Path) -> None:
    alias = silver_root.parent / "root-alias"
    alias.symlink_to(silver_root, target_is_directory=True)
    assert verify_current_bundle(alias).row_count == 1


def test_nondata_alias_is_also_rejected(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    parquet = next((manifest.parent / "data").rglob("*.parquet"))
    evidence = manifest.parent / "quality.json"
    evidence.symlink_to(parquet)
    declare(manifest, evidence)
    with pytest.raises(SilverBundleError, match="symlink"):
        verify_current_bundle(silver_root)


def test_directory_inventory_alias_is_not_normalized_away(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    entry = json.loads(manifest.read_text())["files"][0]
    alias = entry["path"].replace("data/", "data/./")
    rewrite_manifest(manifest, lambda document: document["files"][0].update(path=alias))
    with pytest.raises(SilverBundleError, match="canonical"):
        verify_current_bundle(silver_root)
