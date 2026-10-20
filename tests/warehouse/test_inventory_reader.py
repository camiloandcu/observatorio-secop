from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.cli import main
from observatorio_secop.warehouse.errors import SilverBundleError

from .conftest import rewrite_manifest, silver_row, write_bundle
from .inventory_helpers import add_extra, cli_environment, declare, fingerprint, inventory_entry


def test_count_coherent_undeclared_parquet_is_rejected(silver_root: Path) -> None:
    manifest = silver_root / "versions/version-1/manifest.json"
    add_extra(manifest)
    with pytest.raises(SilverBundleError, match="undeclared data"):
        verify_current_bundle(silver_root)


@pytest.mark.parametrize(
    "relative",
    [
        "data/signing_year=2026/extra.parquet",
        "data/signing_year=2025/extra.parquet",
        "data/signing_year=2026/nested/extra.parquet",
        "data/signing_year=2026/.hidden.parquet",
        "data/signing_year=2026/_hidden.parquet",
        "data/.hidden/signing_year=2026/extra.parquet",
        "data/_temporary/signing_year=2026/extra.parquet",
    ],
)
def test_extra_file_fails_cli_before_decode_or_connection(
    silver_root: Path, monkeypatch: pytest.MonkeyPatch, capsys, relative: str
) -> None:
    add_extra(silver_root / "versions/version-1/manifest.json", relative)
    cli_environment(silver_root, monkeypatch)
    connect = Mock(side_effect=AssertionError("must not connect"))
    decode = Mock(side_effect=AssertionError("must not decode invalid inventory"))
    monkeypatch.setattr("psycopg.connect", connect)
    monkeypatch.setattr("observatorio_secop.warehouse.bundle._read_table", decode)
    for command in ["verify", "load"]:
        assert main([command]) == 2
        assert "undeclared data" in capsys.readouterr().err
    connect.assert_not_called()
    decode.assert_not_called()


@pytest.mark.parametrize("int96", [False, True])
def test_only_explicit_verified_files_are_decoded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, int96: bool
) -> None:
    rows = []
    for year in ["2025", "2026", "unknown"]:
        row = silver_row("version-1", year, contract_id=f"contract-{year}")
        row["signing_year"] = year
        rows.append(row)
    manifest = write_bundle(
        tmp_path, "version-1", rows, parquet_options={"use_deprecated_int96_timestamps": int96}
    )
    data = manifest.parent / "data"
    paths = sorted(data.rglob("*.parquet"))
    renamed = []
    for path, name in zip(
        paths, [".hidden.parquet", "_hidden.parquet", "visible.parquet"], strict=True
    ):
        target = path.with_name(name)
        path.rename(target)
        renamed.append(target)
    rewrite_manifest(
        manifest,
        lambda document: document.update(
            files=[inventory_entry(path, manifest.parent) for path in renamed]
        ),
    )
    for directory in [data, *(path.parent for path in renamed)]:
        marker = directory / "_SUCCESS"
        marker.touch()
        declare(manifest, marker)
        (directory / "._SUCCESS.crc").write_bytes(b"checksum")
    for path in renamed:
        path.with_name(f".{path.name}.crc").write_bytes(b"checksum")
    for relative in ["rejected/part.parquet", "quality/results.json", "data-other/part.parquet"]:
        evidence = manifest.parent / relative
        evidence.parent.mkdir(exist_ok=True)
        evidence.write_bytes(b"evidence which must never be decoded as Parquet")
        declare(manifest, evidence)
    # Extras outside data cannot supply rows and are outside exact data equality.
    (manifest.parent / "rejected/extra.parquet").write_bytes(b"not data")
    before = fingerprint(tmp_path)
    dataset_calls = []
    parquet_calls = []
    original_dataset = ds.dataset
    original_parquet = pq.ParquetFile

    def dataset(source, *args, **kwargs):
        dataset_calls.append(source)
        assert isinstance(source, list)
        assert kwargs["partition_base_dir"] == str(data)
        return original_dataset(source, *args, **kwargs)

    def parquet_file(source, *args, **kwargs):
        parquet_calls.append((Path(source), kwargs))
        return original_parquet(source, *args, **kwargs)

    monkeypatch.setattr(ds, "dataset", dataset)
    monkeypatch.setattr(pq, "ParquetFile", parquet_file)
    bundle = verify_current_bundle(tmp_path)
    expected_paths = sorted(renamed)
    assert dataset_calls == [[str(path)] for path in expected_paths]
    assert [path for path, _ in parquet_calls] == [
        path for path in expected_paths for _ in range(2 if int96 else 1)
    ]
    if int96:
        assert [options for _, options in parquet_calls[1::2]] == [
            {"coerce_int96_timestamp_unit": "us"}
        ] * 3
    actual = sorted(bundle.table.to_pylist(), key=lambda row: row["contract_key"])
    assert [{key: row[key] for key in rows[0]} for row in actual] == sorted(
        rows, key=lambda row: row["contract_key"]
    )
    assert all(row["entity_identity_reason"] == "LEGACY_PROJECTION" for row in actual)
    assert all(row["supplier_identity_reason"] == "LEGACY_PROJECTION" for row in actual)
    assert fingerprint(tmp_path) == before
