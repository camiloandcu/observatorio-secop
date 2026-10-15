from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from observatorio_secop.warehouse.bundle import EXPECTED_SCHEMA, verify_current_bundle
from observatorio_secop.warehouse.cli import main
from observatorio_secop.warehouse.errors import SilverBundleError

from .conftest import rewrite_manifest, silver_row, write_bundle

TEMPORAL_COLUMNS = ("source_updated_at", "bronze_fetched_at_utc")
VALUE = datetime(2026, 1, 2, 12, 34, 56, 123456)


def temporal_bundle(
    root: Path, field: str, dtype: pa.DataType, value: object, *, int96: bool = False
) -> Path:
    schema = pa.schema(
        [pa.field(f.name, dtype, f.nullable) if f.name == field else f for f in EXPECTED_SCHEMA]
    )
    row = silver_row("version-1")
    row[field] = value
    return write_bundle(
        root,
        "version-1",
        [row],
        schema=schema,
        parquet_options={"use_deprecated_int96_timestamps": int96},
    )


def fingerprint(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("field", TEMPORAL_COLUMNS)
@pytest.mark.parametrize("unit,zone", [("us", None), ("ns", None), ("us", "UTC"), ("ns", "UTC")])
def test_supported_timestamp_encodings_preserve_values_and_evidence(
    tmp_path: Path, field: str, unit: str, zone: str | None
) -> None:
    temporal_bundle(tmp_path, field, pa.timestamp(unit, zone), VALUE)
    before = fingerprint(tmp_path)
    bundle = verify_current_bundle(tmp_path)
    assert bundle.table[field].type == pa.timestamp("us")
    assert bundle.table[field].to_pylist() == [VALUE]
    assert fingerprint(tmp_path) == before


def test_legacy_int96_and_null_source_are_accepted_without_rewriting(tmp_path: Path) -> None:
    temporal_bundle(tmp_path, "source_updated_at", pa.timestamp("us"), None, int96=True)
    before = fingerprint(tmp_path)
    assert verify_current_bundle(tmp_path).table["source_updated_at"].to_pylist() == [None]
    assert fingerprint(tmp_path) == before


@pytest.mark.parametrize("field", TEMPORAL_COLUMNS)
@pytest.mark.parametrize(
    "dtype,value,int96",
    [
        (pa.timestamp("ns"), pa.scalar(1767357296123456001, type=pa.timestamp("ns")), False),
        (pa.timestamp("ns"), pa.scalar(1767357296123456001, type=pa.timestamp("ns")), True),
        (pa.timestamp("us", "America/Bogota"), VALUE, False),
        (pa.timestamp("ms"), VALUE, False),
        (pa.string(), "2026-01-02", False),
        (pa.timestamp("us"), datetime(1500, 1, 1), True),
    ],
)
def test_incompatible_timestamps_fail_before_database_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    dtype: pa.DataType,
    value: object,
    int96: bool,
) -> None:
    temporal_bundle(tmp_path, field, dtype, value, int96=int96)
    before = fingerprint(tmp_path)
    with pytest.raises(SilverBundleError):
        verify_current_bundle(tmp_path)
    connect = Mock(side_effect=AssertionError("must not connect"))
    monkeypatch.setattr("psycopg.connect", connect)
    for name, value in {
        "SILVER_ROOT": str(tmp_path),
        "POSTGRES_DB": "test",
        "POSTGRES_USER": "test",
        "POSTGRES_PASSWORD": "test-only",
    }.items():
        monkeypatch.setenv(name, value)
    assert main(["load"]) == 2
    connect.assert_not_called()
    assert fingerprint(tmp_path) == before


def test_legacy_int96_values_are_exact(tmp_path: Path) -> None:
    temporal_bundle(tmp_path, "source_updated_at", pa.timestamp("us"), VALUE, int96=True)
    assert verify_current_bundle(tmp_path).table["source_updated_at"].to_pylist() == [VALUE]


def test_standard_microseconds_do_not_inherit_the_int96_range_limit(tmp_path: Path) -> None:
    value = datetime(1500, 1, 1)
    temporal_bundle(tmp_path, "source_updated_at", pa.timestamp("us"), value)
    assert verify_current_bundle(tmp_path).table["source_updated_at"].to_pylist() == [value]


@pytest.mark.parametrize("submicroseconds", [0, 1])
def test_each_fragment_is_validated_before_dataset_coercion(
    tmp_path: Path, submicroseconds: int
) -> None:
    manifest = temporal_bundle(tmp_path, "source_updated_at", pa.timestamp("us"), VALUE)
    first = next((manifest.parent / "data").rglob("*.parquet"))
    # Put an incompatible later fragment beside the canonical first fragment.
    first.rename(first.with_name("a.parquet"))
    changed = EXPECTED_SCHEMA.set(
        EXPECTED_SCHEMA.get_field_index("source_updated_at"),
        pa.field("source_updated_at", pa.timestamp("ns")),
    )
    row = silver_row("version-1", "contract-2", contract_id="CO1.PCCNTR.2")
    row["source_updated_at"] = pa.scalar(
        1767357296123456000 + submicroseconds, type=pa.timestamp("ns")
    )
    table = pa.Table.from_pylist([row], schema=changed).drop(["signing_year"])
    pq.write_table(table, first.with_name("z.parquet"))
    files = [
        {
            "path": str(p.relative_to(manifest.parent)),
            "size_bytes": p.stat().st_size,
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
        }
        for p in (manifest.parent / "data").rglob("*.parquet")
    ]
    rewrite_manifest(manifest, lambda value: value.update(files=files, counts={"output_rows": 2}))
    if submicroseconds:
        with pytest.raises(SilverBundleError):
            verify_current_bundle(tmp_path)
    else:
        assert verify_current_bundle(tmp_path).row_count == 2
