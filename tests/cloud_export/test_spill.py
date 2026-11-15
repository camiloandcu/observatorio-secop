from __future__ import annotations

from datetime import datetime, timedelta

import pyarrow as pa
import pytest

from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from warehouse.conftest import silver_row, write_bundle


@pytest.mark.parametrize("int96", [False, True])
def test_shared_spill_preserves_multiple_batches_and_microseconds(tmp_path, int96):
    rows = []
    for i in range(2201):
        row = silver_row("A", f"key-{i}")
        row["source_updated_at"] = (
            None if i % 7 == 0 else datetime(2026, 1, 2) + timedelta(microseconds=i)
        )
        rows.append(row)
    path = write_bundle(
        tmp_path / "source",
        "A",
        rows,
        parquet_options={"use_deprecated_int96_timestamps": int96, "row_group_size": 800},
    )
    expected = verify_manifest_bundle(path)
    scratch = tmp_path / "scratch"
    scratch.mkdir(mode=0o700)
    observed = verify_manifest_bundle(path, spill_directory=scratch)
    assert expected.table.equals(observed.table, check_metadata=True)
    assert expected.row_count == observed.row_count == 2201
    assert all(p.stat().st_mode & 0o077 == 0 for p in scratch.iterdir())
    assert observed.table["source_updated_at"].type == pa.timestamp("us")
