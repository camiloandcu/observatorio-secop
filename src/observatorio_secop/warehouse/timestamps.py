"""Lossless normalization of the two supported Silver timestamp representations."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from observatorio_secop.warehouse.errors import SilverBundleError

TIMESTAMP_COLUMNS = ("source_updated_at", "bronze_fetched_at_utc")
CANONICAL_TIMESTAMP = pa.timestamp("us")


def normalize_timestamps(table: pa.Table, int96_reference: pa.Table | None = None) -> pa.Table:
    for name in TIMESTAMP_COLUMNS:
        if name not in table.column_names:
            continue  # The complete schema check reports missing columns.
        column = table.column(name)
        dtype = column.type
        if (
            not pa.types.is_timestamp(dtype)
            or dtype.unit not in {"us", "ns"}
            or dtype.tz not in {None, "UTC"}
        ):
            raise SilverBundleError(
                f"Unsupported Silver timestamp representation: {name} ({dtype})"
            )
        try:
            normalized = pc.cast(column, CANONICAL_TIMESTAMP, safe=True)
            if int96_reference is not None and name in int96_reference.column_names:
                reference = pc.cast(int96_reference.column(name), CANONICAL_TIMESTAMP, safe=True)
                # INT96 decoded directly as nanoseconds can wrap before a safe downcast.
                roundtrip = pc.cast(reference, pa.timestamp("ns"), safe=True)
                original = pc.cast(column, pa.timestamp("ns"), safe=True)
                if not roundtrip.equals(original) or not reference.equals(normalized):
                    raise SilverBundleError(f"Silver INT96 timestamp does not round-trip: {name}")
        except pa.ArrowException as error:
            raise SilverBundleError(
                f"Silver timestamp cannot be represented exactly in the supported range: {name}"
            ) from error
        index = table.schema.get_field_index(name)
        table = table.set_column(
            index, table.schema.field(index).with_type(CANONICAL_TIMESTAMP), normalized
        )
    return table
