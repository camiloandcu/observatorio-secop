"""Precision, semantic time and negative exposure boundaries for portable data."""

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

from observatorio_secop.cloud_export.codecs import canonical_cell, encode
from observatorio_secop.cloud_export.contract import Column, load_contract
from observatorio_secop.cloud_export.errors import ExportError


@pytest.fixture
def contract():
    return load_contract()


def test_static_contract_and_arrow_mapping(contract):
    assert len(contract.relations) == 13
    assert sum(len(r.columns) for r in contract.relations.values()) == 261
    assert contract.relations["fact_contract"].schema.field(
        "contract_value_cop"
    ).type == pa.decimal128(20, 2)
    for relation in contract.relations.values():
        assert len(set(relation.schema.names)) == len(relation.columns)
        assert all(c.glue_type for c in relation.columns)


def test_exact_native_and_unbounded_decimal_roundtrip(contract):
    native = Column("money", "numeric", "decimal", 20, 2)
    large = Decimal("90071992547409.93")
    assert encode(large, native, contract) == large
    c = Column("hhi", "numeric", "decimal-text-v1")
    text = "0.3324860644359977275534139090023001690332"
    assert encode(text, c, contract) == text
    assert Decimal(encode(text, c, contract)) == Decimal(text)
    for value in [1.01, "NaN", "Infinity", "1e2", "1.001", "1000000000000000000.00"]:
        with pytest.raises(ExportError):
            encode(value, native, contract)


def test_known_utc_is_independent_of_physical_sql_type(contract):
    field = next(
        c for c in contract.relations["fact_contract"].columns if c.name == "bronze_fetched_at_utc"
    )
    wall = Column("source_updated_at", "timestamp", "wall-clock-us-v1")
    stamp = datetime(2026, 1, 2, 12, 3, 4, 123456)
    assert encode(stamp, field, contract) == "2026-01-02T12:03:04.123456Z"
    assert encode(stamp, wall, contract) == "2026-01-02T12:03:04.123456"
    equivalent = stamp.replace(tzinfo=UTC).astimezone(timezone(timedelta(hours=-5)))
    assert encode(equivalent, field, contract) == encode(stamp, field, contract)
    with pytest.raises(ExportError):
        encode(equivalent, wall, contract)
    assert encode(date(2026, 1, 2), Column("day", "date", "date"), contract) == date(2026, 1, 2)


def test_nested_json_nulls_and_numeric_text_are_not_floats(contract):
    c = Column(
        "observation_condition", "jsonb", "json-text-v1", json_policy="observation_condition"
    )
    raw = '{"later_invalid_count":9007199254740993,"state":null}'
    assert encode(raw, c, contract) == raw
    assert encode("null", c, contract) == "null"
    assert encode(None, c, contract) is None
    assert canonical_cell(None, c) == b"N;"
    assert canonical_cell("null", c) == b"V4:null;"


@pytest.mark.parametrize(
    "raw",
    [
        '{"raw_payload":{"secret":"x"}}',
        '{"state":{"credential":"x"}}',
        '{"state":"/home/person/private"}',
        '{"state":"https://example.com/key"}',
        '{"state":"person@example.com"}',
        '{"state":"C:\\\\private\\\\secret"}',
        '"unexpected scalar"',
        "[1,2]",
        '{"state":NaN}',
    ],
)
def test_nested_payload_and_shape_rejected(contract, raw):
    c = Column(
        "observation_condition", "jsonb", "json-text-v1", json_policy="observation_condition"
    )
    with pytest.raises(ExportError):
        encode(raw, c, contract)


def test_recursive_coverage_is_closed_at_each_level(contract):
    c = Column("coverage_metadata", "jsonb", "json-text-v1", json_policy="coverage_metadata")
    valid = '{"acquisitions":[{"limits":{"rows":100},"baseline":{"keys_sha256":"abc"}}]}'
    assert encode(valid, c, contract) == valid
    with pytest.raises(ExportError):
        encode('{"acquisitions":[{"limits":{"password":"secret"}}]}', c, contract)
    with pytest.raises(ExportError):
        encode('{"acquisitions":{"state":"wrong shape"}}', c, contract)


def test_array_domain_and_empty_null_distinction(contract):
    c = Column("quality_warning_codes", "text[]", "array-json-v1")
    assert encode(["KNOWN", None], c, contract) == '["KNOWN",null]'
    assert encode([], c, contract) == "[]"
    assert encode(None, c, contract) is None
    for value in ['[["A"]]', "[1]", '{"key":"A"}', '["/tmp/private"]']:
        with pytest.raises(ExportError):
            encode(value, c, contract)


def test_framing_uses_utf8_bytes_and_separates_delimiters(contract):
    c = Column("label", "text", "text")
    assert canonical_cell("ñ", c) == b"V2:\xc3\xb1;"
    assert canonical_cell("", c) == b"V0:;"
    assert canonical_cell("a;V1:b", c) != canonical_cell("a", c) + canonical_cell("b", c)
    for value in [True, 1.0, "1"]:
        with pytest.raises(ExportError):
            encode(value, Column("n", "bigint", "int64"), contract)


def test_real_process_link_is_allowed_without_matching_url_scheme_as_drive(contract):
    value = (
        "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index"
        "?noticeUID=CO1.NTC.123&isFromPublicArea=True&isModal=false&asPopupView=true"
    )
    column = Column("source_url", "text", "text")
    assert encode(value, column, contract) == value
    for restricted in [
        "C:/private/secret",
        "/home/person/file",
        value + "&password=secret",
        value.replace("community.secop.gov.co", "community.secop.gov.co.evil.invalid"),
    ]:
        with pytest.raises(ExportError):
            encode(restricted, column, contract)


def test_duplicate_nested_json_keys_cannot_hide_restricted_values(contract):
    column = Column(
        "observation_condition", "jsonb", "json-text-v1", json_policy="observation_condition"
    )
    with pytest.raises(ExportError):
        encode('{"state":"/home/private","state":"VALID"}', column, contract)


def test_private_text_fast_path_preserves_legacy_rejections():
    import random
    import re

    from observatorio_secop.cloud_export.codecs import PRIVATE, contains_private_text

    original = re.compile(PRIVATE.pattern.replace(r"\w@", r"\w+@"))
    samples = [
        "a" * 64,
        "b" * 1000,
        "contact@example.test",
        "a@b.c",
        "ñ@á.bc",
        "C:/private",
        "/home/person/private",
        r"\\host\share",
        "approved company",
        "x@y",
        "CO1.PCCNTR.1",
    ]
    rng = random.Random(20261114)
    alphabet = "abcÁÑ012@:/\\. _-"
    samples += [
        "".join(rng.choice(alphabet) for _ in range(rng.randrange(1, 100))) for _ in range(500)
    ]
    assert all(contains_private_text(text) == bool(original.search(text)) for text in samples)
