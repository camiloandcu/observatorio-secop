from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from observatorio_secop.source_json import source_json_loads
from observatorio_secop.source_profile.client import SocrataClient
from observatorio_secop.source_profile.config import load_config
from observatorio_secop.source_profile.errors import InvalidResponseError, QueryLimitError
from observatorio_secop.source_profile.identity import summarize_identity

ROOT = Path(__file__).resolve().parents[2]


def test_identity_preserves_numeric_lexemes_and_never_exposes_source_values() -> None:
    raw = b"""[{"id_contrato":"synthetic-1","codigo_entidad":12345678901234567890,
        "codigo_proveedor":"00023","nombre_entidad":"PRIVATE-LABEL",
        "valor_del_contrato":"2.00"},
        {"id_contrato":"synthetic-2","codigo_entidad":1.00,
        "codigo_proveedor":"0","valor_del_contrato":"3.00"}]"""
    rows = source_json_loads(raw)
    assert rows[0]["codigo_entidad"] == "12345678901234567890"
    assert rows[1]["codigo_entidad"] == "1.00"
    summary = summarize_identity(rows)
    assert summary["roles"]["entity"]["code_states"]["OTHER_SHAPE"] == 1
    assert summary["roles"]["supplier"]["code_states"]["PLACEHOLDER"] == 1
    assert summary["value_denominator_cop"] == "5.00"
    assert "PRIVATE-LABEL" not in json.dumps(summary)
    assert "12345678901234567890" not in json.dumps(summary)


def test_identity_records_label_variants_and_unknown_value_denominator() -> None:
    summary = summarize_identity(
        [
            {
                "id_contrato": "a",
                "codigo_entidad": "001",
                "nombre_entidad": "One",
                "valor_del_contrato": "10.00",
            },
            {
                "id_contrato": "b",
                "codigo_entidad": "001",
                "nombre_entidad": "Renamed",
                "valor_del_contrato": "not an amount",
            },
            {"id_contrato": "b", "codigo_entidad": "002", "nombre_entidad": "One"},
        ]
    )
    entity = summary["roles"]["entity"]
    assert entity["codes_with_multiple_labels"] == 1
    assert entity["labels_with_multiple_codes"] == 1
    assert summary["value_denominator_cop"] is None
    assert summary["repeated_contract_observations"] == 1
    assert summary["roles"]["supplier"]["code_states"] == {"ABSENT": 3}


def test_identity_rejects_unapproved_fields_and_empty_evidence() -> None:
    for rows in ([], [{"documento_proveedor": "do-not-publish"}]):
        with pytest.raises(InvalidResponseError):
            summarize_identity(rows)


def test_retries_consume_total_http_attempt_budget(monkeypatch) -> None:
    config = load_config(ROOT / "config/secop_source.yaml")
    config = replace(config, limits=replace(config.limits, max_requests=2, retries=5))
    calls = []

    def timeout(*args, **kwargs):
        calls.append(1)
        raise TimeoutError

    monkeypatch.setattr("observatorio_secop.source_profile.client.urlopen", timeout)
    client = SocrataClient(config, sleeper=lambda _: None)
    with pytest.raises(QueryLimitError, match="budget"):
        client.query(**{"$limit": 1})
    assert len(calls) == client.budget.requests == 2
