from __future__ import annotations

import json
from email.message import Message
from pathlib import Path
from urllib.error import HTTPError

import pytest

from observatorio_secop.source_profile.client import SocrataClient
from observatorio_secop.source_profile.config import load_config
from observatorio_secop.source_profile.errors import (
    InvalidResponseError,
    QueryLimitError,
    SourceUnavailableError,
)

ROOT = Path(__file__).resolve().parents[2]


class FakeResponse:
    def __init__(self, payload: object, *, declared_size: int | None = None) -> None:
        self._body = json.dumps(payload).encode()
        self.headers = Message()
        if declared_size is not None:
            self.headers["Content-Length"] = str(declared_size)

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, amount: int) -> bytes:
        return self._body[:amount]


@pytest.fixture
def client() -> SocrataClient:
    return SocrataClient(load_config(ROOT / "config" / "secop_source.yaml"), sleeper=lambda _: None)


def test_fetch_metadata_and_limited_query(
    monkeypatch: pytest.MonkeyPatch, client: SocrataClient
) -> None:
    responses = iter(
        [
            FakeResponse({"id": "jbjy-vk9h", "columns": []}),
            FakeResponse([{"id_contrato": "CO1.PCCNTR.1"}]),
        ]
    )
    monkeypatch.setattr(
        "observatorio_secop.source_profile.client.urlopen",
        lambda *_args, **_kwargs: next(responses),
    )

    assert client.fetch_metadata()["id"] == "jbjy-vk9h"
    assert client.query(**{"$limit": 1}) == [{"id_contrato": "CO1.PCCNTR.1"}]
    assert client.budget.requests == 2


@pytest.mark.parametrize("limit", [None, 0, 251, True])
def test_query_rejects_missing_or_unsafe_limit(client: SocrataClient, limit: object) -> None:
    params = {} if limit is None else {"$limit": limit}
    with pytest.raises(QueryLimitError, match=r"\$limit"):
        client.query(**params)  # type: ignore[arg-type]
    assert client.budget.requests == 0


def test_empty_response_is_actionable(
    monkeypatch: pytest.MonkeyPatch, client: SocrataClient
) -> None:
    monkeypatch.setattr(
        "observatorio_secop.source_profile.client.urlopen",
        lambda *_args, **_kwargs: FakeResponse([]),
    )
    with pytest.raises(InvalidResponseError, match="no rows"):
        client.query(**{"$limit": 1})


def test_invalid_json_is_actionable(monkeypatch: pytest.MonkeyPatch, client: SocrataClient) -> None:
    response = FakeResponse({})
    response._body = b"not-json"
    monkeypatch.setattr(
        "observatorio_secop.source_profile.client.urlopen", lambda *_args, **_kwargs: response
    )
    with pytest.raises(InvalidResponseError, match="not valid JSON"):
        client.query(**{"$limit": 1})


def test_timeout_retries_then_fails(monkeypatch: pytest.MonkeyPatch, client: SocrataClient) -> None:
    calls = 0

    def timeout(*_args: object, **_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise TimeoutError

    monkeypatch.setattr("observatorio_secop.source_profile.client.urlopen", timeout)
    with pytest.raises(SourceUnavailableError, match="after 3 attempts"):
        client.query(**{"$limit": 1})
    assert calls == 3


def test_declared_oversized_response_is_rejected(
    monkeypatch: pytest.MonkeyPatch, client: SocrataClient
) -> None:
    monkeypatch.setattr(
        "observatorio_secop.source_profile.client.urlopen",
        lambda *_args, **_kwargs: FakeResponse([], declared_size=3_000_000),
    )
    with pytest.raises(QueryLimitError, match="Response exceeds"):
        client.query(**{"$limit": 1})


def test_http_error_retries_then_reports_status(
    monkeypatch: pytest.MonkeyPatch, client: SocrataClient
) -> None:
    calls = 0

    def unavailable(*_args: object, **_kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise HTTPError("https://example.invalid", 503, "unavailable", Message(), None)

    monkeypatch.setattr("observatorio_secop.source_profile.client.urlopen", unavailable)
    with pytest.raises(SourceUnavailableError, match="HTTP 503"):
        client.query(**{"$limit": 1})
    assert calls == 3


def test_query_url_is_resource_json_with_encoded_limit(
    monkeypatch: pytest.MonkeyPatch, client: SocrataClient
) -> None:
    captured: list[str] = []

    def respond(request: object, **_kwargs: object) -> FakeResponse:
        captured.append(request.full_url)  # type: ignore[attr-defined]
        return FakeResponse([{"count": "1"}])

    monkeypatch.setattr("observatorio_secop.source_profile.client.urlopen", respond)
    client.query(**{"$select": "count(*)", "$limit": 1})

    assert captured == [
        "https://www.datos.gov.co/resource/jbjy-vk9h.json?%24select=count%28%2A%29&%24limit=1"
    ]
    assert "csv" not in captured[0]
