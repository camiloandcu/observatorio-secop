from __future__ import annotations

import io
from email.message import Message
from urllib.error import HTTPError, URLError

import pytest

from observatorio_secop.ingestion.errors import PageValidationError, TransportError
from observatorio_secop.ingestion.transport import SocrataTransport


class Response:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.body = body
        self.status = 200
        self.headers = Message()
        for key, value in (headers or {}).items():
            self.headers[key] = value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self, size: int) -> bytes:
        return self.body[:size]


def test_success_preserves_bytes_and_optional_token(config, contract) -> None:
    raw = b'[{"id_contrato":"A"}]\n'
    requests = []

    def opener(request, timeout):
        requests.append((request, timeout))
        return Response(raw, {"Content-Type": "application/json", "Secret": "no"})

    result = SocrataTransport(config, contract, token="sentinel", opener=opener).query(
        {"$limit": 1, "$select": "id_contrato"}
    )
    assert result.raw == raw
    assert requests[0][0].get_header("X-app-token") == "sentinel"
    assert "sentinel" not in requests[0][0].full_url
    assert result.headers == {"content-type": "application/json"}


@pytest.mark.parametrize("body", [b"not-json", b"{}", b"[1]"])
def test_invalid_data_response_is_rejected(config, contract, body: bytes) -> None:
    transport = SocrataTransport(config, contract, opener=lambda *_args, **_kwargs: Response(body))
    with pytest.raises(PageValidationError):
        transport.query({"$limit": 1})


def test_empty_response_is_valid(config, contract) -> None:
    transport = SocrataTransport(config, contract, opener=lambda *_args, **_kwargs: Response(b"[]"))
    assert transport.query({"$limit": 1}).payload == []


def test_429_honors_retry_after_and_recovers(config, contract) -> None:
    outcomes = [
        HTTPError("safe", 429, "rate", {"Retry-After": "3"}, io.BytesIO()),
        Response(b"[]"),
    ]
    delays = []
    events = []

    def opener(*_args, **_kwargs):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    response = SocrataTransport(
        config, contract, opener=opener, sleeper=delays.append, jitter=lambda *_: 0
    ).query({"$limit": 1}, on_retry=events.append)
    assert response.retries == 1
    assert delays == [3.0]
    assert events == [{"attempt": 1, "status": 429, "duration_ms": 0, "delay_seconds": 3.0}]


@pytest.mark.parametrize(
    "error",
    [HTTPError("safe", 503, "down", {}, io.BytesIO()), TimeoutError(), URLError("down")],
)
def test_transient_errors_exhaust_finite_retries(config, contract, error: Exception) -> None:
    calls = []

    def opener(*_args, **_kwargs):
        calls.append(1)
        raise error

    with pytest.raises(TransportError, match="exhausted"):
        SocrataTransport(
            config, contract, opener=opener, sleeper=lambda _: None, jitter=lambda *_: 0
        ).query({"$limit": 1})
    assert len(calls) == config.max_retries + 1


def test_permanent_4xx_is_not_retried(config, contract) -> None:
    calls = []

    def opener(*_args, **_kwargs):
        calls.append(1)
        raise HTTPError("safe", 400, "bad", {}, io.BytesIO())

    with pytest.raises(TransportError, match="permanently"):
        SocrataTransport(config, contract, opener=opener).query({"$limit": 1})
    assert len(calls) == 1


def test_query_is_bounded_and_never_uses_export_endpoint(config, contract) -> None:
    urls = []

    def opener(request, timeout):
        urls.append(request.full_url)
        return Response(b"[]")

    transport = SocrataTransport(config, contract, opener=opener)
    with pytest.raises(PageValidationError):
        transport.query({"$limit": config.max_page_size + 1})
    transport.query({"$limit": 1})
    assert "/resource/" in urls[0]
    assert "$limit=1" in urls[0] or "%24limit=1" in urls[0]
    assert "/export" not in urls[0]


def test_oversized_response_is_rejected(config, contract) -> None:
    response = Response(b"[]", {"Content-Length": str(config.max_response_bytes + 1)})
    transport = SocrataTransport(config, contract, opener=lambda *_args, **_kwargs: response)
    with pytest.raises(PageValidationError, match="exceeds"):
        transport.query({"$limit": 1})
