"""Bounded, retrying Socrata transport that preserves response bytes."""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from observatorio_secop.ingestion.config import IngestionConfig
from observatorio_secop.ingestion.contract import BronzeSourceContract
from observatorio_secop.ingestion.errors import PageValidationError, TransportError

TRANSIENT_STATUS = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class TransportResponse:
    raw: bytes
    payload: Any
    status: int
    duration_ms: int
    retries: int
    headers: dict[str, str]


class SocrataTransport:
    def __init__(
        self,
        config: IngestionConfig,
        contract: BronzeSourceContract,
        *,
        token: str | None = None,
        opener: Any = urlopen,
        sleeper: Any = time.sleep,
        monotonic: Any = time.monotonic,
        jitter: Any = random.uniform,
    ) -> None:
        self.config = config
        self.contract = contract
        self.token = token or None
        self._open = opener
        self._sleep = sleeper
        self._monotonic = monotonic
        self._jitter = jitter

    def fetch_metadata(self) -> TransportResponse:
        return self._request(self.contract.metadata_url)

    def query(
        self,
        params: dict[str, str | int],
        *,
        on_retry: Callable[[dict[str, int | float | None]], None] | None = None,
    ) -> TransportResponse:
        limit = params.get("$limit")
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise PageValidationError("Every query must contain an integer $limit")
        if limit <= 0 or limit > self.config.max_page_size:
            raise PageValidationError("Query $limit is outside the configured bound")
        url = f"{self.contract.resource_url}?{urlencode(params)}"
        response = self._request(url, on_retry=on_retry)
        if not isinstance(response.payload, list) or not all(
            isinstance(row, dict) for row in response.payload
        ):
            raise PageValidationError("Socrata data response must be a JSON array of objects")
        if len(response.payload) > limit:
            raise PageValidationError("Socrata returned more rows than requested")
        return response

    def _request(
        self,
        url: str,
        *,
        on_retry: Callable[[dict[str, int | float | None]], None] | None = None,
    ) -> TransportResponse:
        attempts = self.config.max_retries + 1
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            started = self._monotonic()
            try:
                headers = {"Accept": "application/json"}
                if self.token:
                    headers["X-App-Token"] = self.token
                request = Request(url, headers=headers)
                with self._open(request, timeout=self.config.timeout_seconds) as response:
                    declared = response.headers.get("Content-Length")
                    if declared and int(declared) > self.config.max_response_bytes:
                        raise PageValidationError("Response exceeds max_response_bytes")
                    raw = response.read(self.config.max_response_bytes + 1)
                    if len(raw) > self.config.max_response_bytes:
                        raise PageValidationError("Response exceeds max_response_bytes")
                    try:
                        payload = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError) as error:
                        raise PageValidationError("Socrata response is not valid JSON") from error
                    return TransportResponse(
                        raw=raw,
                        payload=payload,
                        status=getattr(response, "status", 200),
                        duration_ms=round((self._monotonic() - started) * 1000),
                        retries=attempt - 1,
                        headers=_safe_headers(response.headers),
                    )
            except PageValidationError:
                raise
            except HTTPError as error:
                last_error = error
                if error.code not in TRANSIENT_STATUS:
                    raise TransportError(
                        f"Socrata request failed permanently with HTTP {error.code}",
                        status=error.code,
                    ) from error
                retry_after = _retry_after(error.headers.get("Retry-After"))
            except (TimeoutError, URLError) as error:
                last_error = error
                retry_after = None
            if attempt == attempts:
                break
            delay = min(
                self.config.max_retry_after_seconds,
                max(
                    retry_after or 0.0,
                    self.config.backoff_base_seconds * (2 ** (attempt - 1))
                    + self._jitter(0.0, self.config.jitter_seconds),
                ),
            )
            if on_retry:
                on_retry(
                    {
                        "attempt": attempt,
                        "status": last_error.code if isinstance(last_error, HTTPError) else None,
                        "duration_ms": round((self._monotonic() - started) * 1000),
                        "delay_seconds": delay,
                    }
                )
            self._sleep(delay)
        status = last_error.code if isinstance(last_error, HTTPError) else None
        kind = f"HTTP {status}" if status else "network or timeout"
        raise TransportError(
            f"Socrata request exhausted {attempts} attempts ({kind})", status=status
        ) from last_error


def _retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _safe_headers(headers: Any) -> dict[str, str]:
    allowed = {"content-length", "content-type", "retry-after", "x-soda2-fields"}
    return {key.casefold(): value for key, value in headers.items() if key.casefold() in allowed}
