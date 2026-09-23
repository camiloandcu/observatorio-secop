"""Bounded client for Socrata metadata and SoQL queries."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from observatorio_secop.source_profile.config import SourceConfig
from observatorio_secop.source_profile.errors import (
    InvalidResponseError,
    QueryLimitError,
    SourceUnavailableError,
)

LOGGER = logging.getLogger(__name__)
TRANSIENT_STATUS = {429, 500, 502, 503, 504}


@dataclass
class RequestBudget:
    max_requests: int
    max_response_bytes: int
    requests: int = 0

    def reserve(self) -> None:
        if self.requests >= self.max_requests:
            raise QueryLimitError(f"Request budget exceeded ({self.max_requests} requests)")
        self.requests += 1


class SocrataClient:
    """Read only bounded responses; never logs headers, tokens, or payloads."""

    def __init__(self, config: SourceConfig, *, sleeper: Any = time.sleep) -> None:
        self.config = config
        self.budget = RequestBudget(
            max_requests=config.limits.max_requests,
            max_response_bytes=config.limits.max_response_bytes,
        )
        self._sleep = sleeper

    @property
    def metadata_url(self) -> str:
        return f"{self.config.api_base_url}/api/views/{self.config.dataset_id}"

    @property
    def resource_url(self) -> str:
        return f"{self.config.api_base_url}/resource/{self.config.dataset_id}.json"

    def fetch_metadata(self) -> dict[str, Any]:
        payload = self._request_json(self.metadata_url)
        if not isinstance(payload, dict) or not isinstance(payload.get("columns"), list):
            raise InvalidResponseError("Socrata metadata is missing the columns list")
        if payload.get("id") != self.config.dataset_id:
            raise InvalidResponseError("Socrata metadata identifies a different dataset")
        return payload

    def query(self, *, allow_empty: bool = False, **params: str | int) -> list[dict[str, Any]]:
        raw_limit = params.get("$limit")
        if isinstance(raw_limit, bool) or not isinstance(raw_limit, int):
            raise QueryLimitError("Every data query must provide an integer $limit")
        if raw_limit <= 0 or raw_limit > self.config.limits.max_query_rows:
            raise QueryLimitError(
                f"$limit must be between 1 and {self.config.limits.max_query_rows}"
            )
        url = f"{self.resource_url}?{urlencode(params)}"
        payload = self._request_json(url)
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise InvalidResponseError("Socrata query did not return a JSON array of objects")
        if not payload and not allow_empty:
            raise InvalidResponseError("Socrata query returned no rows; evidence is insufficient")
        if len(payload) > raw_limit:
            raise InvalidResponseError("Socrata returned more rows than the requested limit")
        return payload

    def _request_json(self, url: str) -> Any:
        self.budget.reserve()
        attempts = self.config.limits.retries + 1
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            started = time.monotonic()
            try:
                request = Request(url, headers={"Accept": "application/json"})
                with urlopen(request, timeout=self.config.limits.timeout_seconds) as response:
                    declared_size = response.headers.get("Content-Length")
                    if declared_size and int(declared_size) > self.budget.max_response_bytes:
                        raise QueryLimitError(
                            f"Response exceeds {self.budget.max_response_bytes} bytes"
                        )
                    body = response.read(self.budget.max_response_bytes + 1)
                    if len(body) > self.budget.max_response_bytes:
                        raise QueryLimitError(
                            f"Response exceeds {self.budget.max_response_bytes} bytes"
                        )
                LOGGER.info(
                    "Socrata request completed endpoint=%s status=200 duration_ms=%d attempt=%d",
                    self._logical_endpoint(url),
                    (time.monotonic() - started) * 1000,
                    attempt,
                )
                try:
                    return json.loads(body)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise InvalidResponseError("Socrata response is not valid JSON") from error
            except HTTPError as error:
                last_error = error
                if error.code not in TRANSIENT_STATUS or attempt == attempts:
                    break
            except (TimeoutError, URLError) as error:
                last_error = error
                if attempt == attempts:
                    break
            if attempt < attempts:
                self._sleep(min(2 ** (attempt - 1), 2))

        detail = f"HTTP {last_error.code}" if isinstance(last_error, HTTPError) else "network error"
        raise SourceUnavailableError(
            f"Socrata source unavailable after {attempts} attempts ({detail})"
        ) from last_error

    def _logical_endpoint(self, url: str) -> str:
        return "metadata" if "/api/views/" in url else f"resource/{self.config.dataset_id}"
