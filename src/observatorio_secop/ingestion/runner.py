"""Orchestrate bounded two-lane Bronze ingestion."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from observatorio_secop.ingestion.config import (
    IngestionConfig,
    RunWindow,
    build_run_window,
    format_utc,
)
from observatorio_secop.ingestion.contract import BronzeSourceContract
from observatorio_secop.ingestion.errors import PageValidationError, ResumeError
from observatorio_secop.ingestion.logging import EventLogger
from observatorio_secop.ingestion.models import PageRecord, RunManifest
from observatorio_secop.ingestion.query import (
    SourceCursor,
    cursor_from_row,
    null_watermark_query,
    primary_query,
)
from observatorio_secop.ingestion.state import (
    DatasetLock,
    IngestionState,
    observations_from_pages,
)
from observatorio_secop.ingestion.storage import BronzeStorage
from observatorio_secop.ingestion.transport import SocrataTransport
from observatorio_secop.ingestion.validation import validate_metadata, validate_page

Clock = Callable[[], datetime]


class BronzeIngestion:
    def __init__(
        self,
        config: IngestionConfig,
        contract: BronzeSourceContract,
        *,
        transport: SocrataTransport | None = None,
        clock: Clock = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], Any] = uuid.uuid4,
        logger: EventLogger | None = None,
    ) -> None:
        self.config = config
        self.contract = contract
        self.clock = clock
        self.uuid_factory = uuid_factory
        token = os.environ.get("SOCRATA_APP_TOKEN")
        self.transport = transport or SocrataTransport(config, contract, token=token)
        self.logger = logger or EventLogger(secrets=(token,) if token else ())
        self.storage = BronzeStorage(config.bronze_root)
        self.state = IngestionState(self.storage.state_root / "ingestion.sqlite")

    def run(
        self,
        requested_from: str,
        requested_to: str,
        *,
        resume: str | None = None,
    ) -> RunManifest:
        lock_path = self.storage.locks_root / f"{self.contract.dataset_id}.lock"
        with DatasetLock(lock_path):
            self.reconcile_prepared()
            checkpoint = self.state.checkpoint(self.contract.dataset_id)
            window = build_run_window(
                requested_from,
                requested_to,
                checkpoint_completed_through=checkpoint,
                overlap_days=self.config.overlap_days,
            )
            parameters = self._parameters(window)
            manifest = (
                self._resume(resume, parameters)
                if resume
                else self._new_manifest(parameters, checkpoint)
            )
            try:
                metadata = self.transport.fetch_metadata()
                if not isinstance(metadata.payload, dict):
                    raise PageValidationError("Socrata metadata response must be a JSON object")
                manifest.additive_drift = validate_metadata(metadata.payload, self.contract)
                self._extract(manifest, window)
                return self._finish(manifest, window)
            except Exception as error:
                manifest.status = "failed_recoverable"
                manifest.finished_at_utc = self._now()
                if self.storage.staging_path(manifest.run_id).exists():
                    self.storage.write_page_state(manifest.run_id, manifest)
                self.logger.emit(
                    "run_failed",
                    run_id=manifest.run_id,
                    error_type=type(error).__name__,
                    message=str(error),
                )
                raise

    def reconcile_prepared(self) -> list[str]:
        reconciled = []
        for path in self.storage.prepared_runs():
            manifest = self.storage.load_published(path)
            if self.state.has_run(manifest.run_id):
                self.storage.mark_committed(manifest)
                reconciled.append(manifest.run_id)
                continue
            observations = observations_from_pages(path, manifest, self.contract.identity_field)
            counts = self.state.commit(
                manifest, observations, manifest_path=str(path / "manifest.json")
            )
            manifest.counts.update(
                new=counts.new,
                repeated=counts.repeated,
                new_versions=counts.new_versions,
            )
            self.storage.mark_committed(manifest)
            reconciled.append(manifest.run_id)
        return reconciled

    def _new_manifest(self, parameters: dict[str, Any], checkpoint: str | None) -> RunManifest:
        run_id = str(self.uuid_factory())
        manifest = RunManifest(
            run_id=run_id,
            dataset_id=self.contract.dataset_id,
            contract_sha256=self.contract.contract_hash,
            parameters=parameters,
            checkpoint_before=checkpoint,
            started_at_utc=self._now(),
            token_used=bool(getattr(self.transport, "token", None)),
            lanes={
                "primary": {"captures_updates": True, "complete": False},
                "null_watermark": {"captures_updates": False, "complete": False},
            },
            coverage_limitations=[
                "Rows without both ultima_actualizacion and fecha_de_firma are not covered.",
                "The null-watermark lane cannot guarantee capture of later updates.",
            ],
        )
        self.storage.create_staging(manifest)
        return manifest

    def _resume(self, run_id: str, parameters: dict[str, Any]) -> RunManifest:
        manifest = self.storage.load_staging(run_id)
        if manifest.contract_sha256 != self.contract.contract_hash:
            raise ResumeError("Cannot resume with a different source contract")
        if manifest.parameters != parameters:
            raise ResumeError("Cannot resume with different effective parameters")
        manifest.status = "staging"
        manifest.finished_at_utc = None
        return manifest

    def _parameters(self, window: RunWindow) -> dict[str, Any]:
        return {
            "requested_from": window.requested_from_text,
            "requested_to": window.requested_to_text,
            "effective_from": window.effective_from_text,
            "page_size": self.config.page_size,
            "max_rows": self.config.max_rows,
            "overlap_days": self.config.overlap_days,
            "projection": list(self.config.projection),
            "fallback_event_field": self.config.fallback_event_field,
        }

    def _extract(self, manifest: RunManifest, window: RunWindow) -> None:
        received = sum(page.row_count for page in manifest.pages)
        for lane in ("primary", "null_watermark"):
            lane_state = manifest.lanes[lane]
            if lane_state.get("complete"):
                continue
            existing = [page for page in manifest.pages if page.lane == lane]
            cursor = _cursor_from_dict(existing[-1].final_cursor) if existing else None
            sequence = existing[-1].sequence + 1 if existing else 1
            while received < self.config.max_rows:
                request_limit = min(self.config.page_size, self.config.max_rows - received)
                query = self._query(lane, window, request_limit, cursor)
                response = self.transport.query(
                    query,
                    on_retry=lambda event, lane=lane, sequence=sequence: self.logger.emit(
                        "request_retry",
                        run_id=manifest.run_id,
                        lane=lane,
                        page=sequence,
                        **event,
                    ),
                )
                rows = response.payload
                assert isinstance(rows, list)
                drift = validate_page(
                    rows,
                    self.contract,
                    lane=lane,
                    fallback_event_field=self.config.fallback_event_field,
                )
                manifest.additive_drift = sorted(set(manifest.additive_drift) | set(drift))
                final_cursor = (
                    cursor_from_row(rows[-1], self.contract, lane=lane) if rows else cursor
                )
                self._validate_progress(rows, lane, cursor)
                if rows:
                    relative, digest = self.storage.write_page(
                        manifest.run_id, lane, sequence, response.raw
                    )
                    page = PageRecord(
                        lane=lane,
                        sequence=sequence,
                        path=relative,
                        sha256=digest,
                        row_count=len(rows),
                        status=response.status,
                        duration_ms=response.duration_ms,
                        retries=response.retries,
                        requested_cursor=cursor.as_dict() if cursor else None,
                        final_cursor=final_cursor.as_dict() if final_cursor else None,
                        fetched_at_utc=self._now(),
                        query=query,
                    )
                    manifest.pages.append(page)
                    manifest.counts["received"] += len(rows)
                    manifest.counts["written"] += len(rows)
                    manifest.counts["retries"] += response.retries
                    received += len(rows)
                    sequence += 1
                    cursor = final_cursor
                    self.storage.write_page_state(manifest.run_id, manifest)
                    self.logger.emit(
                        "page_written",
                        run_id=manifest.run_id,
                        lane=lane,
                        page=page.sequence,
                        attempt=response.retries + 1,
                        status=response.status,
                        duration_ms=response.duration_ms,
                        row_count=len(rows),
                    )
                if len(rows) < request_limit:
                    lane_state["complete"] = True
                    lane_state["last_cursor"] = cursor.as_dict() if cursor else None
                    self.storage.write_page_state(manifest.run_id, manifest)
                    break
            if received >= self.config.max_rows and not lane_state.get("complete"):
                manifest.truncated = True
                break

    def _query(
        self,
        lane: str,
        window: RunWindow,
        limit: int,
        cursor: SourceCursor | None,
    ) -> dict[str, str | int]:
        if lane == "primary":
            return primary_query(
                self.contract, self.config.projection, window, limit=limit, cursor=cursor
            )
        return null_watermark_query(
            self.contract,
            self.config.projection,
            window,
            fallback_event_field=self.config.fallback_event_field,
            limit=limit,
            cursor=cursor,
        )

    def _validate_progress(
        self, rows: list[dict[str, Any]], lane: str, requested: SourceCursor | None
    ) -> None:
        previous = requested
        for row in rows:
            current = cursor_from_row(row, self.contract, lane=lane)
            if previous is not None and current <= previous:
                raise PageValidationError(f"Cursor did not progress in {lane}: {current.as_dict()}")
            previous = current

    def _finish(self, manifest: RunManifest, window: RunWindow) -> RunManifest:
        manifest.range_complete = (
            all(lane.get("complete") for lane in manifest.lanes.values()) and not manifest.truncated
        )
        manifest.checkpoint_candidate = (
            max(window.requested_to_text, manifest.checkpoint_before or window.requested_to_text)
            if manifest.range_complete
            else None
        )
        manifest.finished_at_utc = self._now()
        manifest.status = "prepared"
        published = self.storage.prepare(manifest)
        observations = observations_from_pages(published, manifest, self.contract.identity_field)
        counts = self.state.commit(
            manifest, observations, manifest_path=str(published / "manifest.json")
        )
        manifest.counts.update(
            new=counts.new,
            repeated=counts.repeated,
            new_versions=counts.new_versions,
        )
        self.storage.mark_committed(manifest)
        self.logger.emit(
            "run_committed",
            run_id=manifest.run_id,
            row_count=manifest.counts["written"],
            range_complete=manifest.range_complete,
            truncated=manifest.truncated,
        )
        return manifest

    def _now(self) -> str:
        return format_utc(self.clock())


def _cursor_from_dict(value: dict[str, str | None] | None) -> SourceCursor | None:
    if not value:
        return None
    identity = value.get("identity")
    if not isinstance(identity, str):
        raise ResumeError("Persisted cursor lacks an identity")
    return SourceCursor(timestamp=value.get("timestamp"), identity=identity)
