"""Bounded full-observation identity refresh with independent resumable progress."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from observatorio_secop.identity import key_set_digest
from observatorio_secop.ingestion.errors import (
    IngestionConfigurationError,
    PageValidationError,
    RequestBudgetExceeded,
    ResumeError,
)
from observatorio_secop.ingestion.models import PageRecord, RunManifest
from observatorio_secop.ingestion.runner import BronzeIngestion
from observatorio_secop.ingestion.state import DatasetLock, observations_from_pages
from observatorio_secop.ingestion.validation import validate_metadata, validate_page
from observatorio_secop.source_json import source_json_loads
from observatorio_secop.warehouse.bundle import verify_manifest_bundle


def load_refresh_ids(path: Path, max_bytes: int) -> list[str]:
    with path.open("rb") as stream:
        raw = stream.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise IngestionConfigurationError("Refresh ID file exceeds the response-byte limit")
    ids = json.loads(raw)
    if (
        not isinstance(ids, list)
        or not ids
        or any(not isinstance(value, str) or not value.strip() or "\0" in value for value in ids)
        or len(ids) != len(set(ids))
    ):
        raise IngestionConfigurationError("Refresh requires a nonempty unique JSON array of IDs")
    return sorted(ids)


class IdentityRefresh(BronzeIngestion):
    def refresh(
        self,
        baseline_path: Path,
        ids: list[str],
        *,
        max_requests: int = 20,
        resume: str | None = None,
    ) -> RunManifest:
        if (
            not ids
            or len(ids) != len(set(ids))
            or len(ids) > self.config.max_rows
            or any(not isinstance(v, str) or not v.strip() or "\0" in v for v in ids)
            or not 0 < max_requests <= 20
        ):
            raise IngestionConfigurationError("Invalid identity refresh IDs or row/request limits")
        if resume and not re.fullmatch(r"[A-Za-z0-9_-]+", resume):
            raise ResumeError("Invalid refresh run ID")
        baseline = verify_manifest_bundle(baseline_path)
        if set(baseline.table["source_dataset_id"].to_pylist()) != {self.contract.dataset_id}:
            raise IngestionConfigurationError("Refresh baseline identifies another dataset")
        if not set(ids) <= set(baseline.table["source_contract_id"].to_pylist()):
            raise IngestionConfigurationError("Refresh IDs must belong to the pinned baseline")
        baseline_document = json.loads(baseline_path.read_bytes())
        dependencies = baseline_document.get("bronze_runs")
        if not isinstance(dependencies, list) or not dependencies:
            raise IngestionConfigurationError("Refresh baseline lacks pinned Bronze dependencies")
        ids = sorted(ids)
        parameters = {
            "projection": list(self.config.projection),
            "baseline_version_id": baseline.version_id,
            "baseline_manifest_sha256": baseline.manifest_sha256,
            "ids_sha256": key_set_digest(ids),
            "requested_ids": ids,
            "max_rows": self.config.max_rows,
            "page_size": self.config.page_size,
            "max_requests_per_invocation": max_requests,
        }
        with DatasetLock(self.storage.locks_root / f"{self.contract.dataset_id}.lock"):
            self.reconcile_prepared()
            if resume and self.storage.run_path(resume).exists():
                completed = self.storage.load_published(self.storage.run_path(resume))
                self._check_resume(completed, parameters)
                if (
                    completed.refresh is None
                    or completed.refresh["baseline_bronze_runs"] != dependencies
                ):
                    raise ResumeError("Refresh baseline dependencies changed")
                return completed
            manifest = (
                self.storage.load_staging(resume)
                if resume
                else self._start(parameters, dependencies)
            )
            self._check_resume(manifest, parameters)
            assert manifest.refresh is not None
            if manifest.refresh["baseline_bronze_runs"] != dependencies:
                raise ResumeError("Refresh baseline dependencies changed")
            self._attempts_before = manifest.refresh["http_attempts"]
            manifest.status = "staging"
            manifest.truncated = False
            manifest.finished_at_utc = None
            self.transport.max_requests = max_requests
            self.transport.requests = 0
            try:
                if not manifest.refresh["metadata_verified"]:
                    response = self.transport.fetch_metadata()
                    if not isinstance(response.payload, dict):
                        raise PageValidationError("Refresh metadata must be an object")
                    validate_metadata(response.payload, self.contract)
                    manifest.refresh["metadata_verified"] = True
                    self._record_attempts(manifest)
                    self.storage.write_page_state(manifest.run_id, manifest)
                self._extract_ids(manifest, ids)
                manifest.range_complete = True
                manifest.refresh["complete"] = True
                manifest.refresh["missing_ids"] = sorted(
                    set(ids) - set(manifest.refresh["returned_ids"])
                )
                manifest.checkpoint_candidate = None
                manifest.finished_at_utc = self._now()
                self._record_attempts(manifest)
                published = self.storage.prepare(manifest)
                observations = observations_from_pages(
                    published, manifest, self.contract.identity_field
                )
                counts = self.state.commit(
                    manifest, observations, manifest_path=str(published / "manifest.json")
                )
                manifest.counts.update(
                    new=counts.new, repeated=counts.repeated, new_versions=counts.new_versions
                )
                self.storage.mark_committed(manifest)
                return manifest
            except RequestBudgetExceeded:
                self._record_attempts(manifest)
                manifest.truncated = True
                manifest.status = "truncated_recoverable"
                self.storage.write_page_state(manifest.run_id, manifest)
                return manifest
            except Exception:
                self._record_attempts(manifest)
                manifest.status = "failed_recoverable"
                if self.storage.staging_path(manifest.run_id).exists():
                    self.storage.write_page_state(manifest.run_id, manifest)
                raise

    def _start(self, parameters: dict[str, Any], dependencies: list[dict[str, str]]) -> RunManifest:
        manifest = RunManifest(
            run_id=str(self.uuid_factory()),
            dataset_id=self.contract.dataset_id,
            contract_sha256=self.contract.contract_hash,
            parameters=parameters,
            source_revision=self.contract.revision,
            source_projection_version=self.contract.projection_version,
            checkpoint_before=self.state.checkpoint(self.contract.dataset_id),
            started_at_utc=self._now(),
            purpose="identity_refresh",
            refresh={
                "baseline_bronze_runs": dependencies,
                "complete": False,
                "metadata_verified": False,
                "next_id_offset": 0,
                "returned_ids": [],
                "missing_ids": [],
                "http_attempts": 0,
            },
        )
        self.storage.create_staging(manifest)
        return manifest

    def _check_resume(self, manifest: RunManifest, parameters: dict[str, Any]) -> None:
        if (
            manifest.purpose != "identity_refresh"
            or manifest.dataset_id != self.contract.dataset_id
            or manifest.source_revision != self.contract.revision
            or manifest.parameters != parameters
            or manifest.contract_sha256 != self.contract.contract_hash
            or manifest.source_projection_version != self.contract.projection_version
        ):
            raise ResumeError("Cannot resume refresh with changed baseline/IDs/limits/profile")
        if not isinstance(manifest.refresh, dict):
            raise ResumeError("Refresh progress evidence is missing")
        directory = (
            self.storage.run_path(manifest.run_id)
            if manifest.status == "committed"
            else self.storage.staging_path(manifest.run_id)
        )
        returned: list[str] = []
        ids = parameters["requested_ids"]
        for index, page in enumerate(manifest.pages):
            offset = index * self.config.page_size
            batch = ids[offset : offset + self.config.page_size]
            if not batch or page.lane != "identity_refresh" or page.sequence != index + 1:
                raise ResumeError("Refresh page sequence disagrees with ID progress")
            rows = source_json_loads((directory / page.path).read_bytes())
            values = [row.get(self.contract.identity_field) for row in rows]
            if (
                len(values) != len(set(values))
                or any(value not in batch for value in values)
                or values != sorted(values)
                or page.query != _id_query(batch, self.config.projection)
                or any(row.get("departamento") != "Antioquia" for row in rows)
            ):
                raise ResumeError("Refresh page content/query disagrees with ID progress")
            returned.extend(values)
        expected_offset = min(len(manifest.pages) * self.config.page_size, len(ids))
        if (
            manifest.refresh["next_id_offset"] != expected_offset
            or manifest.refresh["returned_ids"] != returned
            or manifest.counts["written"] != len(returned)
        ):
            raise ResumeError("Refresh persisted progress disagrees with verified pages")
        if manifest.status == "committed" and (
            expected_offset != len(ids)
            or manifest.refresh.get("complete") is not True
            or manifest.refresh["missing_ids"] != sorted(set(ids) - set(returned))
            or not manifest.range_complete
            or manifest.truncated
        ):
            raise ResumeError("Committed refresh completion evidence is inconsistent")

    def _record_attempts(self, manifest: RunManifest) -> None:
        assert manifest.refresh is not None
        manifest.refresh["http_attempts"] = self._attempts_before + self.transport.requests

    def _extract_ids(self, manifest: RunManifest, ids: list[str]) -> None:
        assert manifest.refresh is not None
        while manifest.refresh["next_id_offset"] < len(ids):
            offset = manifest.refresh["next_id_offset"]
            batch = ids[offset : offset + self.config.page_size]
            query = _id_query(batch, self.config.projection)
            response = self.transport.query(query)
            rows = response.payload
            if not isinstance(rows, list) or len(rows) > len(batch):
                raise PageValidationError("Identity refresh returned an invalid bounded page")
            validate_page(rows, self.contract, lane="identity_refresh")
            returned = [row.get(self.contract.identity_field) for row in rows]
            if (
                any(not isinstance(value, str) or value not in batch for value in returned)
                or len(set(returned)) != len(returned)
                or returned != sorted(returned)
                or any(row.get("departamento") != "Antioquia" for row in rows)
            ):
                raise PageValidationError(
                    "Identity refresh returned duplicate/unrequested/out-of-scope IDs"
                )
            sequence = len(manifest.pages) + 1
            relative, digest = self.storage.write_page(
                manifest.run_id, "identity_refresh", sequence, response.raw
            )
            manifest.pages.append(
                PageRecord(
                    lane="identity_refresh",
                    sequence=sequence,
                    path=relative,
                    sha256=digest,
                    row_count=len(rows),
                    status=response.status,
                    duration_ms=response.duration_ms,
                    retries=response.retries,
                    requested_cursor=None,
                    final_cursor=None,
                    fetched_at_utc=self._now(),
                    query=query,
                )
            )
            manifest.refresh["next_id_offset"] += len(batch)
            manifest.refresh["returned_ids"].extend(returned)
            self._record_attempts(manifest)
            manifest.counts["received"] += len(rows)
            manifest.counts["written"] += len(rows)
            manifest.counts["retries"] += response.retries
            self.storage.write_page_state(manifest.run_id, manifest)


def _id_query(batch: list[str], projection: tuple[str, ...]) -> dict[str, str | int]:
    literals = ",".join("'" + value.replace("'", "''") + "'" for value in batch)
    return {
        "$select": ",".join(projection),
        "$where": f"departamento='Antioquia' AND id_contrato in ({literals})",
        "$order": "id_contrato",
        "$limit": len(batch),
    }
