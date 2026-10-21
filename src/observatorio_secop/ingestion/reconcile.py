"""Bounded signing-population acquisition, durable inspection and explicit resume."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from observatorio_secop.ingestion.campaign import (
    CampaignBudgetExceeded,
    CampaignLedger,
    positive,
    progress,
    record_invocation,
    update_acquisition,
)
from observatorio_secop.ingestion.errors import PageValidationError, ResumeError
from observatorio_secop.ingestion.models import PageRecord, RunManifest
from observatorio_secop.ingestion.population_plan import (
    baseline_binding,
    member_query,
    scope_query,
    validate_rows,
    verify_population_run,
)
from observatorio_secop.ingestion.runner import BronzeIngestion
from observatorio_secop.ingestion.state import DatasetLock, observations_from_pages
from observatorio_secop.ingestion.storage import canonical_json, durable_write, sha256_bytes
from observatorio_secop.ingestion.validation import validate_metadata
from observatorio_secop.population import PopulationScope


class PopulationReconciliation(BronzeIngestion):
    def reconcile(
        self,
        signing_from: str,
        signing_to: str,
        *,
        total_rows: int,
        total_requests: int,
        grant_rows: int,
        grant_requests: int,
        baseline_path: Path | None = None,
        resume: str | None = None,
    ) -> RunManifest:
        scope = PopulationScope(signing_from, signing_to)
        positive(total_rows, "total rows")
        positive(total_requests, "total requests")
        positive(grant_rows, "grant rows")
        positive(grant_requests, "grant requests")
        self._validate_id(resume)
        baseline = baseline_binding(baseline_path, self.contract.dataset_id)
        parameters = {
            "scope": scope.as_dict(),
            "baseline": baseline,
            "ceilings": {"rows": total_rows, "attempts": total_requests},
            "page_size": self.config.page_size,
            "projection": list(self.config.projection),
            "source_revision": self.contract.revision,
            "source_projection_version": self.contract.projection_version,
            "contract_sha256": self.contract.contract_hash,
            "plan_version": "population-reconciliation-v1",
        }
        parameters["plan_id"] = sha256_bytes(canonical_json(parameters))
        lock = self.storage.locks_root / f"{self.contract.dataset_id}.lock"
        with DatasetLock(lock):
            self.reconcile_prepared()
            if resume and self.storage.run_path(resume).exists():
                manifest = self.storage.load_published(self.storage.run_path(resume))
                self._validate_resume(manifest, parameters, self.storage.run_path(resume))
                return manifest
            manifest = self.storage.load_staging(resume) if resume else self._start(parameters)
            self._validate_resume(manifest, parameters, self.storage.staging_path(manifest.run_id))
            # Terminal pages may survive a crash before prepare/commit. Finish them
            # without another invocation grant or source observation.
            if manifest.range_complete:
                progress(manifest)["terminal_reason"] = "plan_exhausted"
                manifest.truncated = False
                return self._finish_campaign(manifest, "acquired")
            ledger = CampaignLedger(manifest, self.storage)
            try:
                ledger.grant(grant_rows, grant_requests, self._now())
                manifest.status = "extracting"
                manifest.truncated = False
                pop = progress(manifest)
                pop["terminal_reason"] = None
                ledger.persist("extracting")
                # Only this campaign owns the hook, under the dataset writer lock.
                self.transport.before_attempt = ledger.reserve
                self.transport.max_requests = None
                self._metadata(manifest, ledger)
                self._scan(manifest, ledger, scope)
                self._members(manifest, ledger, scope)
                pop["complete"] = manifest.range_complete = True
                pop["terminal_reason"] = "plan_exhausted"
                return self._finish_campaign(manifest, "acquired")
            except CampaignBudgetExceeded as error:
                progress(manifest)["terminal_reason"] = error.reason
                manifest.truncated = True
                if error.terminal:
                    return self._finish_campaign(manifest, "truncated")
                manifest.status = "paused_budget"
                ledger.persist("paused_budget")
                return manifest
            except Exception:
                manifest.status = "failed_recoverable"
                progress(manifest)["terminal_reason"] = "request_or_evidence_failure"
                if self.storage.staging_path(manifest.run_id).exists():
                    ledger.persist("failed_recoverable")
                raise
            finally:
                self.transport.before_attempt = None

    def inspect_campaign(self, campaign_id: str) -> dict[str, Any]:
        self._validate_id(campaign_id)
        lock = self.storage.locks_root / f"{self.contract.dataset_id}.lock"
        with DatasetLock(lock):
            directory = self.storage.run_path(campaign_id)
            if directory.exists():
                manifest = self.storage.load_published(directory)
            else:
                directory = self.storage.staging_path(campaign_id)
                manifest = self.storage.load_staging(campaign_id)
            verify_population_run(manifest, directory, self.contract)
            return {
                "campaign_id": manifest.run_id,
                "status": manifest.status,
                "rows": manifest.counts["written"],
                "purpose": manifest.purpose,
                "terminal_reason": progress(manifest)["terminal_reason"],
                "acquisition": progress(manifest)["acquisition"],
                "invocations": progress(manifest)["invocations"],
                "reservations": progress(manifest)["reservations"],
            }

    @staticmethod
    def _validate_id(value: str | None) -> None:
        if value is not None and not re.fullmatch(r"[A-Za-z0-9_-]+", value):
            raise ResumeError("Campaign ID must be a single safe identifier")

    def _start(self, parameters: dict[str, Any]) -> RunManifest:
        baseline = parameters["baseline"]
        manifest = RunManifest(
            run_id=str(self.uuid_factory()),
            dataset_id=self.contract.dataset_id,
            contract_sha256=self.contract.contract_hash,
            parameters=parameters,
            checkpoint_before=self.state.checkpoint(self.contract.dataset_id),
            started_at_utc=self._now(),
            purpose="population_reconciliation",
            source_revision=self.contract.revision,
            source_projection_version=self.contract.projection_version,
            token_used=bool(getattr(self.transport, "token", None)),
            lanes={
                "scope_scan": {"complete": False, "cursor": None},
                "baseline_check": {
                    "complete": baseline is None or not baseline["ids"],
                    "next_offset": 0,
                    "returned_ids": [],
                    "missing_ids": [],
                },
            },
            population={
                "schema_version": "population-campaign-v1",
                "scope": parameters["scope"],
                "scope_id": PopulationScope.from_dict(parameters["scope"]).scope_id,
                "plan_id": parameters["plan_id"],
                "baseline": baseline,
                "ceilings": dict(parameters["ceilings"]),
                "invocations": [],
                "reservations": [],
                "metadata_verified": False,
                "metadata_sha256": None,
                "complete": False,
                "terminal_reason": None,
            },
        )
        update_acquisition(manifest, "planned")
        self.storage.create_staging(manifest)
        return manifest

    def _validate_resume(
        self, manifest: RunManifest, parameters: dict[str, Any], directory: Path
    ) -> None:
        if (
            manifest.parameters != parameters
            or manifest.contract_sha256 != self.contract.contract_hash
        ):
            raise ResumeError("Cannot resume with changed scope/baseline/plan/profile/ceilings")
        verify_population_run(manifest, directory, self.contract)

    def _metadata(self, manifest: RunManifest, ledger: CampaignLedger) -> None:
        pop = progress(manifest)
        if pop["metadata_verified"]:
            return
        ledger.lane, ledger.query, ledger.row_capacity = "metadata", None, 0
        response = self.transport.fetch_metadata()
        if not isinstance(response.payload, dict):
            raise PageValidationError("Population metadata must be a JSON object")
        manifest.additive_drift = validate_metadata(response.payload, self.contract)
        durable_write(self.storage.staging_path(manifest.run_id) / "metadata.json", response.raw)
        pop["metadata_verified"] = True
        pop["metadata_sha256"] = sha256_bytes(response.raw)
        ledger.response(0, None)
        ledger.persist("extracting")

    def _scan(self, manifest: RunManifest, ledger: CampaignLedger, scope: PopulationScope) -> None:
        lane = manifest.lanes["scope_scan"]
        while not lane["complete"]:
            ledger.require(1)
            limit = min(self.config.page_size, ledger.remaining("rows"))
            query = scope_query(scope, list(self.config.projection), limit, lane["cursor"])
            ids = self._page(manifest, ledger, scope, "scope_scan", query, cursor=lane["cursor"])
            lane["cursor"] = ids[-1] if ids else lane["cursor"]
            lane["complete"] = len(ids) < limit
            ledger.persist("extracting")

    def _members(
        self, manifest: RunManifest, ledger: CampaignLedger, scope: PopulationScope
    ) -> None:
        lane = manifest.lanes["baseline_check"]
        baseline = progress(manifest)["baseline"]
        ids = baseline["ids"] if baseline else []
        while not lane["complete"]:
            offset = lane["next_offset"]
            batch = ids[offset : offset + self.config.page_size]
            ledger.require(len(batch))
            query = member_query(batch, list(self.config.projection))
            returned = self._page(manifest, ledger, scope, "baseline_check", query, batch=batch)
            lane["next_offset"] += len(batch)
            lane["returned_ids"].extend(returned)
            lane["missing_ids"] = sorted(
                set(ids[: lane["next_offset"]]) - set(lane["returned_ids"])
            )
            lane["complete"] = lane["next_offset"] == len(ids)
            ledger.persist("extracting")

    def _page(
        self,
        manifest: RunManifest,
        ledger: CampaignLedger,
        scope: PopulationScope,
        lane: str,
        query: dict[str, str | int],
        *,
        cursor: str | None = None,
        batch: list[str] | None = None,
    ) -> list[str]:
        ledger.lane, ledger.query, ledger.row_capacity = lane, query, int(query["$limit"])
        response = self.transport.query(query)
        ids = validate_rows(
            response.payload, lane, scope, self.contract, cursor=cursor, batch=batch
        )
        sequence = len(manifest.pages) + 1
        relative, digest = self.storage.write_page(manifest.run_id, lane, sequence, response.raw)
        final = ids[-1] if ids else cursor
        manifest.pages.append(
            PageRecord(
                lane=lane,
                sequence=sequence,
                path=relative,
                sha256=digest,
                row_count=len(ids),
                status=response.status,
                duration_ms=response.duration_ms,
                retries=response.retries,
                requested_cursor={"timestamp": None, "identity": cursor} if cursor else None,
                final_cursor={"timestamp": None, "identity": final}
                if lane == "scope_scan" and final
                else None,
                fetched_at_utc=self._now(),
                query=query,
            )
        )
        manifest.counts["received"] += len(ids)
        manifest.counts["written"] += len(ids)
        manifest.counts["retries"] += response.retries
        ledger.response(len(ids), sequence)
        return ids

    def _finish_campaign(self, manifest: RunManifest, state: str) -> RunManifest:
        manifest.finished_at_utc = self._now()
        manifest.checkpoint_candidate = None
        record_invocation(manifest, state)
        update_acquisition(manifest, state)
        verify_population_run(manifest, self.storage.staging_path(manifest.run_id), self.contract)
        published = self.storage.prepare(manifest)
        counts = self.state.commit(
            manifest,
            observations_from_pages(published, manifest, self.contract.identity_field),
            manifest_path=str(published / "manifest.json"),
        )
        manifest.counts.update(
            new=counts.new, repeated=counts.repeated, new_versions=counts.new_versions
        )
        self.storage.mark_committed(manifest)
        return manifest
