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
        campaign_id: str | None = None,
        invocation_key: str | None = None,
    ) -> RunManifest:
        scope = PopulationScope(signing_from, signing_to)
        positive(total_rows, "total rows")
        positive(total_requests, "total requests")
        positive(grant_rows, "grant rows")
        positive(grant_requests, "grant requests")
        self._validate_id(resume)
        self._validate_id(campaign_id)
        self._validate_id(invocation_key)
        if (campaign_id is None) != (invocation_key is None):
            raise ResumeError("Named reservation requires campaign_id and invocation_key together")
        if resume is not None and campaign_id is not None and resume != campaign_id:
            raise ResumeError("Resume and reserved campaign identities disagree")
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
        if campaign_id is not None:
            parameters["reservation_mode"] = "named-v1"
        parameters["plan_id"] = sha256_bytes(canonical_json(parameters))
        lock = self.storage.locks_root / f"{self.contract.dataset_id}.lock"
        with DatasetLock(lock):
            self.reconcile_prepared()
            selected_id = campaign_id or resume
            if selected_id and self.storage.run_path(selected_id).exists():
                manifest = self.storage.load_published(self.storage.run_path(selected_id))
                self._validate_resume(manifest, parameters, self.storage.run_path(selected_id))
                self._check_named_grant(manifest, invocation_key, grant_rows, grant_requests)
                return manifest
            if selected_id and self.storage.staging_path(selected_id).exists():
                manifest = self.storage.load_staging(selected_id)
            elif resume and campaign_id is None:
                manifest = self.storage.load_staging(resume)
            else:
                manifest = self._start(parameters, campaign_id=campaign_id)
            self._validate_resume(manifest, parameters, self.storage.staging_path(manifest.run_id))
            self._check_named_grant(manifest, invocation_key, grant_rows, grant_requests)
            # Terminal pages may survive a crash before prepare/commit. Finish them
            # without another invocation grant or source observation.
            if manifest.range_complete:
                progress(manifest)["terminal_reason"] = "plan_exhausted"
                manifest.truncated = False
                return self._finish_campaign(manifest, "acquired")
            ledger = CampaignLedger(manifest, self.storage)
            try:
                self._grant_or_resume(ledger, invocation_key, grant_rows, grant_requests)
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

    @staticmethod
    def _check_named_grant(
        manifest: RunManifest, invocation_key: str | None, rows: int, attempts: int
    ) -> dict[str, Any] | None:
        if invocation_key is None:
            return None
        invocations = progress(manifest)["invocations"]
        matches = [i for i in invocations if i.get("reservation_key") == invocation_key]
        if not matches:
            return None
        if len(matches) != 1:
            raise ResumeError("Named invocation reservation is duplicated")
        invocation = matches[0]
        if invocation["grant_rows"] != rows or invocation["grant_attempts"] != attempts:
            raise ResumeError("Cannot reuse named invocation with changed grant limits")
        if invocation is not invocations[-1]:
            raise ResumeError("Cannot resume a superseded invocation reservation")
        return invocation

    def _grant_or_resume(
        self, ledger: CampaignLedger, invocation_key: str | None, rows: int, attempts: int
    ) -> None:
        invocation = self._check_named_grant(ledger.manifest, invocation_key, rows, attempts)
        if invocation is None:
            ledger.grant(rows, attempts, self._now())
            if invocation_key is not None:
                progress(ledger.manifest)["invocations"][-1]["reservation_key"] = invocation_key
        # A matching named invocation reuses its original grant and reservations.
        # The subsequent persist updates its state without changing allowance.

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
        if value is not None and (
            not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", value)
        ):
            raise ResumeError("Campaign ID must be a single safe identifier")

    def _start(self, parameters: dict[str, Any], *, campaign_id: str | None = None) -> RunManifest:
        baseline = parameters["baseline"]
        manifest = RunManifest(
            run_id=campaign_id or str(self.uuid_factory()),
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
