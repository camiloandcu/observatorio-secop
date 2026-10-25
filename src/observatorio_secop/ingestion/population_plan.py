"""Governed two-lane population queries and independently replayable progress."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

from observatorio_secop.identity import key_set_digest
from observatorio_secop.ingestion.campaign import consumed, progress, update_acquisition
from observatorio_secop.ingestion.compatibility import CompatibilityRegistry
from observatorio_secop.ingestion.contract import BronzeSourceContract
from observatorio_secop.ingestion.errors import PageValidationError, ResumeError
from observatorio_secop.ingestion.models import RunManifest
from observatorio_secop.ingestion.query import _literal
from observatorio_secop.ingestion.storage import BronzeStorage, canonical_json, sha256_bytes
from observatorio_secop.ingestion.validation import validate_metadata, validate_page
from observatorio_secop.population import PopulationScope
from observatorio_secop.source_json import source_json_loads


def scope_query(
    scope: PopulationScope, projection: list[str], limit: int, cursor: str | None
) -> dict[str, str | int]:
    predicates = [
        "departamento='Antioquia'",
        f"fecha_de_firma >= {_literal(scope.signing_from)}",
        f"fecha_de_firma < {_literal(scope.signing_to)}",
    ]
    if cursor is not None:
        predicates.append(f"id_contrato > {_literal(cursor)}")
    return {
        "$select": ",".join(projection),
        "$where": " AND ".join(predicates),
        "$order": "id_contrato ASC",
        "$limit": limit,
    }


def member_query(ids: list[str], projection: list[str]) -> dict[str, str | int]:
    return {
        "$select": ",".join(projection),
        "$where": "departamento='Antioquia' AND id_contrato in ("
        + ",".join(_literal(value) for value in ids)
        + ")",
        "$order": "id_contrato ASC",
        "$limit": len(ids),
    }


def baseline_binding(path: Path | None, dataset_id: str) -> dict[str, Any] | None:
    if path is None:
        return None
    from observatorio_secop.warehouse.bundle import verify_manifest_bundle

    bundle = verify_manifest_bundle(path)
    if set(bundle.table["source_dataset_id"].to_pylist()) != {dataset_id}:
        raise ResumeError("Population baseline identifies another dataset")
    document = source_json_loads(path.read_bytes())
    dependencies = document.get("bronze_runs")
    if not isinstance(dependencies, list) or not dependencies:
        raise ResumeError("Population baseline has no pinned Bronze dependencies")
    ids = sorted(bundle.table["source_contract_id"].to_pylist())
    if len(ids) != len(set(ids)) or any(not isinstance(i, str) or not i for i in ids):
        raise ResumeError("Population baseline has invalid or duplicate contract identities")
    return {
        "version_id": bundle.version_id,
        "manifest_sha256": bundle.manifest_sha256,
        "bronze_runs": dependencies,
        "ids": ids,
        "ids_sha256": key_set_digest(ids),
        "keys_sha256": key_set_digest(bundle.table["contract_key"].to_pylist()),
    }


def validate_rows(
    rows: list[dict[str, Any]],
    lane: str,
    scope: PopulationScope,
    contract: BronzeSourceContract,
    *,
    cursor: str | None = None,
    batch: list[str] | None = None,
) -> list[str]:
    validate_page(rows, contract, lane=lane)
    raw_ids = [row.get(contract.identity_field) for row in rows]
    if any(not isinstance(i, str) or not i.strip() or "\0" in i for i in raw_ids):
        raise PageValidationError("Population response lacks a usable contractual identity")
    ids = cast(list[str], raw_ids)
    if len(ids) != len(set(ids)) or ids != sorted(ids):
        raise PageValidationError("Population response has duplicate or non-progressing IDs")
    if any(row.get("departamento") != "Antioquia" for row in rows):
        raise PageValidationError("Population response violates registration scope")
    if lane == "scope_scan":
        if (cursor is not None and ids and ids[0] <= cursor) or any(
            not scope.contains(row.get("fecha_de_firma"), row.get("departamento")) for row in rows
        ):
            raise PageValidationError(
                "Population scan has out-of-scope rows or non-progressing cursor"
            )
    elif batch is None or any(identity not in batch for identity in ids):
        raise PageValidationError("Baseline member response contains unrequested IDs")
    return ids


def verify_population_run(
    manifest: RunManifest, directory: Path, contract: BronzeSourceContract
) -> None:
    """Recompute queries, lane outcomes, grants and snapshot from durable pages."""
    from observatorio_secop.population import validate_acquisition

    if manifest.purpose != "population_reconciliation" or not isinstance(manifest.population, dict):
        raise ResumeError("Run is not a population reconciliation campaign")
    pop = progress(manifest)
    try:
        CompatibilityRegistry().resolve(
            manifest.dataset_id,
            manifest.contract_sha256,
            manifest.parameters.get("projection"),
            manifest.source_projection_version,
            manifest.source_revision,
        )
        scope = PopulationScope.from_dict(pop["scope"])
        params = dict(manifest.parameters)
        plan_id = params.pop("plan_id")
        if (
            sha256_bytes(canonical_json(params)) != plan_id
            or pop["plan_id"] != plan_id
            or pop["scope_id"] != scope.scope_id
            or params["scope"] != pop["scope"]
            or params["baseline"] != pop["baseline"]
            or params["ceilings"] != pop["ceilings"]
            or manifest.dataset_id != contract.dataset_id
            or manifest.contract_sha256 != contract.contract_hash
            or manifest.source_revision != contract.revision
            or manifest.source_projection_version != contract.projection_version
        ):
            raise ResumeError("Campaign scope/plan/profile identity disagrees")
        for page in manifest.pages:
            if (
                page.lane not in ("scope_scan", "baseline_check")
                or page.path != f"pages/{page.lane}-{page.sequence:05d}.json"
                or (directory / "pages").is_symlink()
                or (directory / page.path).is_symlink()
                or isinstance(page.query.get("$limit"), bool)
                or not isinstance(page.query.get("$limit"), int)
                or not 0 < int(page.query["$limit"]) <= params["page_size"]
            ):
                raise ResumeError("Campaign page path or request bound is invalid")
        BronzeStorage(directory.parent.parent).verify_pages(directory, manifest)
        _verify_evidence(manifest, directory, contract, scope)
        reservations = pop["reservations"]
        page_receipts = []
        previous_invocation = 0
        for ordinal, reservation in enumerate(reservations, 1):
            invocation = pop["invocations"][reservation["invocation_id"] - 1]
            if (
                reservation["reservation_id"] != ordinal
                or reservation["state"] not in ("uncertain", "response")
                or not 0 <= reservation["charged_rows"] <= reservation["max_rows"]
                or (
                    reservation["state"] == "uncertain"
                    and (
                        reservation["charged_rows"] != reservation["max_rows"]
                        or reservation["page_sequence"] is not None
                    )
                )
                or invocation["invocation_id"] != reservation["invocation_id"]
                or invocation["invocation_id"] < previous_invocation
                or reservation["lane"] not in ("metadata", "scope_scan", "baseline_check")
                or any(
                    isinstance(reservation[key], bool) or not isinstance(reservation[key], int)
                    for key in ("max_rows", "charged_rows", "invocation_id", "reservation_id")
                )
            ):
                raise ResumeError("Campaign request reservation is inconsistent")
            previous_invocation = invocation["invocation_id"]
            if reservation["state"] == "response" and reservation["lane"] != "metadata":
                page_receipts.append(reservation["page_sequence"])
            elif reservation["lane"] == "metadata" and (
                reservation["max_rows"] != 0
                or reservation["page_sequence"] is not None
                or reservation["query_sha256"] != sha256_bytes(canonical_json(None))
            ):
                raise ResumeError("Metadata reservation disagrees with its request")
        if sorted(page_receipts) != list(range(1, len(manifest.pages) + 1)):
            raise ResumeError("Campaign has response receipts without matching pages")
        metadata_responses = [
            r for r in reservations if r["lane"] == "metadata" and r["state"] == "response"
        ]
        if len(metadata_responses) != int(pop["metadata_verified"]):
            raise ResumeError("Campaign metadata proof lacks its response receipt")
        named_keys: set[str] = set()
        for invocation_id, invocation in enumerate(pop["invocations"], 1):
            key = invocation.get("reservation_key")
            named = params.get("reservation_mode") == "named-v1"
            if named:
                if (
                    not isinstance(key, str)
                    or not re.fullmatch(r"[A-Za-z0-9_-]+", key)
                    or key in named_keys
                ):
                    raise ResumeError("Campaign named invocation reservation is invalid")
                named_keys.add(key)
            elif key is not None:
                raise ResumeError("Legacy campaign cannot contain named invocation reservations")
            usage = consumed(manifest, invocation_id)
            if (
                invocation["invocation_id"] != invocation_id
                or invocation["consumed"] != usage
                or invocation["state"]
                not in (
                    "extracting",
                    "paused_budget",
                    "failed_recoverable",
                    "acquired",
                    "truncated",
                    "interrupted",
                )
            ) or any(
                isinstance(invocation[f"grant_{k}"], bool)
                or not isinstance(invocation[f"grant_{k}"], int)
                or not invocation[f"grant_{k}"] > 0
                or usage[k] > invocation[f"grant_{k}"]
                for k in ("rows", "attempts")
            ):
                raise ResumeError("Campaign invocation exceeded its grant")
        old = pop["acquisition"]
        validate_acquisition(old, scope_id=scope.scope_id)
        expected = update_acquisition(manifest, old["state"])
        pop["acquisition"] = old
        if expected != old or any(
            consumed(manifest)[k] > pop["ceilings"][k] for k in ("rows", "attempts")
        ):
            raise ResumeError("Campaign acquisition totals or terminal proof disagree")
        complete = all(lane["complete"] for lane in manifest.lanes.values())
        if pop["complete"] != complete or manifest.range_complete != complete:
            raise ResumeError("Campaign terminal completeness disagrees with verified lanes")
        if manifest.checkpoint_candidate is not None:
            raise ResumeError("Population campaign cannot offer an incremental checkpoint")
    except (KeyError, IndexError, TypeError, ValueError, OSError) as error:
        raise ResumeError(f"Invalid population campaign evidence: {error}") from error


def _verify_evidence(
    manifest: RunManifest, directory: Path, contract: BronzeSourceContract, scope: PopulationScope
) -> None:
    pop = progress(manifest)
    baseline = pop["baseline"]
    ids = baseline["ids"] if baseline else []
    if baseline and key_set_digest(ids) != baseline["ids_sha256"]:
        raise ResumeError("Baseline member ID digest disagrees")
    cursor = None
    offset = 0
    returned = []
    scan_complete = False
    received = 0
    for sequence, page in enumerate(manifest.pages, 1):
        rows = source_json_loads((directory / page.path).read_bytes())
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ResumeError("Campaign page must contain source row objects")
        if page.sequence != sequence or len(rows) != page.row_count:
            raise ResumeError("Campaign page sequence/count disagrees")
        if page.lane == "scope_scan" and not scan_complete and offset == 0:
            expected = scope_query(
                scope, manifest.parameters["projection"], int(page.query["$limit"]), cursor
            )
            values = validate_rows(rows, page.lane, scope, contract, cursor=cursor)
            previous: dict[str, str | None] | None = (
                {"timestamp": None, "identity": cursor} if cursor else None
            )
            if page.requested_cursor != previous:
                raise ResumeError("Scope scan requested cursor disagrees")
            cursor = values[-1] if values else cursor
            scan_complete = len(rows) < int(page.query["$limit"])
            final = {"timestamp": None, "identity": cursor} if cursor else None
            if page.final_cursor != final:
                raise ResumeError("Scope scan final cursor disagrees")
        elif page.lane == "baseline_check" and scan_complete:
            batch = ids[offset : offset + manifest.parameters["page_size"]]
            if not batch:
                raise ResumeError("Unexpected baseline check page")
            expected = member_query(batch, manifest.parameters["projection"])
            values = validate_rows(rows, page.lane, scope, contract, batch=batch)
            if page.requested_cursor is not None or page.final_cursor is not None:
                raise ResumeError("Fixed baseline batches cannot claim keyset cursors")
            returned.extend(values)
            offset += len(batch)
        else:
            raise ResumeError("Campaign page violates the two-lane plan")
        if expected != page.query:
            raise ResumeError("Campaign page query differs from its fixed plan")
        matches = [
            r
            for r in pop["reservations"]
            if r["state"] == "response" and r["page_sequence"] == sequence
        ]
        if len(matches) != 1 or any(
            (
                matches[0]["lane"] != page.lane,
                matches[0]["charged_rows"] != len(rows),
                matches[0]["max_rows"] != page.query["$limit"],
                matches[0]["query_sha256"] != sha256_bytes(canonical_json(page.query)),
            )
        ):
            raise ResumeError("Campaign page has no unique matching request receipt")
        received += len(rows)
    scan = manifest.lanes["scope_scan"]
    members = manifest.lanes["baseline_check"]
    if (
        scan != {"complete": scan_complete, "cursor": cursor}
        or members
        != {
            "complete": offset == len(ids),
            "next_offset": offset,
            "returned_ids": returned,
            "missing_ids": sorted(set(ids[:offset]) - set(returned)),
        }
        or manifest.counts["written"] != received
        or manifest.counts["received"] != received
    ):
        raise ResumeError("Campaign lane progress disagrees with durable pages")
    if pop["metadata_verified"]:
        metadata_path = directory / "metadata.json"
        if metadata_path.is_symlink():
            raise ResumeError("Campaign metadata cannot be a symlink")
        raw = metadata_path.read_bytes()
        if sha256_bytes(raw) != pop["metadata_sha256"]:
            raise ResumeError("Campaign metadata hash disagrees")
        validate_metadata(source_json_loads(raw), contract)
