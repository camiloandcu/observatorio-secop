"""Command-line interface for local Bronze ingestion."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from observatorio_secop.ingestion.config import load_ingestion_config
from observatorio_secop.ingestion.contract import load_bronze_contract
from observatorio_secop.ingestion.errors import IngestionError
from observatorio_secop.ingestion.runner import BronzeIngestion
from observatorio_secop.ingestion.state import IngestionState
from observatorio_secop.ingestion.storage import BronzeStorage

DEFAULT_CONFIG = Path("config/bronze_ingestion.yaml")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Ingest bounded SECOP II pages into local Bronze")
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subcommands = result.add_subparsers(dest="command", required=True)
    run = subcommands.add_parser("run", help="Run or resume an extraction")
    run.add_argument("--from", dest="requested_from", required=True)
    run.add_argument("--to", dest="requested_to", required=True)
    run.add_argument("--page-size", type=int)
    run.add_argument("--max-rows", type=int)
    run.add_argument("--overlap-days", type=int)
    run.add_argument("--resume")
    refresh = subcommands.add_parser(
        "refresh-identity", help="Refresh observed IDs without checkpoints"
    )
    refresh.add_argument("--baseline", type=Path, required=True, help="Pinned Silver manifest")
    refresh.add_argument(
        "--ids", type=Path, required=True, help="JSON array of baseline contract IDs"
    )
    refresh.add_argument("--max-rows", type=int, required=True)
    refresh.add_argument("--page-size", type=int, required=True)
    refresh.add_argument("--max-requests", type=int, default=20)
    refresh.add_argument("--resume")
    reconcile = subcommands.add_parser(
        "reconcile", help="Acquire a bounded signing population without changing checkpoints"
    )
    reconcile.add_argument("--signing-from", required=True, help="Inclusive source date YYYY-MM-DD")
    reconcile.add_argument("--signing-to", required=True, help="Exclusive source date YYYY-MM-DD")
    reconcile.add_argument("--total-rows", type=int, required=True)
    reconcile.add_argument("--total-requests", type=int, required=True)
    reconcile.add_argument("--grant-rows", type=int, required=True)
    reconcile.add_argument("--grant-requests", type=int, required=True)
    reconcile.add_argument("--page-size", type=int)
    reconcile.add_argument("--baseline", type=Path, help="Pinned verified Silver manifest")
    reconcile.add_argument("--resume", help="Resume identical campaign with a new bounded grant")
    inspect_campaign = subcommands.add_parser(
        "inspect-campaign",
        help="Verify and inspect durable population progress without source calls",
    )
    inspect_campaign.add_argument("campaign_id")
    subcommands.add_parser("inspect", help="Inspect local run and checkpoint counts")
    subcommands.add_parser("clean-staging", help="Delete only uncommitted staging directories")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_ingestion_config(args.config)
        if args.command in ("run", "refresh-identity", "reconcile"):
            config = config.with_overrides(
                page_size=args.page_size,
                max_rows=getattr(args, "max_rows", None),
                overlap_days=getattr(args, "overlap_days", None),
            )
        contract = load_bronze_contract(config.contract_path, config.projection)
        if args.command in ("reconcile", "inspect-campaign"):
            from observatorio_secop.ingestion.reconcile import PopulationReconciliation
            from observatorio_secop.warehouse.errors import SilverBundleError

            try:
                runner = PopulationReconciliation(config, contract)
                if args.command == "inspect-campaign":
                    report = runner.inspect_campaign(args.campaign_id)
                    print(json.dumps(report, sort_keys=True))
                    return 0
                manifest = runner.reconcile(
                    args.signing_from,
                    args.signing_to,
                    total_rows=args.total_rows,
                    total_requests=args.total_requests,
                    grant_rows=args.grant_rows,
                    grant_requests=args.grant_requests,
                    baseline_path=args.baseline,
                    resume=args.resume,
                )
            except (OSError, ValueError, SilverBundleError) as error:
                raise IngestionError(f"Invalid population campaign: {error}") from error
            assert manifest.population is not None
            print(
                json.dumps(
                    {
                        "run_id": manifest.run_id,
                        "campaign_id": manifest.run_id,
                        "status": manifest.status,
                        "range_complete": manifest.range_complete,
                        "truncated": manifest.truncated,
                        "rows": manifest.counts["written"],
                        "terminal_reason": manifest.population["terminal_reason"],
                        "acquisition": manifest.population["acquisition"],
                    },
                    sort_keys=True,
                )
            )
            return 0 if manifest.range_complete and manifest.status == "committed" else 2
        if args.command == "refresh-identity":
            from observatorio_secop.ingestion.refresh import IdentityRefresh, load_refresh_ids
            from observatorio_secop.warehouse.errors import SilverBundleError

            try:
                manifest = IdentityRefresh(config, contract).refresh(
                    args.baseline,
                    load_refresh_ids(args.ids, config.max_response_bytes),
                    max_requests=args.max_requests,
                    resume=args.resume,
                )
            except (OSError, ValueError, SilverBundleError) as error:
                raise IngestionError(
                    f"Invalid identity refresh baseline or IDs: {error}"
                ) from error
            print(
                json.dumps(
                    {
                        "run_id": manifest.run_id,
                        "status": manifest.status,
                        "range_complete": manifest.range_complete,
                        "truncated": manifest.truncated,
                    },
                    sort_keys=True,
                )
            )
            return 0 if manifest.status == "committed" else 2
        if args.command == "inspect":
            storage = BronzeStorage(config.bronze_root)
            report = {
                **storage.inspect(),
                **IngestionState(storage.state_root / "ingestion.sqlite").summary(
                    contract.dataset_id
                ),
            }
            print(json.dumps(report, sort_keys=True))
            return 0
        if args.command == "clean-staging":
            count = BronzeStorage(config.bronze_root).clean_staging()
            print(json.dumps({"removed_staging_runs": count}))
            return 0
        manifest = BronzeIngestion(config, contract).run(
            args.requested_from, args.requested_to, resume=args.resume
        )
        print(
            json.dumps(
                {
                    "run_id": manifest.run_id,
                    "status": manifest.status,
                    "rows": manifest.counts["written"],
                    "range_complete": manifest.range_complete,
                    "truncated": manifest.truncated,
                },
                sort_keys=True,
            )
        )
        return 0
    except IngestionError as error:
        print(f"Bronze ingestion failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
