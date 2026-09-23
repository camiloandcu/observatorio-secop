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
    subcommands.add_parser("inspect", help="Inspect local run and checkpoint counts")
    subcommands.add_parser("clean-staging", help="Delete only uncommitted staging directories")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_ingestion_config(args.config)
        if args.command == "run":
            config = config.with_overrides(
                page_size=args.page_size,
                max_rows=args.max_rows,
                overlap_days=args.overlap_days,
            )
        contract = load_bronze_contract(config.contract_path, config.projection)
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
