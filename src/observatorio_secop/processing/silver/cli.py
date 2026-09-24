"""Command-line interface for local Silver processing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from observatorio_secop.ingestion.errors import IngestionError
from observatorio_secop.processing.silver.config import load_silver_config
from observatorio_secop.processing.silver.errors import SilverError
from observatorio_secop.processing.silver.runner import SilverPipeline, manifest_summary
from observatorio_secop.processing.silver.storage import SilverStorage

DEFAULT_CONFIG = Path("config/silver_quality.yaml")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Build a typed, deduplicated, quality-gated local Silver version"
    )
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subcommands = result.add_subparsers(dest="command", required=True)
    run = subcommands.add_parser("run", help="Transform committed Bronze runs")
    run.add_argument(
        "--run-id",
        action="append",
        default=[],
        help="Committed Bronze run to include; repeat to select multiple runs",
    )
    subcommands.add_parser("inspect", help="Inspect local Silver publication state")
    subcommands.add_parser("clean-staging", help="Delete only incomplete Silver staging")
    rollback = subcommands.add_parser("rollback", help="Point current to a valid version")
    rollback.add_argument("--version", required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_silver_config(args.config)
        storage = SilverStorage(config.silver_root)
        if args.command == "inspect":
            print(json.dumps(storage.inspect(), sort_keys=True))
            return 0
        if args.command == "clean-staging":
            print(json.dumps({"removed_staging_versions": storage.clean_staging()}))
            return 0
        if args.command == "rollback":
            print(json.dumps(storage.rollback(args.version), sort_keys=True))
            return 0
        manifest = SilverPipeline(config, config_path=args.config).run(tuple(args.run_id))
        print(manifest_summary(manifest))
        return 0 if manifest.critical_passed else 3
    except (IngestionError, SilverError, OSError, ValueError) as error:
        print(f"Silver processing failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
