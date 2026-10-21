"""Command-line interface for local Silver processing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from observatorio_secop.ingestion.errors import IngestionError
from observatorio_secop.population import PopulationScope
from observatorio_secop.processing.silver.config import load_silver_config
from observatorio_secop.processing.silver.errors import SilverError
from observatorio_secop.processing.silver.runner import SilverPipeline, manifest_summary
from observatorio_secop.processing.silver.storage import SilverStorage
from observatorio_secop.warehouse.errors import SilverBundleError

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
    inspect = subcommands.add_parser("inspect", help="Inspect local Silver publication state")
    inspect.add_argument("--version", help="Inspect immutable version/invalid coverage evidence")
    subcommands.add_parser("clean-staging", help="Delete only incomplete Silver staging")
    rollback = subcommands.add_parser("rollback", help="Point current to a valid version")
    rollback.add_argument("--version", required=True)
    population = subcommands.add_parser(
        "run-population", help="Assemble scoped baseline and governed campaigns"
    )
    population.add_argument("--signing-from", required=True)
    population.add_argument("--signing-to", required=True)
    population.add_argument("--campaign-run-id", action="append", required=True)
    population.add_argument("--baseline-version")
    population.add_argument("--baseline-manifest-sha256")
    population.add_argument(
        "--mode", choices=("preview", "authoritative_candidate"), default="preview"
    )
    adoption = subcommands.add_parser("adopt", help="Verify and bind the current legacy population")
    adoption.add_argument("--version", required=True)
    adoption.add_argument("--manifest-sha256", required=True)
    adoption.add_argument("--signing-from", required=True)
    adoption.add_argument("--signing-to", required=True)
    restore = subcommands.add_parser(
        "restore", help="Restore an exact registered compatible version"
    )
    restore.add_argument("--version", required=True)
    restore.add_argument("--manifest-sha256", required=True)
    restore.add_argument("--expected-current-version", required=True)
    restore.add_argument("--expected-current-manifest-sha256", required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_silver_config(args.config)
        storage = SilverStorage(config.silver_root)
        if args.command == "inspect":
            if args.version:
                if Path(args.version).name != args.version or args.version in (".", ".."):
                    raise ValueError("Invalid version identifier")
                directory = storage.version_path(args.version)
                if not directory.exists():
                    directory = storage.invalid_path(args.version)
                inspected_manifest = storage.load_manifest(directory)
                population = inspected_manifest.get("population")
                evidence = (
                    None
                    if population is None
                    else json.loads((directory / population["coverage_path"]).read_bytes())
                )
                print(
                    json.dumps(
                        {"manifest": inspected_manifest, "coverage": evidence}, sort_keys=True
                    )
                )
            else:
                print(json.dumps(storage.inspect(), sort_keys=True))
            return 0
        if args.command == "clean-staging":
            print(json.dumps({"removed_staging_versions": storage.clean_staging()}))
            return 0
        if args.command == "rollback":
            print(json.dumps(storage.rollback(args.version), sort_keys=True))
            return 0
        if args.command == "restore":
            pointer = storage.rollback(
                args.version,
                manifest_sha256=args.manifest_sha256,
                expected_current={
                    "silver_version_id": args.expected_current_version,
                    "manifest_sha256": args.expected_current_manifest_sha256,
                },
                restore=True,
            )
            print(json.dumps(pointer, sort_keys=True))
            return 0
        if args.command == "adopt":
            from observatorio_secop.processing.silver.selection import adopt

            record = adopt(
                storage,
                args.version,
                args.manifest_sha256,
                PopulationScope(args.signing_from, args.signing_to),
                bronze_root=config.bronze_root,
            )
            print(json.dumps(record, sort_keys=True))
            return 0
        if args.command == "run-population":
            manifest = SilverPipeline(config, config_path=args.config).run_population(
                PopulationScope(args.signing_from, args.signing_to),
                tuple(args.campaign_run_id),
                baseline_version_id=args.baseline_version,
                baseline_manifest_sha256=args.baseline_manifest_sha256,
                mode=args.mode,
            )
            print(manifest_summary(manifest))
            return 0 if manifest.critical_passed else 3
        manifest = SilverPipeline(config, config_path=args.config).run(tuple(args.run_id))
        print(manifest_summary(manifest))
        return 0 if manifest.critical_passed else 3
    except (IngestionError, SilverError, SilverBundleError, OSError, ValueError) as error:
        print(f"Silver processing failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
