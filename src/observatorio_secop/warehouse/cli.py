"""CLI for pinned verified Silver loading and explicit population recovery."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from observatorio_secop.warehouse.bundle import (
    VerifiedSilverBundle,
    verify_current_bundle,
    verify_manifest_bundle,
)
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import WarehouseError
from observatorio_secop.warehouse.loader import WarehouseLoader

DEFAULT_CONFIG = Path("config/warehouse.yaml")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Verify and load an immutable Silver version")
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subcommands = result.add_subparsers(dest="command", required=True)
    for command, help_text in (
        ("verify", "Verify a pinned bundle without a database"),
        ("load", "Transactionally load a compatible population"),
        ("restore", "Restore a registered version with exact expected-current identity"),
        ("adopt", "Verify and bind the active legacy snapshot without claiming coverage"),
    ):
        action = subcommands.add_parser(command, help=help_text)
        action.add_argument("--version")
        action.add_argument("--manifest-sha256")
        if command == "restore":
            action.add_argument("--expected-current-version", required=True)
            action.add_argument("--expected-current-hash", required=True)
        if command == "adopt":
            action.add_argument("--bronze-root", type=Path, required=True)
    return result


def _bundle(root: Path, version: str | None, checksum: str | None) -> VerifiedSilverBundle:
    if (version is None) != (checksum is None):
        raise ValueError("Pinned selection requires version and manifest hash together")
    if version is None:
        return verify_current_bundle(root)
    if Path(version).name != version or version in (".", ".."):
        raise ValueError("Version must name one contained immutable bundle")
    path = root / "versions" / version / "manifest.json"
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Pinned version escapes Silver root")
    return verify_manifest_bundle(path, expected_hash=checksum)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_warehouse_config(args.config)
        if args.command in ("restore", "adopt") and args.version is None:
            raise ValueError("Recovery requires an explicit pinned target version and hash")
        bundle = _bundle(config.silver_root, args.version, args.manifest_sha256)
        if args.command == "verify":
            print(
                json.dumps(
                    {
                        "silver_version_id": bundle.version_id,
                        "manifest_sha256": bundle.manifest_sha256,
                        "rows": bundle.row_count,
                        "status": "verified",
                    },
                    sort_keys=True,
                )
            )
            return 0
        loader = WarehouseLoader(config)
        if args.command == "adopt":
            from observatorio_secop.processing.silver.selection import adoption_record

            record = adoption_record(config.silver_root, bundle.version_id, bundle.manifest_sha256)
            if record is None:
                raise ValueError("First create and verify the matching Silver legacy adoption")
            outcome = loader.adopt(bundle, record, bronze_root=args.bronze_root)
        elif args.command == "restore":
            outcome = loader.load(
                bundle,
                restore=True,
                expected_current=(
                    args.expected_current_version,
                    args.expected_current_hash,
                ),
            ).as_dict()
        else:
            outcome = loader.load(bundle).as_dict()
        print(json.dumps(outcome, sort_keys=True))
        return 0
    except (WarehouseError, OSError, ValueError) as error:
        print(f"Warehouse load failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
