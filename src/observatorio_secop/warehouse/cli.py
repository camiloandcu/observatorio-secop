"""CLI for verified Silver publication into local PostgreSQL."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from observatorio_secop.warehouse.bundle import verify_current_bundle
from observatorio_secop.warehouse.config import load_warehouse_config
from observatorio_secop.warehouse.errors import WarehouseError
from observatorio_secop.warehouse.loader import WarehouseLoader

DEFAULT_CONFIG = Path("config/warehouse.yaml")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Load a verified Silver snapshot into PostgreSQL")
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subcommands = result.add_subparsers(dest="command", required=True)
    subcommands.add_parser("verify", help="Verify the current Silver bundle without a database")
    subcommands.add_parser("load", help="Verify and transactionally load the current Silver bundle")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = load_warehouse_config(args.config)
        bundle = verify_current_bundle(config.silver_root)
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
        result = WarehouseLoader(config).load(bundle)
        print(json.dumps(result.as_dict(), sort_keys=True))
        return 0
    except (WarehouseError, OSError, ValueError) as error:
        print(f"Warehouse load failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
