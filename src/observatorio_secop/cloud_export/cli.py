"""Local build/verify commands. This entrypoint has no AWS operations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from observatorio_secop.publication.errors import PublicationError
from observatorio_secop.warehouse.errors import WarehouseError

from .build import build, supervised_verify
from .errors import ExportError
from .package import sha256


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Build or verify a bounded private local export")
    commands = result.add_subparsers(dest="command", required=True)
    action = commands.add_parser("build")
    action.add_argument("--config", type=Path, default=Path("config/warehouse.yaml"))
    action.add_argument("--publication-id", required=True)
    action.add_argument("--silver-root", type=Path, required=True)
    action.add_argument("--output", type=Path, required=True)
    action.add_argument(
        "--label-null-approval",
        type=Path,
        help="Private approval for exactly three pinned supplier-name cells",
    )
    action.add_argument(
        "--export-id", help="Optional assertion of the computed canonical export identity"
    )
    action = commands.add_parser("verify")
    action.add_argument("--package", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "build":
            result = build(
                config_path=args.config,
                silver_root=args.silver_root,
                output=args.output,
                publication_id=args.publication_id,
                export_id=args.export_id,
                label_null_approval=args.label_null_approval,
            )
        else:
            manifest = supervised_verify(args.package)
            result = {
                "status": "verified",
                "export_id": manifest["export_id"],
                "manifest_sha256": sha256(args.package / "manifest.json"),
            }
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ExportError, PublicationError, WarehouseError) as error:
        print(
            json.dumps(
                {"status": "failed", "code": getattr(error, "code", "EXPORT_INPUT_CONFLICT")}
            ),
            file=sys.stderr,
        )
        return 2
    except (OSError, ValueError, KeyError, TypeError):
        print('{"status":"failed","code":"EXPORT_INCOMPLETE"}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
