"""Command line entry points for source profiling and validation."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from observatorio_secop.source_profile.client import SocrataClient
from observatorio_secop.source_profile.config import load_config
from observatorio_secop.source_profile.contract import (
    load_contract,
    validate_contract,
    validate_fixture,
)
from observatorio_secop.source_profile.errors import SourceProfileError
from observatorio_secop.source_profile.workflow import observe_source, write_artifacts

DEFAULT_CONFIG = Path("config/secop_source.yaml")
DEFAULT_CONTRACT = Path("contracts/secop_source.yaml")
DEFAULT_FIXTURE = Path("tests/fixtures/secop_contracts.json")
DEFAULT_REPORT = Path("docs/data-source-profile.md")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile and validate the SECOP II source")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subparsers = parser.add_subparsers(dest="command", required=True)

    live = subparsers.add_parser("live", help="observe the bounded live source")
    live.add_argument("--write", action="store_true", help="atomically replace reviewed artifacts")
    live.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    live.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    live.add_argument("--report", type=Path, default=DEFAULT_REPORT)

    validate = subparsers.add_parser("validate", help="validate frozen artifacts offline")
    validate.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    validate.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)

    check_live = subparsers.add_parser("check-live", help="check critical fields without writes")
    check_live.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "live":
            config = load_config(args.config)
            contract, fixture, report = observe_source(config, SocrataClient(config))
            if args.write:
                write_artifacts(
                    contract,
                    fixture,
                    report,
                    contract_path=args.contract,
                    fixture_path=args.fixture,
                    report_path=args.report,
                )
                print(
                    f"Wrote contract, {len(fixture)} safe fixture rows, and source profile "
                    f"using {contract['observation']['request_count']} bounded requests."
                )
            else:
                print(json.dumps(contract, ensure_ascii=False, indent=2))
            return 0

        contract = load_contract(args.contract)
        if args.command == "validate":
            fixture = json.loads(args.fixture.read_text(encoding="utf-8"))
            if not isinstance(fixture, list):
                raise SourceProfileError("Fixture must be a JSON array")
            validate_fixture(contract, fixture)
            print(f"Contract and {len(fixture)} fixture rows are valid offline.")
            return 0

        config = load_config(args.config)
        client = SocrataClient(config)
        metadata = client.fetch_metadata()
        critical = [column["field"] for column in contract["columns"] if column.get("critical")]
        observed_fields = {column.get("fieldName") for column in metadata["columns"]}
        missing = sorted(set(critical) - observed_fields)
        if missing:
            raise SourceProfileError(f"Live metadata is missing critical columns: {missing}")
        rows = client.query(
            **{
                "$select": ",".join(critical),
                "$where": contract["territory"]["department_predicate"],
                "$order": ",".join(contract["identity"]["fields"]),
                "$limit": min(10, config.limits.max_query_rows),
            }
        )
        additions = validate_contract(contract, rows)
        print(
            f"Live contract check passed for {len(rows)} bounded rows; "
            f"additive sample fields: {len(additions)}."
        )
        return 0
    except (SourceProfileError, OSError, json.JSONDecodeError, ValueError) as error:
        print(f"secop-source-profile failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
