"""Check the local Airflow job heartbeat without importing or spawning Airflow."""

from __future__ import annotations

import argparse
import os
import socket
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote, urlsplit

import psycopg
from psycopg import sql

JOB_TYPES = ("SchedulerJob", "DagProcessorJob")


def connection_options(environment: Mapping[str, str]) -> dict[str, Any]:
    """Credentials remain in memory; diagnostics never serialize this mapping."""
    value = environment.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "").strip()
    parsed = urlsplit(value)
    if parsed.scheme not in ("postgresql", "postgresql+psycopg", "postgresql+psycopg2"):
        raise ValueError("Unsupported metadata connection")
    if not parsed.hostname or not parsed.username or not parsed.path.strip("/"):
        raise ValueError("Incomplete metadata connection")
    if parsed.query or parsed.fragment:
        raise ValueError("Unsupported metadata connection options")
    return {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "user": unquote(parsed.username),
        "password": unquote(parsed.password or ""),
        "dbname": unquote(parsed.path.lstrip("/")),
        "connect_timeout": 2,
        "options": "-c statement_timeout=2000 -c lock_timeout=1000",
    }


def healthy(
    job_type: str,
    *,
    environment: Mapping[str, str] | None = None,
    hostname: str | None = None,
    max_age: int = 30,
) -> bool:
    if job_type not in JOB_TYPES or type(max_age) is not int or not 1 <= max_age <= 300:
        raise ValueError("Invalid heartbeat check")
    env = os.environ if environment is None else environment
    schema = env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_SCHEMA") or "public"
    query = sql.SQL(
        "SELECT EXISTS (SELECT 1 FROM {} WHERE job_type=%s AND hostname=%s "
        "AND state='running' AND latest_heartbeat > "
        "clock_timestamp() - make_interval(secs => %s))"
    ).format(sql.Identifier(schema, "job"))
    with psycopg.connect(**connection_options(env)) as database:
        row = database.execute(
            query, (job_type, hostname or socket.gethostname(), max_age)
        ).fetchone()
    return row == (True,)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-type", choices=JOB_TYPES, required=True)
    parser.add_argument("--max-age", type=int, default=30)
    args = parser.parse_args(argv)
    try:
        passed = healthy(args.job_type, max_age=args.max_age)
    except (ValueError, OSError, psycopg.Error):
        passed = False
    print("Airflow job healthy" if passed else "Airflow job unavailable or heartbeat stale")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
