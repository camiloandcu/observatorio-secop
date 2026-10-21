"""Serial, disposable-only integration entrypoint and sanitized resource evidence."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ci_gate import GateError, load_report, reconcile  # noqa: E402


class PreflightError(ValueError):
    """Runtime is not an explicitly identified disposable target."""


def disposable_target(environment):
    from psycopg import Error
    from psycopg.conninfo import conninfo_to_dict

    try:
        dsn = environment["WAREHOUSE_TEST_DSN"]
        expected = {
            key: environment[f"CI_DISPOSABLE_DB_{name}"]
            for key, name in (("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"))
        }
        actual = conninfo_to_dict(dsn)
        if not expected["dbname"].startswith("secop_ci_"):
            raise ValueError
        if any(not value or actual.get(key) != value for key, value in expected.items()):
            raise ValueError
        # Multi-host and option overrides can silently redirect a nominal target.
        if any(key in actual for key in ("service", "hostaddr", "options")):
            raise ValueError
        if "," in expected["host"] or "," in expected["port"]:
            raise ValueError
        return dsn, expected
    except (KeyError, ValueError, Error) as error:
        raise PreflightError(
            "explicit disposable DSN/host/port/secop_ci_ database required"
        ) from error


def preflight():
    import psycopg

    dsn, target = disposable_target(os.environ)
    try:
        java = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
        if not re.search(r'version "17[.\"]', java.stderr + java.stdout):
            raise ValueError
        versions = {
            package: importlib.metadata.version(package)
            for package in ("pyspark", "dbt-core", "dbt-postgres", "psycopg")
        }
        if not shutil.which("dbt"):
            raise ValueError
        with psycopg.connect(dsn, connect_timeout=5) as connection:
            row = connection.execute(
                "select current_database(), current_setting('port'), version()"
            ).fetchone()
            if row[0] != target["dbname"] or str(connection.info.port) != target["port"]:
                raise ValueError
            versions["postgresql"] = connection.info.server_version
        versions["java_major"] = 17
        versions["python"] = sys.version.split()[0]
        return versions
    except Exception as error:
        # Never echo libpq exception text: it can disclose credentials/connection parameters.
        raise PreflightError(
            "Java 17, Spark, dbt or disposable PostgreSQL readiness failed"
        ) from error


def process_memory():
    """Linux system sample includes Python, JVM and postgres processes, not just child leader."""
    rss = dict.fromkeys(("python", "java", "postgres"), 0)
    for process in Path("/proc").glob("[0-9]*"):
        try:
            name = (process / "comm").read_text().strip()
            group = next((key for key in rss if name.startswith(key)), None)
            if group:
                status = (process / "status").read_text()
                rss[group] += int(re.search(r"VmRSS:\s+(\d+)", status)[1]) * 1024
        except (OSError, TypeError):
            continue
    return rss


def disk_bytes(path):
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def run(report_dir):
    import psycopg

    report_dir.mkdir(parents=True, exist_ok=True)
    report = report_dir / "outcomes.json"
    for name in ("outcomes.json", "summary.json", "resources.json"):
        (report_dir / name).unlink(missing_ok=True)
    started = time.monotonic()
    versions = preflight()
    preflight_seconds = time.monotonic() - started
    environment = os.environ.copy()
    environment["CI_PYTEST_REPORT"] = str(report.resolve())
    environment["PYTHONPATH"] = str(ROOT) + os.pathsep + environment.get("PYTHONPATH", "")
    environment["PYTEST_ADDOPTS"] = "-p scripts.ci_pytest"
    environment["MAKEFLAGS"] = "-j1"
    environment["MFLAGS"] = ""
    memory = dict.fromkeys(("python", "java", "postgres"), 0)
    database_peak = 0
    samples = 0
    process = subprocess.Popen(["make", "-j1", "quality"], cwd=ROOT, env=environment)
    measurement_failed = False
    while True:
        sample = process_memory()
        memory = {key: max(memory[key], value) for key, value in sample.items()}
        try:
            with psycopg.connect(environment["WAREHOUSE_TEST_DSN"], connect_timeout=3) as database:
                size = database.execute("select pg_database_size(current_database())").fetchone()[0]
                database_peak = max(database_peak, size)
        except Exception:
            measurement_failed = True
        samples += 1
        if process.poll() is not None:
            break
        time.sleep(1)
    try:
        summary = reconcile(load_report(report), load_report(ROOT / "config/ci_integration.json"))
    except GateError:
        summary = {"passed": False, "reason": "missing-or-malformed-execution-evidence"}
    summary.update(
        {
            "quality_exitstatus": process.returncode,
            "versions": versions,
            "command": ["make", "-j1", "quality"],
            "fixture_identity": "offline-synthetic-pytest-fixtures",
            "fixture_source_sha256": {
                str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((ROOT / "tests").rglob("*.py"))
                if any(part in {"warehouse", "processing_silver"} for part in path.parts)
            },
            "inventory_sha256": hashlib.sha256(
                (ROOT / "config/ci_integration.json").read_bytes()
            ).hexdigest(),
        }
    )
    summary["passed"] &= process.returncode == 0 and not measurement_failed
    summary["stage_seconds"] = {
        "preflight": preflight_seconds,
        "quality": time.monotonic() - started - preflight_seconds,
    }
    resources = {
        "total_seconds": time.monotonic() - started,
        "cpu_available": os.cpu_count(),
        "ram_available_bytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
        "disk_free_bytes": shutil.disk_usage(ROOT).free,
        "workspace_bytes": disk_bytes(ROOT),
        "peak_sampled_rss_bytes": memory,
        "database_peak_bytes": database_peak,
        "sample_interval_seconds": 1,
        "samples": samples,
        "measurement_failed": measurement_failed,
        "method": "Linux /proc host-wide process-name groups; pg_database_size dedicated DB",
        "limitations": "RSS may double-count shared pages; sampling misses short peaks; container "
        "postgres RSS visible only with shared host PID namespace; database bytes "
        "exclude WAL/container layers; workspace includes installed environment",
        "cache_state": "workflow caches disabled; OS/package state not reset by runner",
        "sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    }
    for name, payload in (("summary.json", summary), ("resources.json", resources)):
        (report_dir / name).write_text(json.dumps(payload, indent=2) + "\n")
    return process.returncode or (0 if summary["passed"] else 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "preflight", "gate"))
    parser.add_argument("--report-dir", type=Path, default=ROOT / "reports/ci")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "preflight":
            print(json.dumps(preflight(), sort_keys=True))
            return 0
        if args.command == "gate":
            summary = reconcile(
                load_report(args.report or args.report_dir / "outcomes.json"),
                load_report(ROOT / "config/ci_integration.json"),
            )
            print(json.dumps(summary, sort_keys=True))
            return 0 if summary["passed"] else 1
        return run(args.report_dir)
    except (PreflightError, GateError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
