"""Required real Airflow analytical replay on uniquely owned disposable services."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import runpy
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
STAGES = {"validate", "acquire", "silver", "warehouse", "candidate", "activate", "outcome"}
RECOVERY_STAGES = ("acquire", "silver", "warehouse", "candidate", "activate")
REQUIRED_CASES = {
    "cold_scheduler_replay",
    "committed_stage_retries",
    "rejected_publication",
    "exact_restoration",
    "safe_operational_surfaces",
}
SERVICES = ("postgres", "airflow-api-server", "airflow-scheduler", "airflow-dag-processor")
E2E_MIN_AVAILABLE_MIB = 2048


class GateError(RuntimeError):
    """Safe fixed diagnostic; raw subprocess/API errors never leave this boundary."""


def free_port() -> int:
    with socket.socket() as stream:
        stream.bind(("127.0.0.1", 0))
        return int(stream.getsockname()[1])


def require_cases(cases: dict[str, Any]) -> None:
    if set(cases) != REQUIRED_CASES or any(v.get("status") != "passed" for v in cases.values()):
        raise GateError("PIPELINE_E2E_REQUIRED_CASE_MISSING_OR_FAILED")


def mark_operator_interrupt(report: dict[str, Any]) -> None:
    """Give Ctrl-C a stable safe failure code while preserving finally cleanup."""
    report["passed"] = False
    report["stage"] = "interrupted"
    report["failure_code"] = "PIPELINE_E2E_INTERRUPTED_BY_OPERATOR"


def require_tasks(rows: list[dict[str, Any]], failed: str | None = None) -> None:
    tasks = {row["task_id"]: row.get("state") for row in rows}
    if len(rows) != len(STAGES) or set(tasks) != STAGES:
        raise GateError("PIPELINE_E2E_TASK_INVENTORY")
    for stage, state in tasks.items():
        if failed is None and state != "success":
            raise GateError("PIPELINE_E2E_TASK_NOT_EXECUTED")
        if failed is not None:
            prior = (
                "validate",
                "acquire",
                "silver",
                "warehouse",
                "candidate",
                "activate",
                "outcome",
            )
            expected = "success" if prior.index(stage) < prior.index(failed) else "upstream_failed"
            if state != ("failed" if stage == failed else expected):
                raise GateError("PIPELINE_E2E_REJECTION_TASK_STATE")


def parameters(fixture: str, baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "mode": "demo",
        "fixture_id": fixture,
        "signing_from": "2026-01-01",
        "signing_to": "2026-02-01",
        "baseline_version": baseline["silver_version_id"] if baseline else None,
        "baseline_sha256": baseline["manifest_sha256"] if baseline else None,
        "total_rows": 100,
        "total_requests": 30,
        "grant_rows": 100,
        "grant_requests": 30,
        "page_size": 2,
        "build_seconds": 600,
        "storage_bytes": 134217728,
    }


class Probe:
    def __init__(self, root: Path, deadline_seconds: int, private_root: Path):
        self.project = "secop-e2e-" + secrets.token_hex(6)
        self.root = root
        self.private = private_root / self.project
        self.private.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.diagnostic_count = 0
        self.deadline = time.monotonic() + deadline_seconds
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("AIRFLOW_", "POSTGRES_", "COMPOSE_", "SECOP_", "SOCRATA_"))
        }
        self.api = "http://127.0.0.1:" + str(free_port())
        overrides = {
            "COMPOSE_PROJECT_NAME": self.project,
            "POSTGRES_USER": "probe",
            "POSTGRES_PASSWORD": secrets.token_hex(24),
            "POSTGRES_DB": "secop_e2e",
            "POSTGRES_PORT": str(free_port()),
            "AIRFLOW_PORT": self.api.rsplit(":", 1)[1],
            "AIRFLOW_UID": "50000",
            "SECOP_RUNTIME_MODE": "demo",
            "SOCRATA_APP_TOKEN": "",
        }
        env_file = root / "runtime.env"
        lines = []
        for line in (ROOT / ".env.example").read_text().splitlines():
            key = line.split("=", 1)[0]
            lines.append(f"{key}={overrides[key]}" if key in overrides else line)
        env_file.write_text("\n".join(lines) + "\n")
        env_file.chmod(0o600)
        override = root / "scheduler.json"
        override.write_text(
            json.dumps(
                {
                    "services": {
                        # Keep the disposable proof below a conservative 2.5 GiB
                        # aggregate container ceiling on a typical WSL2 VM.
                        "postgres": {
                            "mem_limit": "256m",
                            "memswap_limit": "256m",
                            "pids_limit": 128,
                        },
                        "airflow-api-server": {
                            "mem_limit": "512m",
                            "memswap_limit": "512m",
                            "pids_limit": 192,
                        },
                        "airflow-scheduler": {
                            "mem_limit": "1536m",
                            "memswap_limit": "1536m",
                            "pids_limit": 384,
                            "environment": {"SECOP_PIPELINE_FAULT": "${SECOP_E2E_FAULT:-}"},
                        },
                        "airflow-dag-processor": {
                            "mem_limit": "256m",
                            "memswap_limit": "256m",
                            "pids_limit": 128,
                        },
                    }
                }
            )
        )
        self.compose = [
            "docker",
            "compose",
            "--project-name",
            self.project,
            "--env-file",
            str(env_file),
            "--project-directory",
            str(ROOT),
            "-f",
            str(ROOT / "docker-compose.yml"),
            "-f",
            str(override),
            "--profile",
            "airflow",
        ]
        self.token: str | None = None
        self.task_outcomes: dict[str, list[dict[str, Any]]] = {}

    def save_diagnostic(self, label: str, content: str) -> None:
        self.diagnostic_count += 1
        path = self.private / f"{self.diagnostic_count:04d}-{label}.log"
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(content[:1048576])

    def diagnostics(self) -> None:
        try:
            code = """
from pathlib import Path
root=Path('/opt/secop-reports/private-task-logs')
for p in sorted(root.glob('*/*.log'))[-30:]:
    if p.is_file() and p.resolve().is_relative_to(root.resolve()):
        print(p.name)
        print(p.read_text(errors='replace')[-8192:])
"""
            result = subprocess.run(
                self.compose + ["exec", "-T", "airflow-api-server", "python", "-c", code],
                env=self.environment,
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            self.save_diagnostic("task-producers", result.stdout + "\n" + result.stderr)
        except Exception:
            pass

        # Private logs remain local; CI uploads only the allowlisted summary.
        for service in ("airflow-init", *SERVICES):
            try:
                result = subprocess.run(
                    self.compose + ["logs", "--no-color", "--tail", "100", service],
                    env=self.environment,
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
                self.save_diagnostic(service, result.stdout + "\n" + result.stderr)
            except Exception:
                pass  # Diagnostics cannot replace the original failure or skip cleanup.

    def run(self, args: list[str], *, input_text: str | None = None, timeout: int = 180) -> str:
        if time.monotonic() >= self.deadline:
            raise GateError("PIPELINE_E2E_DEADLINE")
        try:
            result = subprocess.run(
                args,
                env=self.environment,
                input=input_text,
                capture_output=True,
                text=True,
                timeout=min(timeout, max(1, int(self.deadline - time.monotonic()))),
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise GateError("PIPELINE_E2E_COMMAND_TIMEOUT_OR_UNAVAILABLE") from None
        if result.returncode:
            self.save_diagnostic("command-failed", result.stdout + "\n" + result.stderr)
            raise GateError("PIPELINE_E2E_COMMAND_FAILED")
        return result.stdout.strip()

    def command(self, args: list[str], **kwargs: Any) -> str:
        return self.run(self.compose + args, **kwargs)

    def api_request(self, path: str, payload: Any = None, method: str | None = None) -> Any:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(
            self.api + path,
            data=json.dumps(payload).encode() if payload is not None else None,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            self.save_diagnostic("api-failed", error.read(1048576).decode(errors="replace"))
            raise GateError("PIPELINE_E2E_API_HTTP_" + str(error.code)) from None
        except (urllib.error.URLError, ValueError):
            raise GateError("PIPELINE_E2E_API_REQUEST_FAILED") from None

    def healthy(self) -> None:
        end = min(self.deadline, time.monotonic() + 300)
        while time.monotonic() < end:
            statuses = []
            for service in SERVICES:
                identity = self.command(["ps", "-q", service])
                if identity:
                    statuses.append(
                        self.run(
                            ["docker", "inspect", "--format", "{{.State.Health.Status}}", identity]
                        )
                    )
            if len(statuses) == len(SERVICES) and all(s == "healthy" for s in statuses):
                return
            if "unhealthy" in statuses:
                raise GateError("PIPELINE_E2E_SERVICE_UNHEALTHY")
            time.sleep(2)
        raise GateError("PIPELINE_E2E_SERVICE_TIMEOUT")

    def start(self, build: bool) -> None:
        self.command(["config", "--quiet"])
        if build:
            self.command(["build", "airflow-api-server"], timeout=1200)
        self.command(["up", "-d", *SERVICES[1:]], timeout=300)
        self.healthy()
        credentials = json.loads(
            self.command(
                [
                    "exec",
                    "-T",
                    "airflow-api-server",
                    "cat",
                    "/opt/airflow/local-state/passwords.json",
                ]
            )
        )
        self.token = self.api_request(
            "/auth/token", {"username": "admin", "password": credentials["admin"]}
        )["access_token"]
        self.command(
            [
                "exec",
                "-T",
                "airflow-api-server",
                "airflow",
                "pools",
                "set",
                "secop_writer",
                "1",
                "Bounded analytical writer",
            ]
        )
        end = min(self.deadline, time.monotonic() + 180)
        while time.monotonic() < end:
            try:
                self.api_request("/api/v2/dags/secop_pipeline", {"is_paused": False}, "PATCH")
                return
            except GateError:
                time.sleep(2)
        raise GateError("PIPELINE_E2E_DAG_NOT_DISCOVERED")

    def fault(self, value: str) -> None:
        self.environment["SECOP_E2E_FAULT"] = value
        self.command(["up", "-d", "--no-deps", "--force-recreate", "airflow-scheduler"])
        self.healthy()

    def trigger(self, label: str, conf: dict[str, Any], *, failed: bool = False) -> str:
        run_id = "e2e-" + label + "-" + secrets.token_hex(6)
        self.api_request(
            "/api/v2/dags/secop_pipeline/dagRuns",
            {"dag_run_id": run_id, "logical_date": datetime.now(UTC).isoformat(), "conf": conf},
        )
        end = min(self.deadline, time.monotonic() + 1200)
        while time.monotonic() < end:
            run = self.api_request("/api/v2/dags/secop_pipeline/dagRuns/" + run_id)
            if run["state"] in ("success", "failed"):
                if run["state"] != ("failed" if failed else "success"):
                    raise GateError("PIPELINE_E2E_UNEXPECTED_RUN_STATE")
                rows = self.api_request(
                    "/api/v2/dags/secop_pipeline/dagRuns/" + run_id + "/taskInstances"
                )["task_instances"]
                require_tasks(rows, "candidate" if failed else None)
                self.task_outcomes[run_id] = [
                    {key: row.get(key) for key in ("task_id", "state", "try_number", "duration")}
                    for row in rows
                ]
                return run_id
            time.sleep(2)
        raise GateError("PIPELINE_E2E_DAGRUN_TIMEOUT")

    def receipt(self, run_id: str) -> dict[str, Any]:
        code = """
import json,sys
from observatorio_secop.orchestration.models import Runtime,execution_id
from observatorio_secop.orchestration.receipts import Receipts,public_summary
r=Runtime.environment()
d=Receipts(r.state,execution_id('secop_pipeline',sys.argv[1])).read()
p=r.bronze/'runs'/d['campaign_id']/'manifest.json'
m=json.loads(p.read_text())
s=public_summary(d)
s['invocations']=len(m['population']['invocations'])
s['acquisition_rows']=m['counts']['written']
print(json.dumps(s))
"""
        return json.loads(
            self.command(["exec", "-T", "airflow-api-server", "python", "-c", code, run_id])
        )

    def discard_rejected(self, publication_id: str) -> None:
        code = """
import json,sys
from observatorio_secop.orchestration.models import Runtime
from observatorio_secop.orchestration.operations import warehouse_config
from observatorio_secop.publication.store import PublicationStore
store=PublicationStore(warehouse_config(Runtime.environment()))
row=store.inspect(sys.argv[1])
if row['state'] != 'failed' or row['ever_activated']:
    raise SystemExit(2)
store.discard(sys.argv[1])
print(json.dumps({'state':store.inspect(sys.argv[1])['state']}))
"""
        result = json.loads(
            self.command(["exec", "-T", "airflow-api-server", "python", "-c", code, publication_id])
        )
        if result != {"state": "discarded"}:
            raise GateError("PIPELINE_E2E_CANDIDATE_DISCARD_FAILED")

    def restore(
        self, target: dict[str, Any], current: dict[str, Any], expected_publication: dict[str, Any]
    ) -> dict[str, Any]:
        code = """
import json,sys
from observatorio_secop.orchestration.models import Runtime
from observatorio_secop.orchestration.operations import warehouse_config
from observatorio_secop.processing.silver.storage import SilverStorage
from observatorio_secop.publication.store import PublicationStore
from observatorio_secop.warehouse.bundle import verify_manifest_bundle
from observatorio_secop.warehouse.loader import WarehouseLoader
p=json.loads(sys.argv[1]); r=Runtime.environment(); config=warehouse_config(r)
target=p['target']['stages']['silver']; current=p['current']['stages']['silver']
keys=('silver_version_id','manifest_sha256')
expected={key:current[key] for key in keys}
bundle=verify_manifest_bundle(r.silver/'versions'/target['silver_version_id']/'manifest.json',
                             expected_hash=target['manifest_sha256'])
pointer=SilverStorage(r.silver).rollback(bundle.version_id,
    manifest_sha256=bundle.manifest_sha256,expected_current=expected,restore=True)
loaded=WarehouseLoader(config).load(bundle,restore=True,
    expected_current=(current['silver_version_id'],current['manifest_sha256']))
publication=p['expected_publication']
activated=PublicationStore(config).activate(p['target']['publication_id'],
    publication['publication_id'],publication['generation'],restore=True)
print(json.dumps({'silver':{key:pointer[key] for key in keys},'warehouse':loaded.as_dict(),
                  'publication':{'status':activated['status'],'generation':activated['generation']}}))
"""
        payload = json.dumps(
            {"target": target, "current": current, "expected_publication": expected_publication}
        )
        return json.loads(
            self.command(["exec", "-T", "airflow-api-server", "python", "-c", code, payload])
        )

    def audit(self, runs: list[str], publication_id: str) -> dict[str, Any]:
        code = runpy.run_path(str(ROOT / "scripts/pipeline_e2e_checks.py"))["AUDIT_CODE"]
        payload = json.dumps({"runs": runs, "publication_id": publication_id})
        return json.loads(
            self.command(["exec", "-T", "airflow-api-server", "python", "-c", code, payload])
        )

    def snapshot(self) -> dict[str, Any]:
        query = """SELECT json_build_object(
'rows',(SELECT count(*) FROM published.fact_contract),
'total',(SELECT sum(contract_value_cop)::text FROM published.fact_contract),
'digest',(SELECT md5(string_agg(to_jsonb(f)::text,',' ORDER BY contract_key))
 FROM published.fact_contract f),
'metadata_digest',(SELECT md5(string_agg(to_jsonb(m)::text,',' ORDER BY to_jsonb(m)::text))
 FROM published.publication_metadata m),
'quality_digest',(SELECT md5(string_agg(to_jsonb(q)::text,',' ORDER BY to_jsonb(q)::text))
 FROM published.mart_data_quality q),
'view_definitions',(SELECT md5(string_agg(pg_get_viewdef(c.oid,true),',' ORDER BY c.relname))
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='published' AND c.relkind='v'),
'publication_id',a.publication_id,'generation',a.generation,
'events',(SELECT count(*) FROM publication_control.activations))
FROM publication_control.active a;"""
        return json.loads(
            self.command(
                ["exec", "-T", "postgres", "psql", "-U", "probe", "-d", "secop_e2e", "-Atq"],
                input_text=query,
            )
        )

    def identity(self) -> dict[str, Any]:
        code = """
import importlib.metadata as m,json
from observatorio_secop.orchestration.fixtures import FIXTURE_IDS,fixture_expected
from observatorio_secop.publication.build import _implementation_digest,ROOT
print(json.dumps({'versions':{n:m.version(n) for n in
('apache-airflow','pyspark','dbt-core','psycopg')},
'fixture_sha256':{n:fixture_expected(n).sha256 for n in FIXTURE_IDS},
'implementation_sha256':_implementation_digest(ROOT)}))
"""
        result = json.loads(
            self.command(["exec", "-T", "airflow-api-server", "python", "-c", code])
        )
        container = self.command(["ps", "-q", "airflow-api-server"])
        result["image_id"] = self.run(["docker", "inspect", "--format", "{{.Image}}", container])
        expected = implementation_digest(ROOT)
        if result["implementation_sha256"] != expected:
            raise GateError("PIPELINE_E2E_STALE_RUNTIME_IMAGE")
        return result

    def resources(self) -> dict[str, Any]:
        identities = self.command(["ps", "-q", *SERVICES]).splitlines()
        output = self.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *identities])
        samples = [json.loads(line) for line in output.splitlines() if line.strip()]
        database_bytes = self.command(
            ["exec", "-T", "postgres", "psql", "-U", "probe", "-d", "secop_e2e", "-Atq"],
            input_text="SELECT pg_database_size(current_database());",
        )
        return {
            "services": [
                {k: item[k] for k in ("Name", "CPUPerc", "MemUsage", "BlockIO", "PIDs")}
                for item in samples
            ],
            "database_bytes": int(database_bytes),
            "method": (
                "post-scenario docker stats snapshot and pg_database_size; "
                "not peak memory or total volume/WAL usage"
            ),
        }

    def cleanup(self) -> bool:
        # Cleanup must still run after the proof deadline expires.
        self.deadline = max(self.deadline, time.monotonic() + 180)
        self.command(["down", "--volumes", "--remove-orphans"], timeout=120)
        selector = "label=com.docker.compose.project=" + self.project
        containers = self.run(["docker", "ps", "-aq", "--filter", selector])
        volumes = self.run(["docker", "volume", "ls", "-q", "--filter", selector])
        return not containers and not volumes


def require_retries(receipt: dict[str, Any]) -> None:
    if (
        any(
            receipt.get("stages", {}).get(stage, {}).get("attempt") != 2
            or receipt["stages"][stage].get("status") != "success"
            for stage in RECOVERY_STAGES
        )
        or receipt.get("invocations") != 1
        or receipt.get("acquisition_rows") != 5
    ):
        raise GateError("PIPELINE_E2E_FIXED_INPUT_RETRY_FAILED")


def require_restoration(
    original: dict[str, Any], before: dict[str, Any], restored: dict[str, Any]
) -> None:
    stable = set(original) - {"generation", "events"}
    if (
        any(restored[key] != original[key] for key in stable)
        or restored["generation"] != before["generation"] + 1
        or restored["events"] != before["events"] + 1
    ):
        raise GateError("PIPELINE_E2E_RESTORATION_MISMATCH")


def require_safe_surfaces(audit: dict[str, Any]) -> None:
    if (
        audit.get("airflow_logs_secret_free") is not True
        or audit.get("xcom_execution_ids_only") is not True
        or audit.get("xcom_rows_checked") != 18
        or audit.get("log_files_checked", 0) <= 0
        or len(audit.get("reader_acl_checks", [])) != 2
        or any(
            item.get("denied_operations") != 4
            or item.get("published_reads") != 12
            or item.get("owner_membership_denied") is not True
            for item in audit.get("reader_acl_checks", [])
        )
    ):
        raise GateError("PIPELINE_E2E_SAFE_SURFACE_CHECK_FAILED")


def implementation_digest(root: Path) -> str:
    """Match the installed publication recipe and reject a stale local image."""
    checksum = hashlib.sha256()
    for path in sorted((root / "src/observatorio_secop").rglob("*.py")):
        checksum.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes())
    for path in (root / "uv.lock", root / "pyproject.toml", root / "scripts/gold_dbt.py"):
        checksum.update(path.name.encode() + b"\0" + path.read_bytes())
    return checksum.hexdigest()


def available_memory_mib(meminfo: Path = Path("/proc/meminfo")) -> int:
    """Read Linux's reclaimable-memory estimate; fail closed on unknown platforms."""
    try:
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) // 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def probe(summary: Path, *, build: bool = False, timeout: int = 2400) -> dict[str, Any]:
    report: dict[str, Any] = {"passed": False, "cleanup_verified": False, "cases": {}}
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="secop-e2e-") as directory:
        runtime = Probe(Path(directory), timeout, summary.parent / "private")
        report["project"] = runtime.project
        try:
            report["stage"] = "startup"
            available_mib = available_memory_mib()
            report["resource_preflight"] = {
                "available_memory_mib": available_mib,
                "required_memory_mib": E2E_MIN_AVAILABLE_MIB,
                "status": "passed" if available_mib >= E2E_MIN_AVAILABLE_MIB else "failed",
            }
            if available_mib < E2E_MIN_AVAILABLE_MIB:
                raise GateError("PIPELINE_E2E_INSUFFICIENT_AVAILABLE_MEMORY")
            runtime.start(build)
            report["runtime_identity"] = runtime.identity()
            report["resource_samples"] = []
            report["stage"] = "cold_scheduler_replay"
            case_started = time.monotonic()
            run_a = runtime.trigger("cold-a", parameters("demo-a"))
            a = runtime.receipt(run_a)
            snapshot_a = runtime.snapshot()
            if (snapshot_a["rows"], snapshot_a["total"]) != (2, "300.00") or snapshot_a[
                "publication_id"
            ] != a["publication_id"]:
                raise GateError("PIPELINE_E2E_KNOWN_RESULT_MISMATCH")
            report["cases"]["cold_scheduler_replay"] = {
                "status": "passed",
                "rows": 2,
                "total_cop": "300.00",
                "publication_id": a["publication_id"],
                "tasks_executed": len(STAGES),
                "task_outcomes": runtime.task_outcomes[run_a],
                "elapsed_seconds": round(time.monotonic() - case_started, 2),
            }
            report["stage"] = "committed_stage_retries"
            report["resource_samples"].append(runtime.resources())
            runtime.fault(",".join(stage + ":after_effect" for stage in RECOVERY_STAGES))
            case_started = time.monotonic()
            run_b = runtime.trigger("retry-b", parameters("demo-b", a["stages"]["silver"]))
            b = runtime.receipt(run_b)
            snapshot_b = runtime.snapshot()
            require_retries(b)
            if (
                (snapshot_b["rows"], snapshot_b["total"]) != (3, "400.00")
                or snapshot_b["publication_id"] != b["publication_id"]
                or snapshot_b["events"] != snapshot_a["events"] + 1
            ):
                raise GateError("PIPELINE_E2E_FIXED_INPUT_RETRY_FAILED")
            report["cases"]["committed_stage_retries"] = {
                "status": "passed",
                "attempts_by_stage": {
                    stage: b["stages"][stage]["attempt"] for stage in RECOVERY_STAGES
                },
                "task_outcomes": runtime.task_outcomes[run_b],
                "elapsed_seconds": round(time.monotonic() - case_started, 2),
                "invocation_grants": 1,
                "publication_id": b["publication_id"],
                "rows": 3,
                "total_cop": "400.00",
            }
            report["stage"] = "rejected_publication"
            report["resource_samples"].append(runtime.resources())
            runtime.fault("")
            rejected = parameters("demo-b", b["stages"]["silver"])
            rejected["storage_bytes"] = 1
            case_started = time.monotonic()
            run_c = runtime.trigger("rejected-c", rejected, failed=True)
            c = runtime.receipt(run_c)
            if runtime.snapshot() != snapshot_b or c["stages"]["candidate"]["status"] != "failed":
                raise GateError("PIPELINE_E2E_PRIOR_PUBLICATION_CHANGED")
            report["cases"]["rejected_publication"] = {
                "status": "passed",
                "candidate_status": "failed",
                "task_outcomes": runtime.task_outcomes[run_c],
                "elapsed_seconds": round(time.monotonic() - case_started, 2),
                "prior_publication_preserved": b["publication_id"],
                "history_unchanged": True,
            }
            report["resource_samples"].append(runtime.resources())
            runtime.discard_rejected(c["publication_id"])
            if runtime.snapshot() != snapshot_b:
                raise GateError("PIPELINE_E2E_DISCARD_CHANGED_PUBLICATION")
            report["cases"]["rejected_publication"]["explicit_discard_verified"] = True
            report["stage"] = "exact_restoration"
            case_started = time.monotonic()
            restored = runtime.restore(a, c, snapshot_b)
            snapshot_restored = runtime.snapshot()
            require_restoration(snapshot_a, snapshot_b, snapshot_restored)
            repeated = runtime.restore(a, a, snapshot_restored)
            if (
                repeated["warehouse"]["status"] != "noop"
                or repeated["publication"]["status"] != "noop"
            ):
                raise GateError("PIPELINE_E2E_RESTORATION_NOT_NOOP")
            if runtime.snapshot() != snapshot_restored:
                raise GateError("PIPELINE_E2E_REPEATED_RESTORE_CHANGED_HISTORY")
            report["cases"]["exact_restoration"] = {
                "status": "passed",
                "publication_id": a["publication_id"],
                "rows": snapshot_restored["rows"],
                "total_cop": snapshot_restored["total"],
                "elapsed_seconds": round(time.monotonic() - case_started, 2),
                "silver": restored["silver"],
                "warehouse_status": restored["warehouse"]["status"],
                "new_activation_events": 1,
                "repeated_restore_noop": True,
                "metadata_and_quality_exact": True,
            }
            report["resource_samples"].append(runtime.resources())
            report["stage"] = "safe_operational_surfaces"
            audit = runtime.audit([run_a, run_b, run_c], a["publication_id"])
            require_safe_surfaces(audit)
            report["cases"]["safe_operational_surfaces"] = {"status": "passed", **audit}
            require_cases(report["cases"])
            report["passed"] = True
            report["stage"] = "complete"
        except KeyboardInterrupt:
            mark_operator_interrupt(report)
            report["private_diagnostics_retained"] = False
        except Exception as error:
            runtime.diagnostics()
            report["private_diagnostics_retained"] = True
            report["failure_code"] = (
                str(error) if isinstance(error, GateError) else "PIPELINE_E2E_FAILED"
            )
        finally:
            try:
                report["cleanup_verified"] = runtime.cleanup()
            except Exception:
                report["cleanup_verified"] = False
            report["passed"] = report["passed"] and report["cleanup_verified"]
            report["elapsed_seconds"] = round(time.monotonic() - started, 2)
            report["source_revision"] = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
            ).stdout.strip()
            report["lock_sha256"] = hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest()
            report["runtime_scope"] = (
                "actual scheduler/Execution API; synthetic transport; real Spark/PostgreSQL/dbt"
            )
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--timeout", type=int, default=2400)
    args = parser.parse_args()
    if not 1 <= args.timeout <= 7200:
        parser.error("timeout must be finite and between 1 and 7200 seconds")
    report = probe(args.summary, build=args.build, timeout=args.timeout)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
