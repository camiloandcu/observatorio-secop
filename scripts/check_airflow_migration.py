"""Prove Airflow migration, authenticated execution and backup recovery on owned Docker state.

Never accepts a database URL: every database and container is created by this probe.
Images must already be available locally; no working Compose resources are consulted.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

OLD_IMAGE = "apache/airflow:2.11.2-python3.12"
NEW_IMAGE = "apache/airflow:3.3.2-python3.12"


def docker(*args, input=None, timeout=240, redact=()):
    try:
        result = subprocess.run(
            ["docker", *args], input=input, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Docker command timed out") from None
    except OSError:
        raise RuntimeError("Docker command could not start") from None
    if result.returncode:
        # Neither output nor argv is safe to include in diagnostic exceptions.
        diagnostic = (result.stdout + result.stderr).decode(errors="replace")
        for secret in redact:
            diagnostic = diagnostic.replace(secret, "[redacted]")
        diagnostic = re.sub(r"(postgresql[^:]*://)[^\s/]+", r"\1[redacted]", diagnostic)
        suffix = ": " + diagnostic[-1800:] if redact else ""
        raise RuntimeError(f"Docker command {args[0]} failed (exit {result.returncode})" + suffix)
    return result.stdout


def cleanup(containers, network):
    """Delete only owned resources and positively verify their absence."""
    failed = False
    try:
        present = set(
            docker("ps", "-a", "--format", "{{.Names}}", timeout=30).decode().splitlines()
        )
        for name in reversed(containers):
            if name in present:
                try:
                    docker("rm", "-f", "-v", name, timeout=30)
                except RuntimeError:
                    failed = True
        networks = set(
            docker("network", "ls", "--format", "{{.Name}}", timeout=30).decode().splitlines()
        )
        if network in networks:
            try:
                docker("network", "rm", network, timeout=30)
            except RuntimeError:
                failed = True
        remaining = set(
            docker("ps", "-a", "--format", "{{.Names}}", timeout=30).decode().splitlines()
        )
        remaining_networks = set(
            docker("network", "ls", "--format", "{{.Name}}", timeout=30).decode().splitlines()
        )
        failed |= bool(remaining.intersection(containers)) or network in remaining_networks
    except RuntimeError:
        failed = True
    if failed:
        raise RuntimeError(
            "Owned Docker resource cleanup failed or could not be verified"
        ) from None


def wait_for(check, description, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(2)
    raise RuntimeError("Timed out waiting for " + description)


def run_probe(root):
    prefix = "secop-rem34-" + uuid.uuid4().hex[:10]
    containers = []
    password = secrets.token_urlsafe(24)
    jwt_secret = secrets.token_urlsafe(48)
    auth_password = secrets.token_urlsafe(24)
    network = prefix + "-net"
    pg = prefix + "-pg"
    api = prefix + "-api"
    (root / "dags").mkdir()
    (root / "state").mkdir()
    root.chmod(0o755)
    (root / "state").chmod(0o700)
    (root / "state/passwords.json").write_text(json.dumps({"admin": auth_password}))
    (root / "state/passwords.json").chmod(0o600)
    (root / "dags/probe.py").write_text(
        "from datetime import datetime, timezone\n"
        "from airflow.sdk import DAG, task\n"
        'with DAG("rem34_synthetic", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), '
        "schedule=None, catchup=False):\n"
        "    @task\n"
        "    def synthetic():\n"
        '        return "execution-api-roundtrip"\n'
        "    synthetic()\n"
    )

    def sql(query, database="postgres"):
        return (
            docker("exec", pg, "psql", "-U", "probe", "-d", database, "-At", "-c", query)
            .decode()
            .strip()
        )

    def airflow(image, database, *command, service=None):
        name = service or prefix + "-cli-" + uuid.uuid4().hex[:6]
        args = ["run", "--pull=never", "--name", name, "--network", network]
        if service:
            args += ["-d"]
        else:
            args += ["--rm"]
        containers.append(name)
        settings = {
            "AIRFLOW_HOME": "/probe/state",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": (
                f"postgresql+psycopg2://probe:{password}@{pg}:5432/{database}"
            ),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__CORE__DAGS_FOLDER": "/probe/dags",
            "AIRFLOW__CORE__FERNET_KEY": "",
        }
        if image == NEW_IMAGE:
            settings.update(
                {
                    "AIRFLOW__CORE__AUTH_MANAGER": (
                        "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
                    ),
                    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": "admin:admin",
                    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS": "False",
                    "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": (
                        "/probe/state/passwords.json"
                    ),
                    "AIRFLOW__API_AUTH__JWT_SECRET": jwt_secret,
                    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": f"http://{api}:8080/execution/",
                    "AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL": "1",
                    "AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL": "1",
                    "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "1",
                }
            )
        for key, value in settings.items():
            args += ["-e", key + "=" + value]
        args += ["-v", f"{root}:/probe", "--user", f"{os.getuid()}:0", image, "airflow", *command]
        print(f"Airflow {image.split(':')[1]}: {' '.join(command[:2])}", flush=True)
        return docker(*args, redact=(password, jwt_secret, auth_password))

    try:
        docker("network", "create", network)
        containers.append(pg)
        docker(
            "run",
            "--pull=never",
            "-d",
            "--name",
            pg,
            "--network",
            network,
            "-e",
            "POSTGRES_USER=probe",
            "-e",
            "POSTGRES_PASSWORD=" + password,
            "postgres:16.4-alpine",
        )

        def pg_ready():
            try:
                docker("exec", pg, "pg_isready", "-h", "127.0.0.1", "-U", "probe", timeout=15)
                return True
            except RuntimeError:
                return False

        wait_for(pg_ready, "disposable PostgreSQL")
        for database in ["source_metadata", "candidate_metadata", "restored_metadata"]:
            sql("CREATE DATABASE " + database)
        airflow(OLD_IMAGE, "source_metadata", "db", "migrate")
        airflow(OLD_IMAGE, "source_metadata", "variables", "set", "rem34_fixture", "preserved")
        airflow(OLD_IMAGE, "source_metadata", "pools", "set", "rem34_pool", "2", "synthetic")
        airflow(
            OLD_IMAGE,
            "source_metadata",
            "connections",
            "add",
            "rem34_connection",
            "--conn-type",
            "http",
            "--conn-host",
            "synthetic.invalid",
        )
        backup = docker("exec", pg, "pg_dump", "-U", "probe", "-Fc", "source_metadata")
        (root / "metadata.backup").write_bytes(backup)
        for database in ["candidate_metadata", "restored_metadata"]:
            docker(
                "exec",
                "-i",
                pg,
                "pg_restore",
                "-U",
                "probe",
                "-d",
                database,
                "--no-owner",
                input=backup,
            )
        airflow(NEW_IMAGE, "candidate_metadata", "db", "migrate")
        airflow(NEW_IMAGE, "candidate_metadata", "db", "check")
        for database in ["candidate_metadata", "restored_metadata"]:
            assert (
                sql("SELECT val FROM variable WHERE key='rem34_fixture'", database) == "preserved"
            )
            assert sql("SELECT slots FROM slot_pool WHERE pool='rem34_pool'", database) == "2"
            assert (
                sql("SELECT count(*) FROM connection WHERE conn_id='rem34_connection'", database)
                == "1"
            )
        airflow(OLD_IMAGE, "restored_metadata", "db", "check")
        assert (
            "preserved"
            in airflow(OLD_IMAGE, "restored_metadata", "variables", "get", "rem34_fixture").decode()
        )
        airflow(NEW_IMAGE, "candidate_metadata", "api-server", service=api)
        airflow(NEW_IMAGE, "candidate_metadata", "dag-processor", service=prefix + "-processor")
        airflow(NEW_IMAGE, "candidate_metadata", "scheduler", service=prefix + "-scheduler")

        # HTTP checks run inside the isolated network; no host port is exposed.
        request_code = """import json, urllib.request, urllib.error, sys
base="http://localhost:8080"
try:
    urllib.request.urlopen(base+"/api/v2/dags")
except urllib.error.HTTPError as e:
    assert e.code in (401,403), e.code
else:
    raise AssertionError("anonymous protected read allowed")
def denied(req):
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        assert e.code in (401,403), e.code
    else:
        raise AssertionError("unauthorized operation allowed")
denied(urllib.request.Request(base+"/api/v2/dags/rem34_synthetic",
    data=b'{"is_paused":false}', method="PATCH",
    headers={"Content-Type":"application/json"}))
wrong=json.dumps({"username":"admin", "password":"deliberately-invalid"}).encode()
denied(urllib.request.Request(base+"/auth/token", data=wrong,
    headers={"Content-Type":"application/json"}))
payload=json.dumps({"username":"admin", "password":sys.argv[1]}).encode()
req=urllib.request.Request(base+"/auth/token", data=payload,
    headers={"Content-Type":"application/json"})
token=json.load(urllib.request.urlopen(req))["access_token"]
req=urllib.request.Request(base+"/api/v2/dags", headers={"Authorization":"Bearer "+token})
assert urllib.request.urlopen(req).status == 200
"""

        def authenticated_api_ready():
            try:
                docker("exec", api, "python", "-c", request_code, auth_password, timeout=15)
                return True
            except RuntimeError:
                return False

        wait_for(authenticated_api_ready, "authenticated API and denied anonymous read")
        wait_for(
            lambda: sql(
                "SELECT count(*) FROM serialized_dag WHERE dag_id='rem34_synthetic'",
                "candidate_metadata",
            )
            == "1",
            "synthetic DAG parsing",
        )
        airflow(NEW_IMAGE, "candidate_metadata", "dags", "unpause", "rem34_synthetic")
        airflow(NEW_IMAGE, "candidate_metadata", "dags", "trigger", "rem34_synthetic")
        wait_for(
            lambda: sql(
                "SELECT state FROM dag_run WHERE dag_id='rem34_synthetic' ORDER BY id DESC LIMIT 1",
                "candidate_metadata",
            )
            == "success",
            "scheduler / execution API task completion",
        )
        assert (
            sql(
                "SELECT state FROM task_instance WHERE dag_id='rem34_synthetic' LIMIT 1",
                "candidate_metadata",
            )
            == "success"
        )
        return {
            "passed": True,
            "old_image": OLD_IMAGE,
            "new_image": NEW_IMAGE,
            "metadata": "PostgreSQL copy migration, variable/connection/pool retained",
            "recovery": "pre-migration pg_dump restored and checked with Airflow 2.11.2",
            "auth": "anonymous read/mutation and wrong password denied; token protected read 200",
            "execution": "synthetic DAG scheduler/execution API/task success",
            "scope": "owned disposable containers only; no business DAG or working state",
        }
    finally:
        primary_error = sys.exc_info()[1]
        try:
            cleanup(containers, network)
        except RuntimeError:
            context = (
                f"; primary probe error: {type(primary_error).__name__}"
                if primary_error is not None
                else ""
            )
            raise RuntimeError("Owned Docker resource cleanup unverified" + context) from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--summary", type=Path, default=Path("reports/optional/airflow-migration.json")
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="secop-airflow-migration-") as directory:
        result = run_probe(Path(directory))
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
