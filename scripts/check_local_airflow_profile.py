"""Exercise repository Compose using isolated services and synthetic PostgreSQL state."""

import argparse
import contextlib
import hashlib
import io
import json
import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as connection:
        connection.bind(("127.0.0.1", 0))
        return connection.getsockname()[1]


def run(command, environment, timeout=180):
    result = subprocess.run(
        command, env=environment, capture_output=True, text=True, timeout=timeout, check=False
    )
    if result.returncode:
        raise RuntimeError("Disposable runtime command failed; sensitive output suppressed")
    return result.stdout.strip()


def request(url, payload=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = json.dumps(payload).encode() if payload is not None else None
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, data=data, headers=headers), timeout=15
        ) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, None


def wait_healthy(compose, services, environment):
    deadline = time.monotonic() + 300
    while True:
        states = []
        for service in ["postgres", *services]:
            container = run(compose + ["ps", "-q", service], environment)
            state = run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
                environment,
            )
            states.append(state)
        if all(state == "healthy" for state in states):
            return
        if "unhealthy" in states or time.monotonic() >= deadline:
            raise RuntimeError("Disposable Compose health failed")
        time.sleep(2)


def mlflow_roundtrip(root, port, password):
    # Isolate user tracking settings; exceptions never escape with database credentials.
    for key in list(os.environ):
        if key.startswith("MLFLOW_"):
            del os.environ[key]
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri(f"postgresql+psycopg2://probe:{password}@127.0.0.1:{port}/mlflow")
        experiment = mlflow.create_experiment(
            "synthetic-compose-profile", artifact_location=(root / "artifacts").as_uri()
        )
        sample = root / "sample.txt"
        sample.write_text("synthetic compose fixture", encoding="utf-8")
        with mlflow.start_run(experiment_id=experiment) as active:
            mlflow.log_metric("synthetic_metric", 1.25)
            mlflow.log_artifact(str(sample))
            run_id = active.info.run_id
        client = MlflowClient()
        assert client.get_run(run_id).data.metrics["synthetic_metric"] == 1.25
        download = root / "download"
        download.mkdir()
        restored = client.download_artifacts(run_id, "sample.txt", str(download))
        assert Path(restored).read_text(encoding="utf-8") == sample.read_text(encoding="utf-8")


def mlflow_existing_state(root, port, password, baseline_python, compose, environment):
    helper = str(ROOT / "scripts/check_mlflow_state_fixture.py")
    old_root = root / "mlflow-existing"
    old_root.mkdir()
    for database in ("mlflow_existing", "mlflow_restored"):
        run(compose + ["exec", "-T", "postgres", "createdb", "-U", "probe", database], environment)

    def fixture(python, mode, database, label):
        controlled = environment.copy()
        controlled["SECOP_PROBE_DSN"] = (
            f"postgresql+psycopg2://probe:{password}@127.0.0.1:{port}/{database}"
        )
        output = old_root / (label + ".json")
        run([str(python), helper, mode, str(old_root), "--output", str(output)], controlled)
        return json.loads(output.read_text())

    baseline = fixture(baseline_python, "seed", "mlflow_existing", "baseline")
    assert baseline["versions"]["sqlalchemy"].startswith("1.4.")
    backup = root / "mlflow-backup.dump"
    # Binary stdout goes directly to a private temporary file, never to logs/evidence.
    backup.touch(mode=0o600)
    with backup.open("wb") as destination:
        result = subprocess.run(
            compose
            + ["exec", "-T", "postgres", "pg_dump", "-U", "probe", "-Fc", "mlflow_existing"],
            env=environment,
            stdout=destination,
            stderr=subprocess.PIPE,
            timeout=180,
            check=False,
        )
    if result.returncode:
        raise RuntimeError("Disposable metadata backup failed; sensitive output suppressed")
    upgraded = fixture(sys.executable, "verify", "mlflow_existing", "upgraded")
    assert upgraded["versions"]["sqlalchemy"].startswith("2.")
    assert upgraded["versions"]["mlflow"] == baseline["versions"]["mlflow"]
    with backup.open("rb") as source:
        result = subprocess.run(
            compose
            + [
                "exec",
                "-T",
                "postgres",
                "pg_restore",
                "-U",
                "probe",
                "--exit-on-error",
                "-d",
                "mlflow_restored",
            ],
            env=environment,
            stdin=source,
            capture_output=True,
            timeout=180,
            check=False,
        )
    if result.returncode:
        raise RuntimeError("Disposable metadata restore failed; sensitive output suppressed")
    restored = fixture(baseline_python, "verify", "mlflow_restored", "restored")
    assert restored["versions"] == baseline["versions"]
    return {
        "baseline": baseline,
        "new_graph_existing_state": upgraded,
        "old_graph_restored_backup": restored,
        "preserved": [
            "experiment/run IDs",
            "finished status",
            "metric",
            "parameter",
            "artifact bytes",
        ],
        "scope": (
            "host direct PostgreSQL backend; synthetic local artifacts; no Docker HTTP/image test"
        ),
    }


def probe(summary, baseline_python):
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AIRFLOW_", "MLFLOW_", "COMPOSE_", "POSTGRES_"))
    }
    project = "secop-profile-" + secrets.token_hex(6)
    evidence = {"project": project, "passed": False, "cleanup_verified": False}
    with tempfile.TemporaryDirectory(prefix=project + "-") as directory:
        root = Path(directory)
        env_file = root / "probe.env"
        password = secrets.token_hex(24)
        postgres_port, api_port = free_port(), free_port()
        values = {
            "COMPOSE_PROJECT_NAME": project,
            "POSTGRES_USER": "probe",
            "POSTGRES_PASSWORD": password,
            "POSTGRES_PORT": str(postgres_port),
            "AIRFLOW_PORT": str(api_port),
            "AIRFLOW_UID": "50000",
        }
        lines = []
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
            key = line.split("=", 1)[0]
            lines.append(f"{key}={values[key]}" if key in values else line)
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        env_file.chmod(0o600)
        compose = [
            "docker",
            "compose",
            "--project-name",
            project,
            "--env-file",
            str(env_file),
            "--project-directory",
            str(ROOT),
            "-f",
            str(ROOT / "docker-compose.yml"),
            "--profile",
            "airflow",
        ]
        started = time.monotonic()
        try:
            evidence["stage"] = "compose-config"
            run(compose + ["config", "--quiet"], environment)
            evidence["stage"] = "compose-startup"
            services = ["airflow-api-server", "airflow-scheduler", "airflow-dag-processor"]
            run(compose + ["up", "-d", *services], environment, timeout=300)
            wait_healthy(compose, services, environment)
            evidence["healthy_services"] = ["postgres", *services]
            evidence["stage"] = "authentication-and-secrets"
            modes = run(
                compose
                + [
                    "exec",
                    "-T",
                    "airflow-api-server",
                    "stat",
                    "-c",
                    "%a",
                    "/opt/airflow/local-state/passwords.json",
                    "/opt/airflow/local-state/jwt-secret",
                ],
                environment,
            )
            assert modes.splitlines() == ["600", "600"]
            evidence["secret_file_modes"] = ["600", "600"]
            credentials = run(
                compose
                + [
                    "exec",
                    "-T",
                    "airflow-api-server",
                    "cat",
                    "/opt/airflow/local-state/passwords.json",
                ],
                environment,
            )
            digest = hashlib.sha256(credentials.encode()).hexdigest()
            api = f"http://127.0.0.1:{api_port}"
            denied, _ = request(api + "/api/v2/dags")
            evidence["anonymous_status"] = denied
            assert denied in (401, 403)
            code, token = request(
                api + "/auth/token",
                {"username": "admin", "password": json.loads(credentials)["admin"]},
            )
            evidence["token_status"] = code
            assert code in (200, 201)
            allowed, _ = request(api + "/api/v2/dags", token=token["access_token"])
            evidence["authenticated_status"] = allowed
            assert allowed == 200
            evidence["authentication"] = {"anonymous": denied, "authenticated": allowed}
            evidence["stage"] = "restart"
            run(compose + ["restart", *services], environment)
            wait_healthy(compose, services, environment)
            persisted = run(
                compose
                + [
                    "exec",
                    "-T",
                    "airflow-api-server",
                    "cat",
                    "/opt/airflow/local-state/passwords.json",
                ],
                environment,
            )
            assert hashlib.sha256(persisted.encode()).hexdigest() == digest
            evidence["password_persists_after_restart"] = True
            evidence["stage"] = "mlflow-postgresql"
            mlflow_roundtrip(root, postgres_port, password)
            evidence["mlflow"] = "host direct PostgreSQL experiment/run/metric/artifact roundtrip"
            evidence["stage"] = "mlflow-existing-state"
            evidence["mlflow_existing_state"] = mlflow_existing_state(
                root, postgres_port, password, baseline_python, compose, environment
            )
            evidence["passed"] = True
            evidence["stage"] = "complete"
        except Exception as error:
            evidence["failure_type"] = type(error).__name__
            evidence["service_diagnostics"] = {}
            for service in (
                "postgres",
                "airflow-init",
                "airflow-api-server",
                "airflow-scheduler",
                "airflow-dag-processor",
            ):
                container = subprocess.run(
                    compose + ["ps", "-aq", service],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                ).stdout.strip()
                if not container:
                    continue
                state = subprocess.run(
                    [
                        "docker",
                        "inspect",
                        "--format",
                        "{{.State.Status}}/{{.State.ExitCode}}",
                        container,
                    ],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                ).stdout.strip()
                logs = subprocess.run(
                    ["docker", "logs", "--tail", "80", container],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
                # Retain only exception class names, never raw lines, credentials or tokens.
                classes = sorted(
                    set(
                        re.findall(
                            r"(?m)^([A-Za-z_.]+(?:Error|Exception)):", logs.stdout + logs.stderr
                        )
                    )
                )
                evidence["service_diagnostics"][service] = {
                    "state_exit": state,
                    "exception_classes": classes,
                }
            raise
        finally:
            run(compose + ["down", "--volumes", "--remove-orphans"], environment)
            containers = run(
                ["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project],
                environment,
            )
            volumes = run(
                [
                    "docker",
                    "volume",
                    "ls",
                    "-q",
                    "--filter",
                    "label=com.docker.compose.project=" + project,
                ],
                environment,
            )
            evidence["cleanup_verified"] = not containers and not volumes
            evidence["elapsed_seconds"] = round(time.monotonic() - started, 2)
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
            assert evidence["cleanup_verified"]
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--baseline-python", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = probe(args.summary, args.baseline_python)
    except Exception:
        print("Disposable Compose profile failed; no sensitive diagnostic output retained.")
        raise SystemExit(1) from None
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
