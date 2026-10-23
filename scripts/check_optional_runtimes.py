"""Check the optional dependency graph using only synthetic, disposable local state."""

import argparse
import json
import os
import subprocess
import tempfile
from importlib.metadata import version
from pathlib import Path


def isolated_environment(source):
    """Exclude caller service settings before constructing disposable smoke state."""
    return {
        key: value for key, value in source.items() if not key.startswith(("AIRFLOW_", "MLFLOW_"))
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, default=Path("reports/optional/runtime-smoke.json"))
    summary = parser.parse_args().summary
    environment = isolated_environment(os.environ)
    os.environ.clear()
    os.environ.update(environment)
    with tempfile.TemporaryDirectory(prefix="secop-remediation-mixed-") as directory:
        root = Path(directory)
        env = os.environ.copy()
        env.update(
            AIRFLOW_HOME=str(root / "airflow"),
            AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///" + str(root / "airflow.db"),
            AIRFLOW__CORE__LOAD_EXAMPLES="False",
            AIRFLOW__CORE__EXECUTOR="LocalExecutor",
            AIRFLOW__CORE__AUTH_MANAGER=(
                "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
            ),
            AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS="False",
            AIRFLOW__CORE__FERNET_KEY="",
        )
        commands = []
        for args in [
            ("version",),
            ("db", "migrate"),
            ("db", "check"),
            ("config", "get-value", "core", "executor"),
            ("config", "get-value", "core", "auth_manager"),
        ]:
            result = subprocess.run(
                ["airflow", *args], env=env, capture_output=True, text=True, timeout=180
            )
            (root / ("airflow-" + args[0] + ".log")).write_text(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError("Airflow smoke failed: " + str(args))
            if args[-1] == "executor":
                assert "LocalExecutor" in result.stdout
            if args[-1] == "auth_manager":
                assert "SimpleAuthManager" in result.stdout
            commands.append({"command": ["airflow", *args], "exitstatus": result.returncode})
        import mlflow
        from mlflow.tracking import MlflowClient

        mlflow.set_tracking_uri("sqlite:///" + str(root / "mlflow.db"))
        experiment = mlflow.create_experiment(
            "compatible-smoke", artifact_location=(root / "artifacts").as_uri()
        )
        with mlflow.start_run(experiment_id=experiment) as run:
            mlflow.log_metric("synthetic_metric", 1.25)
            sample = root / "sample.txt"
            sample.write_text("synthetic compatibility fixture")
            mlflow.log_artifact(str(sample))
            run_id = run.info.run_id
        client = MlflowClient()
        assert client.get_run(run_id).data.metrics["synthetic_metric"] == 1.25
        (root / "download").mkdir()
        downloaded = client.download_artifacts(run_id, "sample.txt", str(root / "download"))
        assert Path(downloaded).read_text() == "synthetic compatibility fixture"
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        app = FastAPI()

        @app.get("/fixture")
        def fixture():
            return {"synthetic": True}

        with TestClient(app) as client:
            assert client.get("/fixture").json() == {"synthetic": True}
            assert client.get("/fixture").status_code == 200
            assert client.get("/missing").status_code == 404
        from cryptography.fernet import Fernet

        f = Fernet(Fernet.generate_key())
        assert f.decrypt(f.encrypt(b"synthetic")) == b"synthetic"
        from PIL import Image

        image = root / "fixture.png"
        Image.new("RGB", (2, 2)).save(image)
        with Image.open(image) as check:
            assert check.size == (2, 2)
        result = {
            "passed": True,
            "versions": {
                n: version(n)
                for n in [
                    "apache-airflow",
                    "mlflow",
                    "fastapi",
                    "starlette",
                    "cryptography",
                    "pillow",
                    "sqlalchemy",
                    "opentelemetry-api",
                ]
            },
            "airflow_commands": commands,
            "mlflow": "sqlite experiment/run/metric/artifact roundtrip",
            "api": "synthetic 200JSON/404 with TestClient",
            "crypto_image": "Fernet roundtrip and Pillow PNG roundtrip",
            "scope": "same isolated all-extras environment; synthetic routes and disposable state",
        }
        summary.parent.mkdir(parents=True, exist_ok=True)
        summary.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
