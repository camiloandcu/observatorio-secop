"""Seed or verify synthetic MLflow state; connection settings never enter output."""

import argparse
import json
import os
from importlib.metadata import version
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("seed", "verify"))
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for key in list(os.environ):
        if key.startswith("MLFLOW_"):
            del os.environ[key]
    import mlflow
    from mlflow.tracking import MlflowClient

    mlflow.set_tracking_uri(os.environ["SECOP_PROBE_DSN"])
    manifest = args.root / "fixture.json"
    if args.mode == "seed":
        experiment = mlflow.create_experiment(
            "synthetic-old-state", artifact_location=(args.root / "old-artifacts").as_uri()
        )
        sample = args.root / "old-sample.txt"
        sample.write_bytes(b"synthetic preserved MLflow artifact\n")
        with mlflow.start_run(experiment_id=experiment) as active:
            mlflow.log_metric("synthetic_metric", 1.25)
            mlflow.log_param("synthetic_parameter", "baseline")
            mlflow.log_artifact(str(sample))
            run_id = active.info.run_id
        manifest.write_text(json.dumps({"experiment_id": experiment, "run_id": run_id}))
    fixture = json.loads(manifest.read_text())
    client = MlflowClient()
    run = client.get_run(fixture["run_id"])
    assert run.info.experiment_id == fixture["experiment_id"]
    assert client.get_experiment(fixture["experiment_id"]).name == "synthetic-old-state"
    assert run.info.status == "FINISHED"
    assert run.data.metrics["synthetic_metric"] == 1.25
    assert run.data.params["synthetic_parameter"] == "baseline"
    download = args.root / ("download-" + args.output.stem)
    download.mkdir()
    artifact = client.download_artifacts(fixture["run_id"], "old-sample.txt", str(download))
    assert Path(artifact).read_bytes() == b"synthetic preserved MLflow artifact\n"
    args.output.write_text(
        json.dumps({"passed": True, "versions": {n: version(n) for n in ("mlflow", "sqlalchemy")}})
    )


if __name__ == "__main__":
    main()
