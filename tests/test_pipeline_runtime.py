"""Check the effective security and resource contract of the opt-in runtime."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _runtime():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())["x-airflow-common"]


def test_runtime_is_installed_isolated_and_read_only():
    runtime = _runtime()
    assert runtime["profiles"] == ["airflow"]
    assert runtime["build"]["dockerfile"] == "infra/docker/airflow/Dockerfile"
    assert runtime["read_only"] is True
    assert runtime["tmpfs"] == ["/tmp:mode=1777,exec,size=512m"]
    assert runtime["working_dir"] == "/opt/secop"
    mounts = runtime["volumes"]
    assert "./dags:/opt/airflow/dags:ro" in mounts
    assert "pipeline_data:/opt/secop-data" in mounts
    assert "pipeline_reports:/opt/secop-reports" in mounts
    assert not any("docker.sock" in mount or "/home/" in mount for mount in mounts)
    environment = runtime["environment"]
    assert environment["SECOP_PIPELINE_ROOT"] == "/opt/secop-data/orchestration"
    assert environment["SECOP_REPORT_ROOT"] == "/opt/secop-reports"
    assert environment["SECOP_RUNTIME_MODE"] == "${SECOP_RUNTIME_MODE:-demo}"
    assert environment["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] == "false"
    assert environment["POSTGRES_HOST"] == "postgres"
    assert environment["POSTGRES_PORT"] == "5432"
    assert "POSTGRES_PASSWORD" in environment
    assert environment["JAVA_TOOL_OPTIONS"] == "-Xss512k -XX:ActiveProcessorCount=2"


def test_build_uses_locked_dependencies_and_allowlisted_resources():
    dockerfile = (ROOT / "infra/docker/airflow/Dockerfile").read_text()
    assert "openjdk-17-jre-headless" in dockerfile
    assert "uv sync --frozen" in dockerfile
    for extra in ("data", "warehouse", "orchestration"):
        assert f"--extra {extra}" in dockerfile
    assert "--no-install-project" in dockerfile
    assert "COPY . " not in dockerfile
    assert "chmod -R a-w /opt/secop" in dockerfile
    assert 'PATH="/opt/secop/.venv/bin:${PATH}"' in dockerfile
    copied = [line.split()[2] for line in dockerfile.splitlines() if line.startswith("COPY ")]
    assert set(copied) == {"pyproject.toml", "src/", "contracts/", "config/", "dbt/", "scripts/"}
    context = (ROOT / "infra/docker/airflow/Dockerfile.dockerignore").read_text().splitlines()
    assert "**" in context
    assert not any(
        line.startswith("!")
        and any(
            forbidden in line for forbidden in ("openspec", "reports", ".env", ".project", "data/")
        )
        for line in context
    )
    for excluded in ("openspec", "reports", ".agents", ".aws"):
        assert excluded in (ROOT / ".dockerignore").read_text().splitlines()


def test_runtime_retains_loopback_authentication_and_separate_metadata():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    api = compose["services"]["airflow-api-server"]
    assert all(port.startswith("127.0.0.1:") for port in api["ports"])
    connection = api["environment"]["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
    assert "${AIRFLOW_DB:" in connection
    init = "\n".join(compose["services"]["airflow-init"]["command"])
    assert "0o600" in init
    assert "probe.touch(exist_ok=False)" in init
    assert "Pipeline writable-root preflight failed" in init
    assert "pipeline_data" in compose["volumes"]
    assert "pipeline_reports" in compose["volumes"]


def test_async_metadata_uses_locked_installed_driver():
    from pathlib import Path

    import yaml

    runtime = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text()
    )
    environment = runtime["x-airflow-common"]["environment"]
    assert environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC"].startswith(
        "postgresql+psycopg://"
    )
