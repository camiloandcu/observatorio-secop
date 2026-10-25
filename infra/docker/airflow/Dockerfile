ARG AIRFLOW_IMAGE=apache/airflow:3.3.2-python3.12
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/secop /opt/secop-data /opt/secop-reports \
    && chown airflow:root /opt/secop /opt/secop-data /opt/secop-reports \
    && chmod 0770 /opt/secop-data /opt/secop-reports

WORKDIR /opt/secop
COPY --chown=airflow:root pyproject.toml uv.lock README.md ./
USER airflow
# Install exactly the reviewed lock; source changes reuse this dependency layer.
RUN uv sync --frozen --no-dev --extra data --extra warehouse --extra orchestration \
    --no-install-project --python /usr/local/bin/python --no-managed-python \
    && uv cache clean

USER root
# Allowlist runtime resources; private planning/data/reports never enter this image.
COPY --chown=root:root src/ ./src/
COPY --chown=root:root contracts/ ./contracts/
COPY --chown=root:root config/ ./config/
COPY --chown=root:root dbt/ ./dbt/
COPY --chown=root:root scripts/ ./scripts/
RUN chmod -R a-w src contracts config dbt scripts
USER airflow
RUN uv sync --frozen --no-dev --extra data --extra warehouse --extra orchestration \
    --python /usr/local/bin/python --no-managed-python \
    && uv cache clean
USER root
RUN chmod -R a-w /opt/secop
USER airflow

ENV PATH="/opt/secop/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_NO_SYNC=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_LOCAL_IP=127.0.0.1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
