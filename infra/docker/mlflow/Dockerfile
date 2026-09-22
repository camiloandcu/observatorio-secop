ARG MLFLOW_IMAGE=ghcr.io/mlflow/mlflow:v3.4.0
FROM ${MLFLOW_IMAGE}

RUN python -m pip install --no-cache-dir "psycopg2-binary==2.9.10"

RUN useradd --create-home --uid 10001 mlflow
USER mlflow
WORKDIR /home/mlflow

EXPOSE 5000
ENTRYPOINT ["mlflow"]
