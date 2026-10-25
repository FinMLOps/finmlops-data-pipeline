# Airflow 3.3.2 on Debian trixie, plus a separate Python environment (/opt/venv)
# with the pipeline's own dependencies, installed from uv.lock. Tasks run our
# scripts with /opt/venv/bin/python, so TFDV/TensorFlow never clash with
# Airflow's own pinned packages.
#
# Not the official apache/airflow image: it is Debian bookworm (glibc 2.36),
# and TFDV 1.21 only ships Linux wheels for glibc >= 2.39.
FROM python:3.11-slim-trixie

ARG AIRFLOW_VERSION=3.3.2
ENV AIRFLOW_HOME=/opt/airflow

# git: DVC needs it later; curl: compose healthcheck; g++: builds pyfarmhash (a TFDV dependency with no wheel)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl g++ \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 50000 --gid 0 airflow \
    && mkdir -p /opt/airflow /opt/venv && chown airflow:root /opt/airflow /opt/venv
COPY --from=ghcr.io/astral-sh/uv:0.11.8 /uv /usr/local/bin/uv

# Airflow itself, pinned by the official constraints file so installs are reproducible
RUN pip install --no-cache-dir \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-fab \
    apache-airflow-providers-standard \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

USER airflow
WORKDIR /opt/airflow
COPY --chown=airflow:root pyproject.toml uv.lock /opt/pipeline/
RUN cd /opt/pipeline \
    && UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --frozen --no-dev --no-install-project \
       --python /usr/local/bin/python

ENTRYPOINT ["airflow"]
