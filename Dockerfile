# Airflow 3.3.2 plus a separate Python environment (/opt/venv) with the pipeline's
# dependencies from uv.lock. Tasks run our scripts with /opt/venv/bin/python, so
# TFDV/TensorFlow never clash with Airflow's own pinned packages.
#
# Debian trixie, not the official apache/airflow image: that one is Debian bookworm
# (glibc 2.36), and TFDV 1.21 only ships Linux wheels for glibc >= 2.39.
#
# Cache mounts (--mount=type=cache) keep apt/uv downloads between builds to speed up
# rebuilds; they are never part of the image.

# --- stage 1: build the pipeline environment (needs a compiler; discarded afterwards) ---
FROM python:3.11-slim-trixie AS pipeline-env
# keep downloaded .debs so the apt cache mount actually caches them
RUN rm -f /etc/apt/apt.conf.d/docker-clean
# g++ builds pyfarmhash, a TFDV dependency with no prebuilt wheel
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends g++
COPY pyproject.toml uv.lock /opt/pipeline/
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=from=ghcr.io/astral-sh/uv:0.11.8,source=/uv,target=/bin/uv \
    cd /opt/pipeline \
    && UV_CACHE_DIR=/root/.cache/uv UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/opt/venv \
       uv sync --frozen --no-dev --no-install-project --python /usr/local/bin/python

# --- stage 2: the Airflow image we actually run ---
FROM python:3.11-slim-trixie
ARG AIRFLOW_VERSION=3.3.2
RUN rm -f /etc/apt/apt.conf.d/docker-clean
# git: for DVC; curl: compose healthcheck
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && useradd --create-home --uid 50000 --gid 0 airflow \
    && mkdir -p /opt/airflow && chown airflow:root /opt/airflow \
    && chmod g+rwX /opt/airflow  # group 0 can write, and every AIRFLOW_UID runs with gid 0

# Airflow, pinned by the official constraints file; installed with uv (much faster than pip)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=from=ghcr.io/astral-sh/uv:0.11.8,source=/uv,target=/bin/uv \
    UV_CACHE_DIR=/root/.cache/uv UV_LINK_MODE=copy uv pip install --system \
    "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
    apache-airflow-providers-standard \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

COPY --from=pipeline-env /opt/venv /opt/venv

# containers run as any uid (AIRFLOW_UID), which has no home or username of its own;
# Airflow needs both (it reads the name via getpass, which checks $USER first)
ENV AIRFLOW_HOME=/opt/airflow HOME=/opt/airflow USER=airflow
USER airflow
WORKDIR /opt/airflow
ENTRYPOINT ["airflow"]
