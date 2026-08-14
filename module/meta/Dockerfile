# syntax=docker/dockerfile:1
# Slim job image: Meta Marketing API SDK only (for PodOperator / batch workers).
# Build from repo root: docker build -f jobs/meta/Dockerfile -t merino-meta-jobs:local jobs/meta

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=meta_rate_limit . /tmp/merino_meta_rate_limit
RUN pip install --no-cache-dir /tmp/merino_meta_rate_limit

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY merino_meta_jobs ./merino_meta_jobs

# Smoke: SDK imports (override CMD in KubernetesPodOperator for real jobs).
CMD ["python", "-m", "merino_meta_jobs"]
