# Merino Meta job image

Lightweight image with the official [facebook-business](https://github.com/facebook/facebook-python-business-sdk) SDK for Marketing API work. Intended for `KubernetesPodOperator` (or similar) from Airflow, **not** as a replacement for the Airflow scheduler/worker image.

## Build

From the `merino-airflow` repo root (`merino/jobs/airflow`):

```bash
docker build -f jobs/meta/Dockerfile -t merino-meta-jobs:local jobs/meta
```

## Push (Artifact Registry example)

```bash
REGION=us-central1
PROJECT=your-gcp-project
REPO=merino-jobs
TAG=v0.1.0

docker tag merino-meta-jobs:local "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/merino-meta-jobs:${TAG}"
docker push "${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/merino-meta-jobs:${TAG}"
```

## Run

Default command verifies SDK imports:

```bash
docker run --rm merino-meta-jobs:local
```

Override `command` / `args` in Kubernetes to run your own module, e.g. `python -c "..."` or extend `merino_meta_jobs` with new entrypoints.
