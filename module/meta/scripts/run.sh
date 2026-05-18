cd /path/to/merino/jobs/airflow
docker build -f jobs/meta/Dockerfile -t merino-meta-jobs:local jobs/meta
docker run --rm merino-meta-jobs:local


