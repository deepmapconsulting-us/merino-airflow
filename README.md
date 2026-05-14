# merino-airflow

DAG repository synced into the Merino Airflow deployment via git-sync.

The Helm values point at this repo with `subPath: dags`, so the
`dags/` directory in the repo root is mounted as the Airflow DAG bag.

## Layout

```
dags/        # Airflow DAGs (one .py file per DAG)
jobs/        # Slim Docker images + Python libs for PodOperator / batch (not git-synced as DAGs)
```

See `jobs/meta/README.md` for the Meta Marketing API job image build and push.

## Adding a DAG

1. Drop a new `*.py` file under `dags/`.
2. Commit to `main`.
3. git-sync will pick it up on its next interval (default ~60s).
