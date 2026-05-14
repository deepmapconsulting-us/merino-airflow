"""Ingest Facebook (Meta) Marketing traffic / insights into Merino (stub).

Replace the placeholder task with Graph / Marketing API export logic,
secrets from Airflow Connections or Variables, and your sink (GCS, BQ, etc.).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="facebook_traffic_ingestion",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["facebook", "traffic", "ingestion", "meta"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def facebook_traffic_ingestion():
    @task
    def fetch_traffic_stub() -> None:
        # Wire Meta Marketing API + landing path; this keeps the DAG valid until then.
        print("facebook_traffic_ingestion: stub run — add API client and persistence.")

    fetch_traffic_stub()


facebook_traffic_ingestion()
