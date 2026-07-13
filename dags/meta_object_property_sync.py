"""Sync Meta campaign / adset / ad properties into Postgres dimension tables.

Reads the latest ``facebook_campaign_config_update`` GCS snapshot and upserts
``marketing.meta_campaign``, ``marketing.meta_adset``, and ``marketing.meta_ad``.
Also captures ad set ``targeting`` into ``marketing.meta_adset_config`` when it
changes (SCD Type 2).

Variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| ``meta_object_property_full_init`` | ``false`` | One-time full Graph detail bootstrap |
| ``FACEBOOK_ACTIVE_ACCOUNTS`` | all accounts in snapshot | Account filter |
| ``meta_access_token`` | env ``META_ACCESS_TOKEN`` | Graph API token |
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum  # type: ignore[import-not-found]
from airflow.sdk import dag, task  # type: ignore[import-not-found]

try:
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found]
except ImportError:
    from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore[import-not-found,no-redef]

from meta_gcs import (
    REPORT_TIMEZONE,
    campaign_config_logical_date,
    env_config_value,
    gcs_console_link,
    meta_access_token,
    read_json_from_gcs,
    read_latest_snapshot_pointer,
    variable_get,
)

MODULE_PATH = Path(__file__).resolve().parents[1] / "module" / "meta"
if MODULE_PATH.exists():
    sys.path.insert(0, str(MODULE_PATH))

from merino_meta_jobs.adset_config import (  # noqa: E402  # type: ignore[import-not-found]
    active_adsets_from_flat,
    fetch_active_adset_configs,
    sync_adset_budget_versions,
    sync_adset_config_versions,
    sync_adset_targeting_daily_snapshots,
)
from merino_meta_jobs.ad_creative import (  # noqa: E402  # type: ignore[import-not-found]
    ads_for_creative_registry_from_snapshot,
    fetch_and_sync_ad_creatives,
)
from merino_meta_jobs.facebook_graph import MetaGraphClient, ensure_act_prefix  # noqa: E402  # type: ignore[import-not-found]
from merino_meta_jobs.object_property import (  # noqa: E402  # type: ignore[import-not-found]
    detail_rows_for_new_ids,
    flatten_config_snapshot,
    full_init_rows,
    incremental_rows_from_snapshot,
    load_existing_ids,
    stub_rows_from_metrics,
    sync_all_rows,
)
from merino_meta_jobs.traffic import account_ids_from_text, DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS  # noqa: E402  # type: ignore[import-not-found]

DAG_ID = "meta_object_property_sync"
CAMPAIGN_CONFIG_DAG_ID = "facebook_campaign_config_update"
CONFIG_GCS_PREFIX = "facebook_campaign_config_update"
ACTIVE_ACCOUNTS_ENV = "FACEBOOK_ACTIVE_ACCOUNTS"
LOOKUP_WINDOW_ENV = "FACEBOOK_TRAFFIC_LOOKUP_WINDOWS"
FULL_INIT_VARIABLE_NAME = "meta_object_property_full_init"
POSTGRES_CONN_ID = "merino_analytics"
DEFAULT_META_PAGE_LIMIT = 500


@dag(
    dag_id=DAG_ID,
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, 0, 0, tz=REPORT_TIMEZONE),
    catchup=False,
    # Only one run at a time so a slow/failed full-init or retry cannot overlap
    # the next scheduled sync and double-write dimension tables.
    max_active_runs=1,
    tags=["meta", "config", "dimension"],
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md=__doc__,
)
def meta_object_property_sync():
    config_source = _config_source_for_display()

    @task
    def log_config_source(source: dict[str, Any]) -> None:
        print(f"{DAG_ID}: pointer: {source.get('pointer_uri') or '<none>'}")
        print(f"{DAG_ID}: snapshot: {source.get('snapshot_uri') or '<none>'}")
        if source.get("error"):
            print(f"{DAG_ID}: config unavailable during parse: {source['error']}")
        else:
            flat = flatten_config_snapshot(source.get("snapshot", {}))
            print(
                f"{DAG_ID}: snapshot objects campaigns={len(flat['campaigns'])} "
                f"adsets={len(flat['adsets'])} ads={len(flat['ads'])}"
            )

    @task(max_active_tis_per_dag=1)
    def sync_object_properties(source: dict[str, Any]) -> dict[str, Any]:
        if source.get("error"):
            raise RuntimeError(source["error"])

        snapshot = source.get("snapshot")
        if not isinstance(snapshot, dict):
            raise RuntimeError("campaign config snapshot is missing")

        snapshot_uri = str(source.get("snapshot_uri") or "")
        access_token = meta_access_token()
        page_limit = int(os.environ.get("META_GRAPH_PAGE_LIMIT", DEFAULT_META_PAGE_LIMIT))
        full_init = variable_get(FULL_INIT_VARIABLE_NAME, "false").strip().lower() in {"1", "true", "yes"}
        account_ids = _property_account_ids(snapshot)

        from airflow.models import Variable  # type: ignore[import-not-found]
        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        client = MetaGraphClient(access_token)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()

        detail_fetches = 0
        try:
            if full_init:
                campaign_rows: list[tuple[Any, ...]] = []
                adset_rows: list[tuple[Any, ...]] = []
                ad_rows: list[tuple[Any, ...]] = []
                for account_id in account_ids:
                    rows = full_init_rows(
                        client,
                        account_id,
                        page_limit=page_limit,
                        config_snapshot_uri=snapshot_uri,
                    )
                    campaign_rows.extend(rows["campaigns"])
                    adset_rows.extend(rows["adsets"])
                    ad_rows.extend(rows["ads"])
                counts = sync_all_rows(
                    conn,
                    campaign_rows=campaign_rows,
                    adset_rows=adset_rows,
                    ad_rows=ad_rows,
                )
                Variable.set(FULL_INIT_VARIABLE_NAME, "false")
                print(f"{DAG_ID}: full init complete; set {FULL_INIT_VARIABLE_NAME}=false")
            else:
                flat = flatten_config_snapshot(snapshot)
                incremental = incremental_rows_from_snapshot(flat, config_snapshot_uri=snapshot_uri)
                existing = load_existing_ids(conn)
                detail = detail_rows_for_new_ids(
                    client,
                    flat,
                    existing,
                    config_snapshot_uri=snapshot_uri,
                )
                detail_fetches = (
                    len(detail["campaigns"]) + len(detail["adsets"]) + len(detail["ads"])
                )
                counts = sync_all_rows(
                    conn,
                    campaign_rows=incremental["campaigns"] + detail["campaigns"],
                    adset_rows=incremental["adsets"] + detail["adsets"],
                    ad_rows=incremental["ads"] + detail["ads"],
                )

            stub_counts = stub_rows_from_metrics(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        result = {
            "campaigns_upserted": counts["campaigns"],
            "adsets_upserted": counts["adsets"],
            "ads_upserted": counts["ads"],
            "detail_fetches": detail_fetches,
            "stubs_inserted": sum(stub_counts.values()),
            "full_init": full_init,
        }
        print(f"{DAG_ID}: sync complete {result}")
        return result

    @task
    def sync_adset_configs(source: dict[str, Any]) -> dict[str, Any]:
        if source.get("error"):
            raise RuntimeError(source["error"])

        snapshot = source.get("snapshot")
        if not isinstance(snapshot, dict):
            raise RuntimeError("campaign config snapshot is missing")

        snapshot_uri = str(source.get("snapshot_uri") or "")
        access_token = meta_access_token()
        account_ids = set(_property_account_ids(snapshot))

        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        flat = flatten_config_snapshot(snapshot)
        active_adsets = [
            row
            for row in active_adsets_from_flat(flat)
            if not account_ids or ensure_act_prefix(str(row.get("source_account_id") or "")) in account_ids
        ]

        client = MetaGraphClient(access_token)
        config_rows = fetch_active_adset_configs(
            client,
            active_adsets,
            config_snapshot_uri=snapshot_uri,
        )

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            scd_counts = sync_adset_config_versions(conn, config_rows)
            targeting_daily_counts = sync_adset_targeting_daily_snapshots(conn, config_rows)
            budget_counts = sync_adset_budget_versions(conn, config_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        result = {
            "active_adsets": len(active_adsets),
            **scd_counts,
            **targeting_daily_counts,
            **budget_counts,
        }
        print(f"{DAG_ID}: adset config sync complete {result}")
        return result

    @task
    def sync_ad_creative_media(source: dict[str, Any]) -> dict[str, Any]:
        if source.get("error"):
            raise RuntimeError(source["error"])

        snapshot = source.get("snapshot")
        if not isinstance(snapshot, dict):
            raise RuntimeError("campaign config snapshot is missing")

        snapshot_uri = str(source.get("snapshot_uri") or "")
        access_token = meta_access_token()
        lookup_window_days = int(
            env_config_value(LOOKUP_WINDOW_ENV, str(DEFAULT_TRAFFIC_LOOKUP_WINDOW_DAYS))
        )
        active_accounts = env_config_value(ACTIVE_ACCOUNTS_ENV)
        ads = ads_for_creative_registry_from_snapshot(
            snapshot,
            active_accounts_value=active_accounts,
            lookup_window_days=lookup_window_days,
        )

        from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-not-found]

        client = MetaGraphClient(access_token)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        try:
            result = fetch_and_sync_ad_creatives(
                client,
                conn,
                ads,
                config_snapshot_uri=snapshot_uri,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        print(f"{DAG_ID}: ad creative registry sync complete {result}")
        return result

    wait_for_campaign_config = ExternalTaskSensor(
        task_id="wait_for_facebook_campaign_config_update",
        external_dag_id=CAMPAIGN_CONFIG_DAG_ID,
        external_task_id=None,
        execution_date_fn=campaign_config_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=3 * 60 * 60,
    )
    config_task = log_config_source(config_source)
    sync_task = sync_object_properties(config_source)
    adset_config_task = sync_adset_configs(config_source)
    ad_creative_task = sync_ad_creative_media(config_source)
    wait_for_campaign_config >> config_task >> sync_task >> adset_config_task >> ad_creative_task


def _config_source_for_display() -> dict[str, Any]:
    source: dict[str, Any] = {
        "pointer_uri": "",
        "pointer_link": "",
        "snapshot_uri": "",
        "snapshot_link": "",
        "snapshot": {},
        "error": "",
    }
    try:
        import google.auth  # type: ignore[import-not-found]
        from google.cloud import storage  # type: ignore[import-not-found]

        credentials, _project_id = google.auth.default()
        storage_client = storage.Client(credentials=credentials)
        pointer_uri, pointer = read_latest_snapshot_pointer(storage_client, CONFIG_GCS_PREFIX)
        snapshot_uri = str(pointer["final_output"])
        snapshot = read_json_from_gcs(storage_client, snapshot_uri)
        source.update(
            {
                "pointer_uri": pointer_uri,
                "pointer_link": gcs_console_link(pointer_uri),
                "snapshot_uri": snapshot_uri,
                "snapshot_link": gcs_console_link(snapshot_uri),
                "snapshot": snapshot,
            }
        )
    except Exception as exc:
        source["error"] = str(exc)
    return source


def _property_account_ids(snapshot: dict[str, Any]) -> list[str]:
    active_accounts = {
        ensure_act_prefix(account_id)
        for account_id in account_ids_from_text(
            env_config_value(ACTIVE_ACCOUNTS_ENV)
        )
    }
    account_ids: list[str] = []
    for account_id, account in sorted(snapshot.get("accounts", {}).items()):
        resolved = ensure_act_prefix(str(account.get("id") or account_id))
        if active_accounts and resolved not in active_accounts:
            continue
        account_ids.append(resolved)
    return account_ids


meta_object_property_sync()
