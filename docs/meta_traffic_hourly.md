# Meta Traffic Hourly DAG

`meta_traffic_hourly` pulls Meta campaign, adset, and ad/creative traffic metrics
for the objects that are currently relevant, writes the raw daily-to-date pulls
into analytics snapshot tables, then writes 4-hour delta rows by subtracting the
previous same-day snapshot.

The DAG is driven by the campaign config snapshot produced by
`facebook_campaign_config_update`. That config snapshot controls which accounts
and adsets appear in the Airflow graph, so the DAG can show work grouped by Meta
account and adset instead of running one large traffic import task.

## Top-Level Flow

1. The DAG runs every 4 hours.
2. It waits for the matching `facebook_campaign_config_update` DAG run to finish
   successfully.
3. At DAG parse time, it reads the latest campaign config pointer from GCS.
4. It resolves that pointer to the campaign config snapshot JSON in GCS.
5. It filters accounts using `facebook_active_accounts` or
   `FACEBOOK_ACTIVE_ACCOUNTS`.
6. It filters adsets using status and `FACEBOOK_TRAFFIC_LOOKUP_WINDOWS`.
7. It creates Airflow task groups by account, then by adset.
8. Each campaign task group pulls campaign-level metrics from Meta.
9. Each adset task group pulls adset-level metrics and ad/creative metrics from Meta.
10. The raw current pulls are inserted into the campaign, adset, and ad snapshot
    tables.
11. The DAG finds the previous same-day snapshot rows for the same objects and
    writes metric deltas into the matching hourly tables.

## Schedule And Dependency

The DAG is scheduled with `schedule=timedelta(hours=4)`, matching
`facebook_campaign_config_update`.

Before any traffic work starts, `wait_for_facebook_campaign_config_update` waits
for the corresponding `facebook_campaign_config_update` 4-hour report bucket to
succeed. Manual runs are floored to the same `00/04/08/12/16/20` Pacific bucket,
so a `16:48` traffic run waits on the `16:00` campaign config run instead of a
non-bucketed `16:48` logical date.

The sensor runs in `reschedule` mode, so it does not hold a worker slot while it
waits.

## Config Snapshot Source

The visible DAG hierarchy is based on the latest successful campaign config
snapshot:

```text
gs://airflow-run-us-west2/facebook_campaign_config_update/latest_success.json
```

That pointer contains `final_output`, which points to the actual snapshot file:

```text
gs://airflow-run-us-west2/facebook_campaign_config_update/<date>/<datetime>/snapshot.json
```

The DAG logs both the pointer URI and the resolved snapshot URI, plus Google
Cloud Console links, so it is easy to open the exact config input used to build
the displayed task hierarchy.

If the GCS config cannot be read while Airflow parses the DAG, the DAG still
imports. In that case it shows a fallback task, logs the config read error, and
raises at runtime instead of breaking scheduler parsing.

## Account Selection

The config snapshot has an `accounts` object keyed by Meta ad account id, for
example:

```json
{
  "accounts": {
    "act_1986518498656337": {
      "id": "act_1986518498656337",
      "status": 1,
      "timezone_name": "America/Los_Angeles",
      "campaigns": []
    }
  }
}
```

`meta_traffic_hourly` filters those accounts through the active account list.
The active account list is read from Airflow Variable `facebook_active_accounts`,
falling back to environment variable `FACEBOOK_ACTIVE_ACCOUNTS`.

The value is a comma-separated string:

```text
act_1986518498656337, act_4157857287789311
```

The DAG splits on commas, trims whitespace, normalizes ids to Meta's `act_`
format, and keeps only matching accounts from the config snapshot. If the active
account list contains two accounts, only those two accounts are used for traffic
work.

## Campaign And Adset Selection

Inside each selected account, the DAG walks:

```text
account -> campaigns -> adsets
```

Campaigns and adsets are included when either condition is true:

- `status == "ACTIVE"`
- the object is not active, but `updated_at` is within the lookup window

The lookup window is controlled by `FACEBOOK_TRAFFIC_LOOKUP_WINDOWS`, defaulting
to `3` days. This keeps recently changed paused or inactive campaigns and adsets
in the import long enough to capture late or changing metrics, without pulling
old inactive objects forever.

Each selected account carries its campaigns, each campaign carries its selected
adsets, and each adset carries config ad metadata such as `creative_id` when it
is available from the campaign config snapshot.

## Airflow Display

The DAG creates one account task group per selected account:

```text
account_act_4157857287789311
```

Inside each account group, it creates one campaign task group per selected
campaign, then one adset task group per selected adset:

```text
campaign_6973015953052
  adset_6973015953252
```

This gives the Airflow graph a hierarchy that matches the campaign config data:

```text
wait_for_facebook_campaign_config_update
  -> log_campaign_config_source
    -> account_<account_id>
      -> campaign_<campaign_id>
        -> pull_campaign_metrics
        -> write_campaign_snapshot_rows
        -> write_campaign_delta_rows
        -> adset_<adset_id>
          -> pull_adset_metrics
          -> write_adset_snapshot_rows
          -> write_adset_delta_rows
          -> pull_ad_metrics
          -> write_ad_snapshot_rows
          -> write_ad_delta_rows
```

Because this is a parse-time graph, the displayed accounts, campaigns, and
adsets reflect the latest campaign config snapshot available when the scheduler
parsed the DAG.

## Pull Raw Metrics

Each campaign group starts with `pull_campaign_metrics`, which reads campaign
daily-to-date insights for the DAG's metric date:

```text
/{campaign_id}/insights?level=campaign
```

Each adset group then reads adset daily-to-date insights:

```text
/{adset_id}/insights?level=adset
```

Finally, each adset group reads ad-level insights. These rows are also the
creative snapshot surface because the existing ad tables include `creative_id`:

```text
/{adset_id}/insights?level=ad
```

The tasks read the Meta access token from Airflow Variable `meta_access_token`,
falling back to `META_ACCESS_TOKEN`. The requests use Meta's daily reporting
window for the current report date because Meta does not expose true hourly
insights for these endpoints. Pulled fields include:

- `campaign_id`
- `campaign_name`
- `adset_id`
- `adset_name`
- `ad_id`
- `ad_name`
- `impressions`
- `clicks`
- `spend`
- `reach`
- `frequency`
- `ctr`
- `cpc`
- `cpm`
- Meta repeated metric arrays such as `actions` and `conversions`

The page size defaults to `500` and can be overridden with
`META_GRAPH_PAGE_LIMIT`.

The tasks return compact snapshots containing the metric date, account id,
object ids, config snapshot URI, and raw insights rows.

## Write Snapshot Rows

The snapshot tasks store raw current pulls in:

- `marketing.meta_campaign_snapshot_metric`
- `marketing.meta_adset_snapshot_metric`
- `marketing.meta_ad_snapshot_metric`

The task uses:

```python
PostgresHook(postgres_conn_id="merino_analytics")
```

For each insight row, the DAG creates one snapshot row with:

- deterministic `snapshot_run_id`
- `snapshot_at` from the pull generation time
- `partition_hour` from the Airflow logical date, floored to the 4-hour bucket in
  `META_REPORT_TIMEZONE` (default `America/Los_Angeles`)
- fixed source fields: `company="merino"`, `platform="meta"`,
  `source="facebook"`
- account, campaign, adset, ad, and creative identity when present for that
  snapshot level
- scalar traffic metrics such as impressions, clicks, spend, reach, frequency,
  CTR, CPC, and CPM

Rows are inserted with `ON CONFLICT DO NOTHING`, so retrying the task does not
duplicate already inserted snapshot rows.

Each snapshot task returns the `snapshot_run_id`, level, object ids, and inserted
row count for the delta write step.

## Write Hourly Rows

`write_*_delta_rows` converts the current snapshots into 4-hour delta rows in:

- `marketing.meta_campaign_hourly_metric`
- `marketing.meta_adset_hourly_metric`
- `marketing.meta_ad_hourly_metric`

For every current snapshot row in the current `snapshot_run_id`, the DAG looks up
the most recent previous snapshot row for the same report date with the same
business key:

- company
- platform
- source
- source account
- campaign id
- adset id, for adset and ad rows
- ad id, for ad rows
- breakdown key
- attribution window

It then subtracts the previous values from the current values for the scalar
metrics currently handled by the DAG:

- impressions
- clicks
- spend
- reach
- frequency
- ctr
- cpc
- cpm

On the first run of the report day, where the Airflow logical date is hour `00`
in `META_REPORT_TIMEZONE`, there is no earlier same-day snapshot to subtract, so
the delta rows equal the snapshot rows. Later 4-hour runs subtract the previous
same-day snapshot.

If a run's 4-hour report bucket is already older than the current
`META_REPORT_TIMEZONE` 4-hour bucket, the DAG skips delta writes for that run.
This covers manual or delayed historical runs: Meta returns the latest daily
total for the requested report date, not the value that existed at that old
4-hour boundary, so writing a delta would corrupt or backfill the wrong interval.
Snapshot rows can still be written for audit/debugging.

The results are inserted with deterministic `report_run_id` values. Inserts also
use `ON CONFLICT DO NOTHING` to keep task retries idempotent.

## Runtime Configuration

The DAG uses these Airflow Variables and environment variables:

```text
meta_access_token
META_ACCESS_TOKEN
facebook_active_accounts
FACEBOOK_ACTIVE_ACCOUNTS
FACEBOOK_TRAFFIC_LOOKUP_WINDOWS
META_GRAPH_PAGE_LIMIT
META_REPORT_TIMEZONE
```

The PostgreSQL connection id is:

```text
merino_analytics
```

## Why The DAG Is Data-Driven

The campaign config snapshot already knows which accounts, campaigns, adsets,
and ads exist. Reusing that snapshot avoids blindly pulling traffic for every
Meta account and every old adset on each run.

The result is smaller, more recoverable work:

- if one adset fails, that adset task can be retried without pulling the whole
  account again
- inactive adsets are still imported briefly after changes, controlled by the
  lookup window
- Airflow shows work grouped by account and adset, making failures easier to
  locate

## Current Limitations

The displayed hierarchy is based on the latest config snapshot at DAG parse
time. It is not a different static graph per historical run. If a historical run
must build tasks from the exact config snapshot for that logical date, the DAG
would need runtime dynamic task mapping instead of parse-time task groups.

The current delta write handles scalar metrics inserted into the snapshot tables.
Repeated Meta metric arrays such as `actions`, `action_values`,
`cost_per_action_type`, and `conversions` are present in the schema as JSONB
payload columns, but the DAG does not yet write them.
