# Meta traffic snapshot backfill (Jul 18–28 gap)

Manual runbook to refill `marketing.meta_*_daily_snapshot` for **2026-07-18 through 2026-07-28**.

Question 187 (`7.sql`) reads **`marketing.meta_ad_daily_snapshot`** and sums
`action_values ->> 'purchase'` by `report_date`. Today the table jumps from
**2026-07-17** straight to **2026-07-29**:

| report_date | ad_rows | purchase_value |
|-------------|--------:|---------------:|
| 2026-07-17  | 630     | 1803.53        |
| 2026-07-18 … 2026-07-28 | *(missing)* | |
| 2026-07-29  | 627     | 1599.76        |

This doc plans work from **Postgres dimension tables** (`created_at`), ignores
**active status**, pulls **campaign → adset** first (ad level in a later phase),
and runs imports **one entity + one date at a time**.

---

## Why `reimport_meta_traffic_snapshot.sh` did not fill the gap

Manual DAG triggers use historical `logical_date` values (e.g. `2026-07-20
10:00 PT`). The DAG still has upstream sensors:

1. `wait_for_facebook_campaign_config_update`
2. `wait_for_meta_object_property_sync`

Those sensors look for **successful DAG runs on the same logical date**. There is
no `facebook_campaign_config_update` run for `2026-07-20 10:00`, so the sensor
fails and `sync_daily_reports_from_insights` never runs (`upstream_failed`).

The ingestion task itself does **not** need GCS config to discover traffic. It
calls `delivered_ad_hierarchy()`, which queries Meta Insights at account level
for each report date and builds campaign / adset / ad work from delivered rows.
GCS config is only used to enrich `creative_id`.

**Do not re-trigger the full DAG for historical dates.** Either:

- run only `sync_daily_reports_from_insights` via `airflow tasks test` (quick), or
- follow the dimension-table loop below (controlled, campaign/adset-first).

---

## Strategy

| Phase | Level | Target table | Fixes Q187? |
|-------|-------|----------------|-------------|
| 0 | — | — | Verify gap |
| 1 | Plan | — | List campaigns/adsets/ads from DB by `created_at` |
| 2 | Campaign | `marketing.meta_campaign_daily_snapshot` | No (sanity check spend/impressions) |
| 3 | Adset | `marketing.meta_adset_daily_snapshot` | No |
| 4 | Ad | `marketing.meta_ad_daily_snapshot` | **Yes** (`purchase` lives here) |

Phases 2–3 validate Meta API access and upsert path before the heavier ad pull.

**Rules**

- **Ignore `status`** on dimension rows (ACTIVE / PAUSED / ARCHIVED do not filter).
- **Include an entity on report date `D`** when `created_at::date <= D`.
- **Skip rows already present** unless `force=true`.
- **One batch at a time**: one `(report_date, account, campaign[, adset])` per API call so failures are easy to retry.

---

## Phase 0 — Verify the gap

```sql
SELECT
    report_date::date,
    COUNT(*) AS ad_rows,
    ROUND(SUM((action_values ->> 'purchase')::numeric), 2) AS purchase_value
FROM marketing.meta_ad_daily_snapshot
WHERE report_date BETWEEN '2026-07-15' AND '2026-07-31'
GROUP BY 1
ORDER BY 1;
```

Campaign / adset sanity (optional):

```sql
SELECT report_date::date, COUNT(*) FROM marketing.meta_campaign_daily_snapshot
WHERE report_date BETWEEN '2026-07-15' AND '2026-07-31' GROUP BY 1 ORDER BY 1;

SELECT report_date::date, COUNT(*) FROM marketing.meta_adset_daily_snapshot
WHERE report_date BETWEEN '2026-07-15' AND '2026-07-31' GROUP BY 1 ORDER BY 1;
```

---

## Phase 1 — List entities to pull (by `created_at`)

Dimension tables: `marketing.meta_campaign`, `marketing.meta_adset`,
`marketing.meta_ad`. Populated by `meta_object_property_sync` (latest properties
only; `created_at` is Meta object create time).

### 1a. Campaigns eligible for a date range

Replace `:start_date` / `:end_date` (e.g. `2026-07-18`, `2026-07-28`).

```sql
-- Campaigns that existed on or before end_date (ignore status)
SELECT
    c.source_account_id,
    c.campaign_id,
    c.name,
    c.status,
    c.created_at::date AS created_date,
    GREATEST(:start_date::date, c.created_at::date) AS pull_from,
    :end_date::date AS pull_through
FROM marketing.meta_campaign c
WHERE c.created_at IS NOT NULL
  AND c.created_at::date <= :end_date::date
ORDER BY c.source_account_id, c.campaign_id;
```

### 1b. Adsets eligible for the same range

```sql
SELECT
    a.source_account_id,
    a.campaign_id,
    a.adset_id,
    a.name,
    a.status,
    a.created_at::date AS created_date,
    GREATEST(:start_date::date, a.created_at::date, c.created_at::date) AS pull_from,
    :end_date::date AS pull_through
FROM marketing.meta_adset a
JOIN marketing.meta_campaign c ON c.campaign_id = a.campaign_id
WHERE a.created_at IS NOT NULL
  AND a.created_at::date <= :end_date::date
ORDER BY a.source_account_id, a.campaign_id, a.adset_id;
```

### 1c. Ads (phase 4 — for Q187)

```sql
SELECT
    ad.source_account_id,
    ad.campaign_id,
    ad.adset_id,
    ad.ad_id,
    ad.creative_id,
    ad.name,
    ad.status,
    ad.created_at::date AS created_date,
    GREATEST(
        :start_date::date,
        ad.created_at::date,
        a.created_at::date,
        c.created_at::date
    ) AS pull_from,
    :end_date::date AS pull_through
FROM marketing.meta_ad ad
JOIN marketing.meta_adset a ON a.adset_id = ad.adset_id
JOIN marketing.meta_campaign c ON c.campaign_id = ad.campaign_id
WHERE ad.created_at IS NOT NULL
  AND ad.created_at::date <= :end_date::date
ORDER BY ad.source_account_id, ad.campaign_id, ad.adset_id, ad.ad_id;
```

### 1d. Expand to a daily work queue (campaign example)

```sql
WITH params AS (
    SELECT DATE '2026-07-18' AS start_date, DATE '2026-07-28' AS end_date
),
campaigns AS (
    SELECT
        c.source_account_id,
        c.campaign_id,
        GREATEST(p.start_date, c.created_at::date) AS first_date,
        p.end_date AS last_date
    FROM marketing.meta_campaign c
    CROSS JOIN params p
    WHERE c.created_at IS NOT NULL
      AND c.created_at::date <= p.end_date
),
dates AS (
    SELECT generate_series(
        (SELECT start_date FROM params),
        (SELECT end_date FROM params),
        INTERVAL '1 day'
    )::date AS report_date
)
SELECT
    d.report_date,
    c.source_account_id,
    c.campaign_id
FROM campaigns c
CROSS JOIN dates d
WHERE d.report_date BETWEEN c.first_date AND c.last_date
  AND NOT EXISTS (
      SELECT 1
      FROM marketing.meta_campaign_daily_snapshot s
      WHERE s.report_date = d.report_date
        AND s.source_account_id = c.source_account_id
        AND s.campaign_id = c.campaign_id
  )
ORDER BY d.report_date, c.source_account_id, c.campaign_id;
```

Same pattern for adsets against `marketing.meta_adset_daily_snapshot` and ads
against `marketing.meta_ad_daily_snapshot`.

**Current inventory (2026-08-01):** 19 campaigns, 259 adsets in dimension
tables (all have `created_at`).

---

## Phase 2 — Campaign daily import (one-by-one)

### API

Uses `campaign_daily_snapshot()` in `merino_meta_jobs/traffic.py`:

```text
GET act_<id>/insights
  level=campaign
  time_range={since: D, until: D}
  filtering=[{field: campaign.id, operator: IN, value: [<campaign_id>]}]
```

### Upsert

`marketing.meta_campaign_daily_snapshot` via `upsert_daily_rows()` in
`traffic_snapshot_rows.py`. Conflict key:
`(report_date, source_account_id, campaign_id, attribution_window)`.

### Loop (pseudocode)

```text
FOR each row in campaign_work_queue ORDER BY report_date, source_account_id, campaign_id:
    snapshot = campaign_daily_snapshot(token, account_id, [campaign_id], report_date)
    rows = [campaign_row(snapshot, insight, account, run_id, report_date, status_resolver)]
    upsert_daily_rows(CAMPAIGN_DAILY_TABLE, ..., rows)
    LOG report_date, campaign_id, len(rows)
    SLEEP 1s   # optional rate-limit cushion
```

`status_resolver` for backfill: treat everything as `"active"` (same as
`meta_region_snapshot_backfill.BackfillStatusResolver`).

### Fast path — bypass sensors (full day, all levels)

Runs the existing DAG task without waiting on GCS / property sync. Pulls **both**
the logical day and previous day (DAG behavior).

```bash
POD=$(kubectl -n airflow get pods -l component=scheduler -o jsonpath='{.items[0].metadata.name}')

for DAY in 2026-07-18 2026-07-19 2026-07-20 2026-07-21 2026-07-22 \
           2026-07-23 2026-07-24 2026-07-25 2026-07-26 2026-07-27 2026-07-28; do
  echo "=== $DAY ==="
  kubectl -n airflow exec "$POD" -c scheduler -- \
    airflow tasks test meta_traffic_snapshot sync_daily_reports_from_insights "${DAY}T10:00:00-07:00"
done
```

Use this if you want campaign + adset + ad in one shot. Use the phased loop if
you need finer control or API errors on ad-level breakdowns.

---

## Phase 3 — Adset daily import (one-by-one)

### API

`adset_daily_snapshot(token, account_id, campaign_id, [adset_id], report_date)`

```text
GET <campaign_id>/insights
  level=adset
  filtering=[{field: adset.id, operator: IN, value: [<adset_id>]}]
```

### Upsert

`marketing.meta_adset_daily_snapshot`

### Loop

```text
FOR each row in adset_work_queue ORDER BY report_date, source_account_id, campaign_id, adset_id:
    snapshot = adset_daily_snapshot(token, account_id, campaign_id, [adset_id], report_date)
    rows = [adset_row(...)]
    upsert_daily_rows(ADSET_DAILY_TABLE, ..., rows)
```

Chunk size: **1 adset per call** for easiest retries; increase to 10–50 if stable.

---

## Phase 4 — Ad daily import (required for Q187)

`action_values.purchase` is stored at **ad** level only.

### API

`ad_daily_snapshot(token, account_id, campaign_id, [ad_id], report_date)`

### Loop

```text
FOR each row in ad_work_queue ORDER BY report_date, source_account_id, campaign_id, ad_id:
    snapshot = ad_daily_snapshot(token, account_id, campaign_id, [ad_id], report_date)
    rows = [ad_row(...)]   # needs adset_by_ad_id map from dimension hierarchy
    upsert_daily_rows(AD_DAILY_TABLE, ..., rows)
```

After phase 4, re-run the Phase 0 verification query. Expect 11 new dates
between 2026-07-17 and 2026-07-29.

---

## Existing code to reuse

| Piece | Location |
|-------|----------|
| Meta insight pulls | `jobs/airflow/module/meta/merino_meta_jobs/traffic.py` |
| Row builders + upsert | `jobs/airflow/module/meta/merino_meta_jobs/traffic_snapshot_rows.py` |
| Dimension load + `created_at` windows | `jobs/airflow/module/meta/merino_meta_jobs/region_snapshot_backfill.py` |
| Region backfill DAG (pattern) | `jobs/airflow/dags/meta_region_snapshot_backfill.py` |
| Scheduled DAG (do not trigger for history) | `jobs/airflow/dags/meta_traffic_snapshot.py` |

`meta_region_snapshot_backfill` already implements plan → loop → upsert for
**region** tables. A sibling DAG `meta_daily_snapshot_backfill` with
`levels: ["campaign", "adset"]` (then `"ad"`) and targets
`meta_campaign_daily_snapshot` / `meta_adset_daily_snapshot` /
`meta_ad_daily_snapshot` would mirror that pattern.

---

## Recommended execution order

1. **Confirm gap** (Phase 0 SQL).
2. **Export work queue** (Phase 1d) for `2026-07-18` … `2026-07-28`.
3. **Campaign loop** (Phase 2) — confirm rows land in
   `meta_campaign_daily_snapshot`.
4. **Adset loop** (Phase 3) — confirm `meta_adset_daily_snapshot`.
5. **Ad loop** (Phase 4) — fills Q187 / `meta_ad_daily_snapshot`.
6. **Re-verify** purchase totals per day.

**Shortcut:** if workers are healthy and you only need the gap closed quickly,
run the `airflow tasks test` loop in Phase 2 (bypasses sensors, includes ad
level). Use the phased DB-driven loop when you need to ignore status, respect
`created_at`, or recover from partial API failures.

---

## Prerequisites

- `META_ACCESS_TOKEN` or Airflow variable `meta_access_token` on the scheduler pod
- Postgres connection `merino_analytics` (Airflow) or local port-forward to Cloud SQL
- Dimension tables populated (`meta_object_property_sync` has run at least once)
- Optional: `FACEBOOK_ACTIVE_ACCOUNTS` if limiting accounts

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| DAG run `success` but gap unchanged | Only sensors/logging ran; sync skipped earlier | Use `tasks test` or phased import |
| `wait_for_facebook_campaign_config_update` failed | Historical logical_date has no config DAG run | Bypass sensors |
| Zero rows for a date | No delivery that day (Meta returned empty insights) | Expected for paused entities; check campaign-level first |
| Rows at campaign but not ad | Phase 4 not run yet | Run ad loop |
| `created_at` NULL on dimension row | Object stub without Graph detail | Include anyway with `created_at IS NULL` fallback to `start_date` |

---

## Related

- `service/airflow/reimport_meta_traffic_snapshot.sh` — triggers full DAG (hits sensor issue for historical dates)
- `jobs/airflow/docs/meta_traffic_hourly.md` — hourly delta DAG (different tables)
- Metabase Q187 SQL: `service/metabase/schema/versions/20260602/questions/7.sql`
