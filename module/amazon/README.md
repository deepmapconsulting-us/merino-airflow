# Amazon jobs

`merino-amazon-jobs` is the runtime image for Amazon SP-API and Amazon Ads
ingestion. Airflow owns schedules and launches the image with
`KubernetesPodOperator`; this package owns API calls, parsing, and Postgres
writes.

Production image:

```text
us-west2-docker.pkg.dev/merino-agent/merino/merino-amazon-jobs:0.1.7
```

Build and push from the merino repo root. The tag defaults to
`jobs/airflow/module/amazon/VERSION`.

```bash
bash scripts/deployment/docker-build-amazon-jobs.sh
bash scripts/deployment/docker-build-amazon-jobs.sh --no-push
bash scripts/deployment/docker-build-amazon-jobs.sh 0.1.7
```

After a tag bump, point `AMAZON_IMAGE` in `jobs/airflow/dags/amazon_k8s.py` at
the same version.

## Airflow configuration

The Amazon pods use Airflow connection `merino_analytics` and these Variables:

- Required shared SP-API credentials: `sp_api_client_id` and
  `sp_api_client_secret`
- Required regional SP-API refresh tokens: `sp_api_na_refresh_token` for US,
  CA, MX, and BR; `sp_api_oc_refresh_token` for AU
- Required seller identity: `amazon_account_key`, `amazon_seller_id`, and
  `amazon_seller_display_name`
- Required brand identity: `amazon_brand_key` and `amazon_brand_name`
- Optional Amazon Ads credentials: `amazon_ads_client_id`,
  `amazon_ads_client_secret`, and `amazon_ads_refresh_token`
- Optional marketplace-scoped Ads profiles: `amazon_ads_profile_id_us`,
  `amazon_ads_profile_id_ca`, `amazon_ads_profile_id_mx`,
  `amazon_ads_profile_id_br`, and `amazon_ads_profile_id_au`

Amazon Ads ingestion is enabled per marketplace by setting its profile
Variable. When any profile is configured, all three Amazon Ads credential
Variables are required.

Credentials are passed as pod environment variables. DAG commands do not echo
them or enable shell tracing.

Local Sales & Traffic backfill (`init_batch_job/scripts/load_amazon_sales_traffic_sample.sh`)
loads the regional tokens from GSM
(`airflow-variables-sp_api_na_refresh_token` and
`airflow-variables-sp_api_oc_refresh_token`), falling back to
`terraform/.secrets/amazon_giagio.env`, and builds `AMAZON_DATABASE_URL` via the
Metabase Postgres port-forward. Giagio brand/account defaults are applied
automatically. `AMAZON_SELLER_ID` is loaded from GSM
(`airflow-variables-sp_api_seller_id`, with `amazon_seller_id` as alias).

Until the NA-specific secret is provisioned, Airflow and the local loader accept
`airflow-variables-sp_api_refresh_token` as the NA token only. AU never falls
back to that token.

`OC` is the credential group for Australia. Amazon's SDK and database still use
the official `FE` endpoint region for AU.

Each seller account belongs to one brand. Giagio uses `giagio` / `Giagio`.
A second brand with separate Seller Central credentials must use a distinct
account key, seller ID, brand key, and brand name.

## Production DAGs

- `amazon_sales_traffic`: daily at 08:00 UTC; refreshes the latest three
  complete days for PARENT, CHILD, and SKU.
- `amazon_inventory`: daily at 09:00 UTC; runs listings, FBA inventory, then
  inventory age for US, CA, MX, BR, and AU. These current observed snapshots
  use the run's `data_interval_end` calendar date.
- `amazon_orders`: daily at 10:00 UTC; reloads Orders API 2026 data with a
  three-day overlap.
- `amazon_brand_analytics`: Mondays at 11:00 UTC; loads the previous complete
  Sunday-Saturday week.
- `amazon_ads`: daily at 12:00 UTC; refreshes the latest 14 complete days.
  It considers all five marketplaces and skips any marketplace without its own
  configured Ads profile; profiles are never reused across marketplaces.

All DAGs use `max_active_runs=1`, retry failed pods twice, and disable catchup.
The four SP-API DAGs (`amazon_sales_traffic`, `amazon_inventory`,
`amazon_orders`, `amazon_brand_analytics`) also share Airflow pool
`amazon_sp_api` (1 slot) and a Redis job lock so they cannot all call
`createReport` at once. Amazon Ads does not use that pool.

Pods receive MCP Redis (`MERINO_REDIS_HOST` plus
`merino-mcp-redis-password`) so the lock and `createReport` pacing
(1 request / 60s globally, 3 Sales & Traffic reports / 5 minutes) survive
across pods. On 429 the runtime waits 60s, then doubles.

Manual triggers accept `start`, `end`, and `marketplaces` where the runtime
supports them. `marketplaces` can be a comma-separated string or a JSON list.
Sales & Traffic also accepts `overwrite`.

```json
{
  "start": "2026-06-01",
  "end": "2026-06-14",
  "marketplaces": ["US", "CA"],
  "overwrite": true
}
```

Inventory accepts `end` or `snapshot_date` for the snapshot date and
`marketplaces` to select a subset of US, CA, MX, BR, and AU.

## Console entrypoints

The image exposes:

```text
merino-amazon-with-lock
merino-amazon-jobs
merino-amazon-listings
merino-amazon-fba-inventory
merino-amazon-fba-inventory-age
merino-amazon-orders
merino-amazon-brand-analytics
merino-amazon-ads
```
