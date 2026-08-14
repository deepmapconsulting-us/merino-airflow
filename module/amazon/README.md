# Amazon jobs

`merino-amazon-jobs` is the runtime image for Amazon SP-API and Amazon Ads
ingestion. Airflow owns schedules and launches the image with
`KubernetesPodOperator`; this package owns API calls, parsing, and Postgres
writes.

Production image:

```text
us-west2-docker.pkg.dev/merino-agent/merino/merino-amazon-jobs:0.1.0
```

## Airflow configuration

The Amazon pods use Airflow connection `merino_analytics` and these Variables:

- Required SP-API credentials: `sp_api_client_id`, `sp_api_client_secret`,
  `sp_api_refresh_token`
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
merino-amazon-jobs
merino-amazon-listings
merino-amazon-fba-inventory
merino-amazon-fba-inventory-age
merino-amazon-orders
merino-amazon-brand-analytics
merino-amazon-ads
```
