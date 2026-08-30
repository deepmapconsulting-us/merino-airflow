# Merino Airflow Structure

For **DAG ordering, cross-DAG sensors, task descriptions, and mermaid graphs**, see
[`docs/dags.md`](dags.md).

This repo is the Airflow orchestration repo for Merino jobs. Airflow owns the
schedules and task wiring. Platform-specific data pull code lives in module
images that Airflow launches as Kubernetes pods.

## Repository Layout

```text
dags/
  Airflow DAG definitions. These are synced into Airflow by git-sync and should
  stay focused on orchestration.

module/
  Platform-specific job images and Python packages. These are built and pushed
  as container images, then invoked from DAG tasks.

module/meta/
  Meta Marketing API job image. Contains the Meta SDK dependency and
  merino_meta_jobs package.

module/amazon/
  Amazon SP-API and Amazon Ads job image. Contains the vendor clients,
  merino_amazon_jobs package, and source-specific console entrypoints.
```

## Main Idea

Use one Airflow repo, but separate runtime images by data platform.

Airflow should not install every vendor SDK into the scheduler or worker image.
Instead, each platform gets a slim image with only the dependencies it needs:

- `module/meta` builds a Meta image, for example `merino-meta-jobs`.
- `module/amazon` builds `merino-amazon-jobs`.
- DAGs call those images with `KubernetesPodOperator` or the equivalent pod
  execution operator.

The image is platform-specific, not DAG-specific. A single Meta image can expose
many Meta jobs, such as traffic, campaign insights, ad insights, or account
metadata. The DAG decides which one to run by passing the Python module and
arguments.

## Meta Job Pattern

The Meta image owns the Meta Marketing API runtime:

- official `facebook-business` SDK dependency
- Merino-specific package code under `merino_meta_jobs`
- entrypoints for different Meta pulls
- API pagination, request shaping, and output writing

The default image command can stay a smoke check, but real DAG tasks should
override the command and arguments to run a specific job module.

Example shape:

```text
python -m merino_meta_jobs.traffic --date {{ ds }}
python -m merino_meta_jobs.campaign_insights --start-date {{ ds }} --end-date {{ ds }}
python -m merino_meta_jobs.account_metadata
```

## DAG Pattern

DAG files under `dags/` should stay thin. Their job is to schedule work and pass
runtime parameters.

For a Meta task, the DAG should:

1. Select the pushed Meta image tag.
2. Start a Kubernetes pod with that image.
3. Pass the module name and job parameters as command arguments.
4. Mount or inject credentials through Airflow/Kubernetes secrets.
5. Let the code inside `module/meta` perform the actual Meta API pull.

Conceptually:

```python
KubernetesPodOperator(
    task_id="fetch_meta_traffic",
    image="REGION-docker.pkg.dev/PROJECT/REPO/merino-meta-jobs:TAG",
    cmds=["python", "-m", "merino_meta_jobs.traffic"],
    arguments=["--date", "{{ ds }}"],
)
```

## Why This Shape

This keeps responsibilities clear:

- Airflow DAGs define when jobs run and how tasks depend on each other.
- Module images define how each external platform is queried.
- Vendor SDKs stay out of the Airflow scheduler/worker image.
- Meta and Amazon can evolve independently without bloating one shared runtime.
- Multiple DAGs can reuse the same platform image with different parameters.

## Amazon Job Pattern

The Amazon image exposes source-specific entrypoints and is launched by five
thin production DAGs:

```text
amazon_sales_traffic    Daily three-day PARENT/CHILD/SKU refresh
amazon_inventory        Daily listings -> FBA inventory -> inventory age
amazon_orders           Daily Orders API 2026 overlap refresh
amazon_brand_analytics  Weekly previous complete Sunday-Saturday
amazon_ads              Daily attribution-window refresh
```

Shared pod configuration is in `dags/amazon_k8s.py`. It injects the
`merino_analytics` Airflow connection, SP-API Variables, seller identity, and
optional Amazon Ads Variables into
`us-west2-docker.pkg.dev/merino-agent/merino/merino-amazon-jobs:0.1.8`.
Seller identity Variables are required. Amazon Ads profile Variables are
marketplace-scoped so a profile cannot be reused for another country.
Credentials remain environment values and are not included in task commands.
SP-API credentials use `sp_api_na_refresh_token` for US, CA, MX, and BR, and
`sp_api_oc_refresh_token` for AU. OC is a credential group; AU continues to use
Amazon's official FE endpoint region.
SP-API pods take Airflow pool `amazon_sp_api` (1 slot), wrap the command with
`merino-amazon-with-lock`, and pace `createReport` through MCP Redis so the
four SP-API DAGs cannot exhaust the shared Reports quota at once. Amazon Ads
skips that lock. The inventory DAG records current observed snapshots against the
`data_interval_end` calendar date.
