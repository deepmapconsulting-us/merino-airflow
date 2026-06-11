# Meta Airflow DAGs — ordering, tasks, and dependencies

All Meta marketing DAGs share a **2-hour partition** in `America/Los_Angeles`
(see [`campaign_config_logical_date`](../dags/meta_gcs.py)). Downstream DAGs use
`ExternalTaskSensor` with `execution_date_fn=campaign_config_logical_date` so each
run waits for the **matching** `facebook_campaign_config_update` bucket—not merely
“the latest” run.

Shared helpers live in [`meta_gcs.py`](../dags/meta_gcs.py) (GCS pointers, tokens,
partition math) and [`meta_status.py`](../dags/meta_status.py) (Redis config cache,
`active_status` for metrics).

---

## Cross-DAG relationship (mermaid)

```mermaid
flowchart TB
  subgraph root["Root — every 2 hours"]
    FCC["facebook_campaign_config_update<br/>schedule: 0 */2 * * *"]
    FCC_T["sync_campaign_config"]
    FCC --> FCC_T
  end

  subgraph dim["Dimensions — every 2 hours"]
    MOPS["meta_object_property_sync<br/>schedule: 0 */2 * * *"]
    W1["wait_for_facebook_campaign_config_update"]
    L1["log_config_source"]
    SOP["sync_object_properties"]
    SAC["sync_adset_configs"]
    SCM["sync_ad_creative_media"]
    W1 --> L1 --> SOP --> SAC --> SCM
  end

  subgraph hourly["Hourly metrics — every 2 hours"]
    MAHM["meta_ad_hourly_metric<br/>schedule: 0 */2 * * *"]
    W2a["wait_for_facebook_campaign_config_update"]
    W2b["wait_for_meta_object_property_sync<br/>(sync_object_properties)"]
    L2["log_campaign_config_source"]
    MAHM_W["pull / write per campaign"]
    W2a --> W2b --> L2 --> MAHM_W
  end

  subgraph daily_traffic["Daily traffic — twice daily"]
    MTS["meta_traffic_snapshot<br/>schedule: 0 2,14 * * *"]
    W3a["wait_for_facebook_campaign_config_update"]
    W3b["wait_for_meta_object_property_sync<br/>(sync_object_properties)"]
    L3["log_campaign_config_source"]
    MTS_W["pull / write campaign → adset → ad → gender/age"]
    W3a --> W3b --> L3 --> MTS_W
  end

  subgraph media["Creative media — daily"]
    MCMA["meta_creative_media_analysis<br/>schedule: 0 3 * * *"]
    W4a["wait_for_facebook_campaign_config_update"]
    W4b["wait_for_meta_object_property_sync<br/>(sync_object_properties)"]
    W4c["wait_for_meta_ad_creative_registry<br/>(sync_ad_creative_media)"]
    L4["log_campaign_config_source"]
    MCMA_W["download_ad_* → analyze_ad_* per ad"]
    W4a --> W4b --> W4c --> L4 --> MCMA_W
  end

  FCC_T -.->|ExternalTaskSensor<br/>whole DAG success| W1
  FCC_T -.-> W2a
  FCC_T -.-> W3a
  FCC_T -.-> W4a

  SOP -.->|ExternalTaskSensor| W2b
  SOP -.-> W3b
  SOP -.-> W4b

  SCM -.->|ExternalTaskSensor| W4c

  FCC_T -->|"GCS snapshot + Redis cache"| MOPS
  FCC_T -->|"GCS snapshot (read at parse)"| MAHM
  FCC_T -->|"GCS snapshot (read at parse)"| MTS
  FCC_T -->|"GCS snapshot (read at parse)"| MCMA
```

### Data flow (what each layer produces)

| Layer | DAG | Primary outputs |
| --- | --- | --- |
| Config | [`facebook_campaign_config_update`](../dags/facebook_campaign_config_update.py) | GCS `facebook_campaign_config_update/.../snapshot.json`, `latest_success.json`, Redis `meta:campaign_config:*` |
| Dimensions | [`meta_object_property_sync`](../dags/meta_object_property_sync.py) | Postgres `marketing.meta_campaign`, `meta_adset`, `meta_ad`, `meta_adset_config`, `meta_ad_creative` |
| Hourly metrics | [`meta_ad_hourly_metric`](../dags/meta_ad_hourly_metric.py) | Postgres `marketing.meta_ad_hourly_metric` |
| Daily snapshots | [`meta_traffic_snapshot`](../dags/meta_traffic_snapshot.py) | Postgres `marketing.meta_*_daily_snapshot`, `meta_*_region_daily_snapshot`, `meta_ad_gender_age_daily_snapshot` |
| Media analysis | [`meta_creative_media_analysis`](../dags/meta_creative_media_analysis.py) | GCS `meta_analysis/...` via MCP, Postgres `marketing.creative_media_analysis_*` |

---

## DAG reference

### 1. `facebook_campaign_config_update`

**Code:** [`dags/facebook_campaign_config_update.py`](../dags/facebook_campaign_config_update.py)  
**Schedule:** `0 */2 * * *` (12×/day, on the hour, Pacific)  
**Module:** [`merino_meta_jobs/account_snapshot.py`](../module/meta/merino_meta_jobs/account_snapshot.py)

```mermaid
flowchart LR
  A["sync_campaign_config"] --> B["GCS snapshot + latest pointer + Redis cache"]
```

| Task | What it does |
| --- | --- |
| `sync_campaign_config` | Pulls current Meta account → campaign → adset → ad tree via Graph API; writes partitioned snapshot to GCS; publishes `latest_success.json`; caches snapshot in Redis via [`cache_config_snapshot`](../dags/meta_status.py). |

**Downstream:** Every other Meta DAG either sensors this run or reads the GCS pointer / snapshot JSON.

---

### 2. `meta_object_property_sync`

**Code:** [`dags/meta_object_property_sync.py`](../dags/meta_object_property_sync.py)  
**Schedule:** `0 */2 * * *`  
**Modules:** [`object_property.py`](../module/meta/merino_meta_jobs/object_property.py), [`adset_config.py`](../module/meta/merino_meta_jobs/adset_config.py), [`ad_creative.py`](../module/meta/merino_meta_jobs/ad_creative.py)

```mermaid
flowchart LR
  W["wait_for_facebook_campaign_config_update"] --> L["log_config_source"]
  L --> SOP["sync_object_properties"]
  SOP --> SAC["sync_adset_configs"]
  SAC --> SCM["sync_ad_creative_media"]
```

| Task | What it does |
| --- | --- |
| `wait_for_facebook_campaign_config_update` | Waits for the aligned `facebook_campaign_config_update` run (whole DAG success). |
| `log_config_source` | Logs GCS pointer / snapshot URI and object counts (parse-time snapshot). |
| `sync_object_properties` | Upserts campaign, adset, ad dimensions into Postgres (incremental from GCS snapshot, or full init when `meta_object_property_full_init=true`). **Metric DAGs sensor this task.** |
| `sync_adset_configs` | Fetches ad set `targeting` from Graph; SCD Type 2 history in `marketing.meta_adset_config`. |
| `sync_ad_creative_media` | Fetches `/adcreatives` per ad; upserts `marketing.meta_ad_creative` (`has_video`, `has_image`, `video_ids`). **Not a media download**—Graph metadata only. |

---

### 3. `meta_ad_hourly_metric`

**Code:** [`dags/meta_ad_hourly_metric.py`](../dags/meta_ad_hourly_metric.py)  
**Schedule:** `0 */2 * * *`  
**Module:** [`traffic.py`](../module/meta/merino_meta_jobs/traffic.py)

```mermaid
flowchart TB
  W1["wait_for_facebook_campaign_config_update"] --> W2["wait_for_meta_object_property_sync"]
  W2 --> L["log_campaign_config_source"]
  L --> TG["TaskGroup account_* / campaign_*"]
  TG --> P["pull_campaign_ad_hourly_metrics"]
  P --> W["write_campaign_ad_hourly_metrics"]
```

| Task | What it does |
| --- | --- |
| `wait_for_*` | Same 2-hour alignment; property sensor targets `sync_object_properties` only (does not wait for adset config or creative registry). |
| `log_campaign_config_source` | Logs filtered account/campaign/adset counts from GCS config. |
| `pull_campaign_ad_hourly_metrics` | Meta Insights API, ad level, advertiser-time-zone hourly breakdown for configured adsets. |
| `write_campaign_ad_hourly_metrics` | Upserts `marketing.meta_ad_hourly_metric`. |

Task groups: `account_{id}` → `campaign_{id}` → pull/write pair.

---

### 4. `meta_traffic_snapshot`

**Code:** [`dags/meta_traffic_snapshot.py`](../dags/meta_traffic_snapshot.py)  
**Schedule:** `0 2,14 * * *` (02:00 and 14:00 Pacific)  
**Module:** [`traffic.py`](../module/meta/merino_meta_jobs/traffic.py)

```mermaid
flowchart TB
  W1["wait_for_facebook_campaign_config_update"] --> W2["wait_for_meta_object_property_sync"]
  W2 --> L["log_campaign_config_source"]
  L --> TG["TaskGroup account_* / campaign_*"]
  TG --> C["pull/write campaign snapshots"]
  TG --> CR["pull/write campaign region snapshots"]
  TG --> AS["pull/write adset snapshots"]
  TG --> ASR["pull/write adset region snapshots"]
  TG --> AD["pull/write ad snapshots"]
  TG --> ADR["pull/write ad region snapshots"]
  TG --> GA["pull/write ad gender/age snapshots"]
```

| Task pair (per campaign) | Postgres table |
| --- | --- |
| `pull_campaign_snapshots` → `write_campaign_snapshots` | `marketing.meta_campaign_daily_snapshot` |
| `pull_campaign_region_snapshots` → `write_campaign_region_snapshots` | `marketing.meta_campaign_region_daily_snapshot` |
| `pull_adset_snapshots` → `write_adset_snapshots` | `marketing.meta_adset_daily_snapshot` |
| `pull_adset_region_snapshots` → `write_adset_region_snapshots` | `marketing.meta_adset_region_daily_snapshot` |
| `pull_ad_snapshots` → `write_ad_snapshots` | `marketing.meta_ad_daily_snapshot` |
| `pull_ad_region_snapshots` → `write_ad_region_snapshots` | `marketing.meta_ad_region_daily_snapshot` |
| `pull_ad_gender_age_snapshots` → `write_ad_gender_age_snapshots` | `marketing.meta_ad_gender_age_daily_snapshot` |

Same external sensors as hourly metrics (`sync_object_properties` only).

---

### 5. `meta_creative_media_analysis`

**Code:** [`dags/meta_creative_media_analysis.py`](../dags/meta_creative_media_analysis.py)  
**Schedule:** `0 3 * * *` (daily 03:00 Pacific)  
**Module:** [`media_analysis.py`](../module/meta/merino_meta_jobs/media_analysis.py) (HTTP client to media-analysis-mcp)

```mermaid
flowchart TB
  W1["wait_for_facebook_campaign_config_update"] --> W2["wait_for_meta_object_property_sync"]
  W2 --> W3["wait_for_meta_ad_creative_registry"]
  W3 --> L["log_campaign_config_source"]
  L --> TG["TaskGroup account_* / campaign_* / adset_*"]
  TG --> D["download_ad_{ad_id}"]
  D --> A["analyze_ad_{ad_id}"]
  D --> A
```

| Task | What it does |
| --- | --- |
| `wait_for_meta_ad_creative_registry` | Waits for `sync_ad_creative_media` (creative **registry** in Postgres, not MCP download). |
| `download_ad_{ad_id}` | POST to media-analysis-mcp `/api/v1/download-ad-creative-assets` — downloads video/image assets, frames, optional GCS upload (`meta_analysis` bucket). |
| `analyze_ad_{ad_id}` | POST to `/api/v1/creative-media-analysis` — LLM analysis; upserts `marketing.creative_media_analysis_snapshot` and traffic link rows. |

Ad selection at parse time uses the GCS config snapshot ([`media_analysis_ads_from_adset`](../module/meta/merino_meta_jobs/traffic.py)), not `marketing.meta_ad_creative` (registry is ordering + Metabase; filtering on `has_video` / `has_image` is planned).

Per-ad chain: `download_ad_* >> analyze_ad_*`.

---

## ExternalTaskSensor cheat sheet

| Waiting DAG | Sensor task | Waits on DAG | Waits on task |
| --- | --- | --- | --- |
| `meta_object_property_sync` | `wait_for_facebook_campaign_config_update` | `facebook_campaign_config_update` | *(entire DAG)* |
| `meta_ad_hourly_metric` | `wait_for_facebook_campaign_config_update` | `facebook_campaign_config_update` | *(entire DAG)* |
| `meta_ad_hourly_metric` | `wait_for_meta_object_property_sync` | `meta_object_property_sync` | `sync_object_properties` |
| `meta_traffic_snapshot` | same as hourly | same | `sync_object_properties` |
| `meta_creative_media_analysis` | `wait_for_facebook_campaign_config_update` | `facebook_campaign_config_update` | *(entire DAG)* |
| `meta_creative_media_analysis` | `wait_for_meta_object_property_sync` | `meta_object_property_sync` | `sync_object_properties` |
| `meta_creative_media_analysis` | `wait_for_meta_ad_creative_registry` | `meta_object_property_sync` | `sync_ad_creative_media` |

Alignment function: [`campaign_config_logical_date`](../dags/meta_gcs.py) in every sensor above.

---

## Typical timeline (one 2-hour bucket)

```mermaid
gantt
  title Meta pipeline (example 02:00 Pacific bucket)
  dateFormat HH:mm
  axisFormat %H:%M

  section Config
  facebook_campaign_config_update     :a1, 02:00, 20m

  section Dimensions
  meta_object_property_sync           :a2, after a1, 45m

  section Metrics
  meta_ad_hourly_metric               :a3, after a2, 60m

  section Daily only
  meta_traffic_snapshot (if 02:00 run) :a4, after a2, 90m
  meta_creative_media_analysis (03:00) :a5, 03:00, 120m
```

- **02:00 bucket:** config → dimensions → hourly (and traffic snapshot when scheduled).
- **03:00 daily:** creative media analysis waits for the **02:00** config/dimension/creative-registry bucket via `campaign_config_logical_date`, then downloads and analyzes ads.

---

## Related docs

- Repo layout: [`structure.md`](structure.md)
- Legacy hourly traffic notes: [`meta_traffic_hourly.md`](meta_traffic_hourly.md) *(older DAG name; live DAG is `meta_ad_hourly_metric`)*
- Postgres tables: [`../../metabase_schema/docs/schema.md`](../../metabase_schema/docs/schema.md)
