from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import jinja2
import pendulum

AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
DAGS = AIRFLOW_ROOT / "dags"
sys.path.insert(0, str(DAGS))


class FakeOperator:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs

    def __rshift__(self, other: object) -> object:
        return other


def load_dag_module(name: str):
    mock_k8s = types.ModuleType("amazon_k8s")
    mock_k8s.amazon_pod = lambda **kwargs: FakeOperator(**kwargs)
    sys.modules["amazon_k8s"] = mock_k8s

    mock_airflow = types.ModuleType("airflow")
    mock_airflow_sdk = types.ModuleType("airflow.sdk")
    mock_airflow_sdk.dag = lambda **_kwargs: lambda fn: fn
    sys.modules.setdefault("airflow", mock_airflow)
    sys.modules["airflow.sdk"] = mock_airflow_sdk

    spec = importlib.util.spec_from_file_location(
        f"{name}_dag_for_test",
        DAGS / f"{name}.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def render(command: str, conf: dict[str, object] | None = None) -> str:
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    environment.globals["macros"] = SimpleNamespace(timedelta=timedelta)
    template = environment.from_string(command)
    return template.render(
        dag_run=SimpleNamespace(
            conf=conf or {},
            run_after=pendulum.datetime(2026, 8, 13, 9, 0, tz="UTC"),
        )
    )


class AmazonDagTest(unittest.TestCase):
    def test_sales_traffic_renders_three_day_all_granularity_refresh(self) -> None:
        module = load_dag_module("amazon_sales_traffic")

        command = render(module.SALES_TRAFFIC_COMMAND)

        self.assertIn('START_DATE="2026-08-10"', command)
        self.assertIn('END_DATE="2026-08-12"', command)
        self.assertIn(
            "--granularity PARENT --granularity CHILD --granularity SKU", command
        )
        self.assertIn("merino-amazon-jobs", command)

    def test_sales_traffic_manual_conf_overrides_window_marketplaces_and_overwrite(
        self,
    ) -> None:
        module = load_dag_module("amazon_sales_traffic")

        command = render(
            module.SALES_TRAFFIC_COMMAND,
            {
                "start": "2026-06-01",
                "end": "2026-06-14",
                "marketplaces": ["US", "CA"],
                "overwrite": True,
            },
        )

        self.assertIn('START_DATE="2026-06-01"', command)
        self.assertIn('END_DATE="2026-06-14"', command)
        self.assertIn('MARKETPLACES="US,CA"', command)
        self.assertIn("ARGS+=(--overwrite)", command)

    def test_inventory_commands_render_without_data_interval(self) -> None:
        module = load_dag_module("amazon_inventory")

        listings = render(module.inventory_command("merino-amazon-listings", "US"))
        inventory = render(
            module.inventory_command("merino-amazon-fba-inventory", "US")
        )
        age = render(module.inventory_command("merino-amazon-fba-inventory-age", "US"))

        for command, entrypoint in (
            (listings, "merino-amazon-listings"),
            (inventory, "merino-amazon-fba-inventory"),
            (age, "merino-amazon-fba-inventory-age"),
        ):
            self.assertIn('SNAPSHOT_DATE="2026-08-13"', command)
            self.assertIn(entrypoint, command)
            self.assertIn("--marketplace US", command)

    def test_inventory_marketplace_filter_skips_unselected_market(self) -> None:
        module = load_dag_module("amazon_inventory")

        command = render(
            module.inventory_command("merino-amazon-listings", "CA"),
            {"marketplaces": "US,MX"},
        )

        self.assertIn('MARKETPLACES="US,MX"', command)
        self.assertIn('case ",$MARKETPLACES," in', command)
        self.assertIn("*,CA,*)", command)

    def test_orders_renders_2026_incremental_overlap(self) -> None:
        module = load_dag_module("amazon_orders")

        command = render(module.ORDERS_COMMAND)

        self.assertIn('START_DATE="2026-08-09"', command)
        self.assertIn('END_DATE="2026-08-12"', command)
        self.assertIn("merino-amazon-orders", command)
        self.assertIn("--start-date", command)
        self.assertIn("--end-date", command)

    def test_brand_analytics_uses_previous_complete_sunday_saturday(self) -> None:
        module = load_dag_module("amazon_brand_analytics")

        command = render(module.BRAND_ANALYTICS_COMMAND)

        self.assertIn('START_DATE="2026-08-02"', command)
        self.assertIn('END_DATE="2026-08-08"', command)
        self.assertIn("merino-amazon-brand-analytics", command)
        self.assertIn("--period WEEK", command)

    def test_ads_renders_fourteen_day_refresh(self) -> None:
        module = load_dag_module("amazon_ads")

        command = render(module.ADS_COMMAND)

        self.assertIn('START_DATE="2026-07-30"', command)
        self.assertIn('END_DATE="2026-08-12"', command)
        self.assertIn("merino-amazon-ads", command)

    def test_ads_uses_only_marketplace_scoped_profiles(self) -> None:
        module = load_dag_module("amazon_ads")

        command = render(module.ADS_COMMAND)

        self.assertIn('MARKETPLACES="US,CA,MX,BR,AU"', command)
        self.assertIn(
            'PROFILE_VAR="AMAZON_ADS_PROFILE_ID_${MARKETPLACE}"',
            command,
        )
        self.assertIn('PROFILE_ID="${!PROFILE_VAR:-}"', command)
        self.assertIn('[[ -z "$PROFILE_ID" ]] && continue', command)
        self.assertIn('--profile-id "$PROFILE_ID"', command)
        self.assertNotIn("${AMAZON_ADS_PROFILE_ID:-", command)

    def test_windowed_dags_accept_manual_dates_and_marketplaces(self) -> None:
        conf = {
            "start": "2026-05-03",
            "end": "2026-05-09",
            "marketplaces": "CA,MX",
        }

        commands = (
            render(load_dag_module("amazon_orders").ORDERS_COMMAND, conf),
            render(
                load_dag_module("amazon_brand_analytics").BRAND_ANALYTICS_COMMAND,
                conf,
            ),
            render(load_dag_module("amazon_ads").ADS_COMMAND, conf),
        )

        for command in commands:
            self.assertIn('START_DATE="2026-05-03"', command)
            self.assertIn('END_DATE="2026-05-09"', command)
            self.assertIn('MARKETPLACES="CA,MX"', command)

    def test_all_dags_are_serial_and_have_expected_schedules(self) -> None:
        expected = {
            "amazon_sales_traffic.py": 'schedule="0 8 * * *"',
            "amazon_inventory.py": 'schedule="0 9 * * *"',
            "amazon_orders.py": 'schedule="0 10 * * *"',
            "amazon_brand_analytics.py": 'schedule="0 11 * * 1"',
            "amazon_ads.py": 'schedule="0 12 * * *"',
        }

        for filename, schedule in expected.items():
            source = (DAGS / filename).read_text(encoding="utf-8")
            self.assertIn(schedule, source)
            self.assertIn("max_active_runs=1", source)
            self.assertIn('"retries": 2', source)

    def test_inventory_dependencies_and_task_ids_are_deterministic(self) -> None:
        source = (DAGS / "amazon_inventory.py").read_text(encoding="utf-8")

        self.assertIn("for marketplace in MARKETPLACES:", source)
        self.assertIn('task_id=f"listings_{marketplace.lower()}"', source)
        self.assertIn('task_id=f"fba_inventory_{marketplace.lower()}"', source)
        self.assertIn('task_id=f"inventory_age_{marketplace.lower()}"', source)
        self.assertIn("listings >> inventory >> inventory_age", source)


def load_k8s_module():
    mock_kubernetes = types.ModuleType("kubernetes")
    mock_client = types.ModuleType("kubernetes.client")

    class FakeEnvVar:
        def __init__(
            self, name: str, value: str | None = None, **_kwargs: object
        ) -> None:
            self.name = name
            self.value = value

    class FakeResources:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

    mock_client.models = SimpleNamespace(
        V1EnvVar=FakeEnvVar,
        V1ResourceRequirements=FakeResources,
    )
    mock_kubernetes.client = mock_client
    sys.modules["kubernetes"] = mock_kubernetes
    sys.modules["kubernetes.client"] = mock_client

    class FakeKubernetesPodOperator:
        def __new__(cls, **kwargs: object) -> dict[str, object]:
            return dict(kwargs)

    mock_pod = types.ModuleType("airflow.providers.cncf.kubernetes.operators.pod")
    mock_pod.KubernetesPodOperator = FakeKubernetesPodOperator
    sys.modules["airflow.providers.cncf.kubernetes.operators.pod"] = mock_pod

    spec = importlib.util.spec_from_file_location(
        "amazon_k8s_for_test",
        DAGS / "amazon_k8s.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class AmazonKubernetesTest(unittest.TestCase):
    def test_pod_uses_production_image_and_connection(self) -> None:
        module = load_k8s_module()

        pod = module.amazon_pod(task_id="amazon_test")

        self.assertEqual(
            pod["image"],
            "us-west2-docker.pkg.dev/merino-agent/merino/merino-amazon-jobs:0.1.0",
        )
        self.assertEqual(pod["namespace"], "airflow")
        self.assertEqual(pod["service_account_name"], "merino-airflow-task-runner")
        self.assertTrue(pod["get_logs"])
        self.assertNotIn("secrets", pod)

    def test_env_injects_postgres_sp_api_identity_and_optional_ads_variables(
        self,
    ) -> None:
        module = load_k8s_module()

        env = {item.name: item.value for item in module.amazon_pod_env()}

        self.assertEqual(
            env["AMAZON_DATABASE_URL"], "{{ conn.merino_analytics.get_uri() }}"
        )
        self.assertEqual(env["SP_API_CLIENT_ID"], "{{ var.value.sp_api_client_id }}")
        self.assertEqual(
            env["SP_API_CLIENT_SECRET"], "{{ var.value.sp_api_client_secret }}"
        )
        self.assertEqual(
            env["SP_API_REFRESH_TOKEN"], "{{ var.value.sp_api_refresh_token }}"
        )
        self.assertEqual(
            env["AMAZON_ACCOUNT_KEY"],
            "{{ var.value.amazon_account_key }}",
        )
        self.assertEqual(
            env["AMAZON_SELLER_DISPLAY_NAME"],
            "{{ var.value.amazon_seller_display_name }}",
        )
        self.assertEqual(
            env["AMAZON_SELLER_ID"],
            "{{ var.value.amazon_seller_id }}",
        )
        self.assertEqual(
            env["AMAZON_BRAND_KEY"],
            "{{ var.value.amazon_brand_key }}",
        )
        self.assertEqual(
            env["AMAZON_BRAND_NAME"],
            "{{ var.value.amazon_brand_name }}",
        )
        self.assertEqual(
            env["AMAZON_ADS_CLIENT_ID"],
            "{{ var.value.get('amazon_ads_client_id', '') }}",
        )
        self.assertEqual(
            env["AMAZON_ADS_CLIENT_SECRET"],
            "{{ var.value.get('amazon_ads_client_secret', '') }}",
        )
        self.assertEqual(
            env["AMAZON_ADS_REFRESH_TOKEN"],
            "{{ var.value.get('amazon_ads_refresh_token', '') }}",
        )

    def test_env_injects_optional_marketplace_ads_profiles(self) -> None:
        module = load_k8s_module()

        env = {item.name: item.value for item in module.amazon_pod_env()}

        for marketplace in ("US", "CA", "MX", "BR", "AU"):
            self.assertEqual(
                env[f"AMAZON_ADS_PROFILE_ID_{marketplace}"],
                "{{ var.value.get('amazon_ads_profile_id_"
                f"{marketplace.lower()}', '') }}}}",
            )
        self.assertNotIn("AMAZON_ADS_PROFILE_ID", env)


if __name__ == "__main__":
    unittest.main()
