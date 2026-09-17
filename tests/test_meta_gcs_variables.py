from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


class MetaGcsVariableTest(unittest.TestCase):
    def test_variable_get_falls_back_to_airflow_database(self) -> None:
        class SdkVariable:
            @staticmethod
            def get(_key: str) -> str:
                raise PermissionError("secret backend denied access")

        class DatabaseVariable:
            @staticmethod
            def get(key: str) -> str:
                return {"facebook_active_accounts": "act_4157857287789311"}[key]

        airflow = types.ModuleType("airflow")
        airflow.__path__ = []  # type: ignore[attr-defined]
        airflow_sdk = types.ModuleType("airflow.sdk")
        airflow_sdk.Variable = SdkVariable  # type: ignore[attr-defined]
        airflow_models = types.ModuleType("airflow.models")
        airflow_models.Variable = DatabaseVariable  # type: ignore[attr-defined]
        pendulum = types.ModuleType("pendulum")

        module_path = Path(__file__).resolve().parents[1] / "dags" / "meta_gcs.py"
        spec = importlib.util.spec_from_file_location("meta_gcs_variable_test_module", module_path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        with patch.dict(
            sys.modules,
            {
                "airflow": airflow,
                "airflow.sdk": airflow_sdk,
                "airflow.models": airflow_models,
                "pendulum": pendulum,
            },
        ):
            spec.loader.exec_module(module)
            value = module.variable_get("facebook_active_accounts")

        self.assertEqual(value, "act_4157857287789311")


if __name__ == "__main__":
    unittest.main()
