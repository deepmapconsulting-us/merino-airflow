from pathlib import Path
import subprocess


SCRIPT = Path(__file__).resolve().parents[1] / "backfill_amazon.sh"


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_dry_run_triggers_historical_amazon_dags() -> None:
    result = run_script(
        "--start-date",
        "2026-05-21",
        "--end-date",
        "2026-08-20",
        "--marketplaces",
        "US,CA",
        "--dry-run",
    )

    assert result.returncode == 0
    assert "DRY RUN amazon_sales_traffic" in result.stdout
    assert "DRY RUN amazon_orders" in result.stdout
    assert "DRY RUN amazon_ads" in result.stdout
    assert '"marketplaces": ["US", "CA"]' in result.stdout
    assert '"overwrite": true' in result.stdout
    assert "manual__2026-05-21__2026-08-20__" in result.stdout


def test_rejects_end_date_before_start_date() -> None:
    result = run_script(
        "--start-date",
        "2026-08-20",
        "--end-date",
        "2026-05-21",
        "--dry-run",
    )

    assert result.returncode != 0
    assert "before start date" in result.stderr


def test_rejects_unknown_marketplace() -> None:
    result = run_script("--marketplaces", "US,UK", "--dry-run")

    assert result.returncode != 0
    assert "Unsupported marketplace: UK" in result.stderr
