from __future__ import annotations

import json
from math import isclose
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dbt_dag_opt.cli import app
from dbt_dag_opt.cost import (
    CostInputs,
    compute_cost,
    cost_inputs_from_replay,
    credits_per_hour_for,
)
from dbt_dag_opt.errors import InvalidArtifactError
from dbt_dag_opt.models import DagArtifacts
from dbt_dag_opt.replay import build_replay

runner = CliRunner()


def test_credits_per_hour_table() -> None:
    assert credits_per_hour_for("XS") == 1.0
    assert credits_per_hour_for("L") == 8.0
    assert credits_per_hour_for("6XL") == 512.0
    # case + punctuation tolerance
    assert credits_per_hour_for("l") == 8.0
    assert credits_per_hour_for("2xl") == 32.0
    assert credits_per_hour_for("X-Large") == 16.0
    assert credits_per_hour_for("2X-LARGE") == 32.0
    assert credits_per_hour_for("large") == 8.0
    with pytest.raises(InvalidArtifactError):
        credits_per_hour_for("GIGANTIC")


def test_compute_cost_synthetic_min_billing(
    synthetic_replay_artifacts: DagArtifacts,
) -> None:
    """Synthetic fixture: 40s wall, 50 CPU-sec, 2 threads, 40s critical path.

    Min-billing raises billed wall-clock AND the floor to 60s each, so
    headroom is zero (the run is already at the billing floor) while the
    waste_fraction based on 40s wall-clock still shows 30/80 = 37.5%.
    """
    report = build_replay(synthetic_replay_artifacts)
    inputs = cost_inputs_from_replay(
        report,
        warehouse_size="XS",
        credits_per_hour=None,
        rate_per_credit_usd=2.0,
        apply_minimum_billing=True,
    )
    cost = compute_cost(inputs)

    assert cost.billed_seconds == 60.0
    assert cost.min_billing_applied is True
    assert isclose(cost.rate_per_second_usd, 2.0 / 3600.0)
    # 60 * (1 * 2 / 3600) = 0.0333...
    assert isclose(cost.run_cost_usd, 60.0 * (2.0 / 3600.0))
    assert isclose(cost.floor_cost_usd, cost.run_cost_usd)
    assert isclose(cost.headroom_usd, 0.0, abs_tol=1e-12)
    assert cost.idle_thread_seconds == pytest.approx(30.0)
    assert cost.waste_fraction == pytest.approx(0.375)
    assert cost.idle_cost_usd == pytest.approx(cost.run_cost_usd * 0.375)
    assert inputs.warehouse_size_label == "XS"


def test_compute_cost_no_min_billing(synthetic_replay_artifacts: DagArtifacts) -> None:
    report = build_replay(synthetic_replay_artifacts)
    inputs = cost_inputs_from_replay(
        report,
        warehouse_size="XS",
        credits_per_hour=None,
        rate_per_credit_usd=2.0,
        apply_minimum_billing=False,
    )
    cost = compute_cost(inputs)

    assert cost.billed_seconds == pytest.approx(40.0)
    assert cost.min_billing_applied is False
    assert cost.run_cost_usd == pytest.approx(40.0 * (2.0 / 3600.0))
    # floor == wall_clock here since critical path == wall_clock
    assert cost.floor_cost_usd == pytest.approx(cost.run_cost_usd)
    assert cost.headroom_usd == pytest.approx(0.0, abs=1e-12)


def test_compute_cost_with_headroom() -> None:
    """1-hour run on L with critical path = half of wall-clock should show
    exactly 50% headroom and 75% thread-idleness waste."""
    inputs = CostInputs(
        wall_clock_seconds=3600.0,
        total_cpu_seconds=3600.0,
        thread_count=4,
        critical_path_seconds=1800.0,
        credits_per_hour=8.0,
        rate_per_credit_usd=2.0,
        apply_minimum_billing=True,
        warehouse_size_label="L",
    )
    cost = compute_cost(inputs)

    assert cost.min_billing_applied is False  # wall >> 60s
    assert cost.run_cost_usd == pytest.approx(16.0)
    assert cost.floor_cost_usd == pytest.approx(8.0)
    assert cost.headroom_usd == pytest.approx(8.0)
    assert cost.idle_thread_seconds == pytest.approx(10800.0)
    assert cost.waste_fraction == pytest.approx(0.75)
    assert cost.idle_cost_usd == pytest.approx(12.0)


def test_cli_replay_with_warehouse_size(
    dbt_dugout_manifest_path: Path, dbt_dugout_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "replay",
            "--manifest",
            str(dbt_dugout_manifest_path),
            "--run-results",
            str(dbt_dugout_run_results_path),
            "--warehouse-size",
            "L",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "Cost estimate" in result.stdout
    assert "Run cost" in result.stdout
    assert "Critical-path floor" in result.stdout
    assert "Headroom" in result.stdout
    assert "Idle cost" in result.stdout
    # dbt_dugout's wall-clock is sub-second; min billing should kick in
    assert "raised from" in result.stdout


def test_cli_replay_json_cost_key(
    dbt_dugout_manifest_path: Path, dbt_dugout_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "replay",
            "--manifest",
            str(dbt_dugout_manifest_path),
            "--run-results",
            str(dbt_dugout_run_results_path),
            "--warehouse-size",
            "L",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    cost = payload["cost"]
    assert cost["warehouse_size"] == "L"
    assert cost["credits_per_hour"] == 8.0
    assert cost["rate_per_credit_usd"] == 2.0
    assert cost["min_billing_applied"] is True
    assert cost["billed_seconds"] == pytest.approx(60.0)
    assert cost["run_cost_usd"] > 0


def test_cli_rate_without_size_fails(
    dbt_dugout_manifest_path: Path, dbt_dugout_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "replay",
            "--manifest",
            str(dbt_dugout_manifest_path),
            "--run-results",
            str(dbt_dugout_run_results_path),
            "--rate-per-credit",
            "3.0",
        ],
    )
    assert result.exit_code != 0
    combined = (result.stdout + (result.stderr or "")).lower()
    assert "--warehouse-size" in combined or "warehouse-size" in combined
