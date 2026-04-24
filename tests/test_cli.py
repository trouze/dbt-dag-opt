from __future__ import annotations

import json
from pathlib import Path

import responses
from typer.testing import CliRunner

from dbt_dag_opt import __version__
from dbt_dag_opt.cli import app

runner = CliRunner()


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_analyze_from_files_json(
    tiny_manifest_path: Path, tiny_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "analyze",
            "--manifest",
            str(tiny_manifest_path),
            "--run-results",
            str(tiny_run_results_path),
            "--format",
            "json",
            "--top",
            "1",
        ],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    # Top path is from raw.orders with distance 35
    (source, info), = payload.items()
    assert source == "source.demo.raw.orders"
    assert info["distance"] == 35.0


def test_analyze_from_files_table(
    tiny_manifest_path: Path, tiny_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "analyze",
            "--manifest",
            str(tiny_manifest_path),
            "--run-results",
            str(tiny_run_results_path),
            "--format",
            "table",
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "source.demo.raw.orders" in result.stdout
    # default table now includes the Bottleneck column (weights are passed by the CLI)
    assert "Bottleneck" in result.stdout


def test_analyze_show_path_renders_full_chain(
    tiny_manifest_path: Path, tiny_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "analyze",
            "--manifest",
            str(tiny_manifest_path),
            "--run-results",
            str(tiny_run_results_path),
            "--format",
            "table",
            "--show-path",
        ],
    )
    assert result.exit_code == 0, result.stdout
    # fact_orders is the terminal model in tiny fixture; intermediate stg_orders sits in between
    assert "stg_orders" in result.stdout
    assert "fact_orders" in result.stdout
    assert "→" in result.stdout


def test_analyze_output_file_writes_file(
    tmp_path: Path, tiny_manifest_path: Path, tiny_run_results_path: Path
) -> None:
    out = tmp_path / "paths.json"
    result = runner.invoke(
        app,
        [
            "analyze",
            "--manifest",
            str(tiny_manifest_path),
            "--run-results",
            str(tiny_run_results_path),
            "--format",
            "json",
            "--output",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert out.exists()
    json.loads(out.read_text())  # valid JSON


def test_analyze_rejects_both_modes(
    tiny_manifest_path: Path, tiny_run_results_path: Path
) -> None:
    result = runner.invoke(
        app,
        [
            "analyze",
            "--manifest",
            str(tiny_manifest_path),
            "--run-results",
            str(tiny_run_results_path),
            "--account-id",
            "42",
        ],
    )
    assert result.exit_code != 0
    combined = (result.stdout + (result.stderr or "")).lower()
    assert "either file mode" in combined


def test_analyze_requires_some_input() -> None:
    result = runner.invoke(app, ["analyze"])
    assert result.exit_code != 0


@responses.activate
def test_analyze_from_cloud(
    monkeypatch,
    tiny_manifest_path: Path,
    tiny_run_results_path: Path,
) -> None:
    monkeypatch.setenv("DBT_CLOUD_TOKEN", "sekret")
    base = "https://cloud.getdbt.com/api/v2/accounts/42/jobs/7/artifacts"
    manifest_body = json.loads(tiny_manifest_path.read_text())
    rr_body = json.loads(tiny_run_results_path.read_text())
    responses.add(responses.GET, f"{base}/manifest.json", json=manifest_body, status=200)
    responses.add(responses.GET, f"{base}/run_results.json", json=rr_body, status=200)

    result = runner.invoke(
        app,
        ["analyze", "--account-id", "42", "--job-id", "7", "--format", "json", "--top", "1"],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert "source.demo.raw.orders" in payload


def test_replay_from_files_text(
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
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "Run summary" in result.stdout
    assert "Thread utilization" in result.stdout
    assert "Observed critical path" in result.stdout


def test_replay_from_files_json(
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
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["thread_count"] == 4
    assert len(payload["events"]) == 57
    assert payload["critical_path"]


def test_replay_rejects_both_modes(
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
            "--account-id",
            "42",
        ],
    )
    assert result.exit_code != 0
