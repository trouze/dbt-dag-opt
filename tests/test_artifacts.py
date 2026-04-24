from __future__ import annotations

import json
from pathlib import Path

import pytest
import responses

from dbt_dag_opt.artifacts import load_from_cloud, load_from_files
from dbt_dag_opt.errors import ArtifactLoadError, DbtCloudAPIError, InvalidArtifactError


def test_load_from_files_returns_parsed_artifacts(
    tiny_manifest_path: Path, tiny_run_results_path: Path
) -> None:
    data = load_from_files(tiny_manifest_path, tiny_run_results_path)
    assert "nodes" in data.manifest
    assert "results" in data.run_results


def test_load_from_files_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ArtifactLoadError, match=r"manifest\.json not found"):
        load_from_files(tmp_path / "missing.json", tmp_path / "also_missing.json")


def test_load_from_files_malformed_json(tmp_path: Path, tiny_run_results_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    with pytest.raises(InvalidArtifactError, match="not valid JSON"):
        load_from_files(bad, tiny_run_results_path)


def test_load_from_files_missing_keys(tmp_path: Path, tiny_run_results_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"nodes": {}}))  # missing child_map, parent_map
    with pytest.raises(InvalidArtifactError, match="missing required key"):
        load_from_files(bad, tiny_run_results_path)


@responses.activate
def test_load_from_cloud_job_latest() -> None:
    base = "https://cloud.getdbt.com/api/v2/accounts/42/jobs/7/artifacts"
    responses.add(
        responses.GET,
        f"{base}/manifest.json",
        json={"nodes": {}, "child_map": {}, "parent_map": {}},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{base}/run_results.json",
        json={"results": []},
        status=200,
    )
    data = load_from_cloud(account_id="42", job_id="7", token="sekret")
    assert data.manifest == {"nodes": {}, "child_map": {}, "parent_map": {}}
    assert responses.calls[0].request.headers["Authorization"] == "Bearer sekret"


@responses.activate
def test_load_from_cloud_with_run_id_uses_run_endpoint() -> None:
    base = "https://cloud.getdbt.com/api/v2/accounts/42/runs/999/artifacts"
    responses.add(
        responses.GET,
        f"{base}/manifest.json",
        json={"nodes": {}, "child_map": {}, "parent_map": {}},
        status=200,
    )
    responses.add(responses.GET, f"{base}/run_results.json", json={"results": []}, status=200)
    load_from_cloud(account_id="42", job_id="7", token="sekret", run_id="999")
    assert "runs/999" in responses.calls[0].request.url


@responses.activate
def test_load_from_cloud_raises_on_http_error() -> None:
    responses.add(
        responses.GET,
        "https://cloud.getdbt.com/api/v2/accounts/42/jobs/7/artifacts/manifest.json",
        json={"error": "forbidden"},
        status=403,
    )
    with pytest.raises(DbtCloudAPIError) as exc:
        load_from_cloud(account_id="42", job_id="7", token="bad")
    assert exc.value.status_code == 403
