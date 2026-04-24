"""Load dbt artifacts (`manifest.json`, `run_results.json`) from disk or dbt Cloud."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from dbt_dag_opt.errors import ArtifactLoadError, DbtCloudAPIError, InvalidArtifactError
from dbt_dag_opt.models import DagArtifacts

DEFAULT_BASE_URL = "https://cloud.getdbt.com"
DEFAULT_TIMEOUT = 30
_HTTP_ERROR_THRESHOLD = 400


def load_from_files(manifest_path: Path, run_results_path: Path) -> DagArtifacts:
    manifest = _read_json(manifest_path, "manifest.json")
    run_results = _read_json(run_results_path, "run_results.json")
    _validate(manifest, run_results)
    return DagArtifacts(manifest=manifest, run_results=run_results)


def load_from_cloud(
    account_id: str,
    job_id: str,
    token: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    run_id: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> DagArtifacts:
    """Fetch artifacts from the dbt Cloud Admin API.

    If `run_id` is provided, fetches artifacts for that specific run; otherwise
    fetches from the job's most recent run. Uses the Administrative API, not
    the Discovery/GraphQL API.
    """
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})

    base = base_url.rstrip("/")
    if run_id:
        url_template = f"{base}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{{name}}"
    else:
        url_template = f"{base}/api/v2/accounts/{account_id}/jobs/{job_id}/artifacts/{{name}}"

    try:
        manifest = _fetch(session, url_template.format(name="manifest.json"), timeout)
        run_results = _fetch(session, url_template.format(name="run_results.json"), timeout)
    finally:
        session.close()

    _validate(manifest, run_results)
    return DagArtifacts(manifest=manifest, run_results=run_results)


def _fetch(session: requests.Session, url: str, timeout: int) -> dict[str, Any]:
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        raise DbtCloudAPIError(f"Network error fetching {url}: {exc}") from exc

    if response.status_code >= _HTTP_ERROR_THRESHOLD:
        raise DbtCloudAPIError(
            f"dbt Cloud returned {response.status_code} for {url}: {response.text[:200]}",
            status_code=response.status_code,
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise DbtCloudAPIError(f"dbt Cloud returned non-JSON body for {url}") from exc
    if not isinstance(data, dict):
        raise DbtCloudAPIError(f"dbt Cloud returned non-object JSON for {url}")
    return data


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise ArtifactLoadError(f"{label} not found at {path}") from exc
    except json.JSONDecodeError as exc:
        raise InvalidArtifactError(f"{label} at {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise InvalidArtifactError(f"{label} at {path} must be a JSON object")
    return data


def _validate(manifest: dict[str, Any], run_results: dict[str, Any]) -> None:
    for key in ("nodes", "child_map", "parent_map"):
        if key not in manifest:
            raise InvalidArtifactError(f"manifest.json missing required key: {key!r}")
    if "results" not in run_results:
        raise InvalidArtifactError("run_results.json missing required key: 'results'")
