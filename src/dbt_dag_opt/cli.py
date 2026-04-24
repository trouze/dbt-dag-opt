"""Typer CLI for dbt-dag-opt."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated

import typer

from dbt_dag_opt import __version__, artifacts
from dbt_dag_opt.errors import DbtDagOptError
from dbt_dag_opt.formatters import Format, render
from dbt_dag_opt.graph import build_dag
from dbt_dag_opt.longest_path import longest_paths_from_each_source
from dbt_dag_opt.models import DagArtifacts
from dbt_dag_opt.replay import build_replay
from dbt_dag_opt.replay_formatters import ReplayFormat, render_replay

app = typer.Typer(
    name="dbt-dag-opt",
    help="Find the longest critical paths through your dbt DAG.",
    no_args_is_help=True,
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"dbt-dag-opt {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    _version: Annotated[
        bool,
        typer.Option(
            "--version", callback=_version_callback, is_eager=True, help="Show version and exit."
        ),
    ] = False,
) -> None:
    """Root callback — hosts global --version."""


@app.command("analyze")
def analyze(
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", help="Path to manifest.json (file mode)."),
    ] = None,
    run_results: Annotated[
        Path | None,
        typer.Option("--run-results", help="Path to run_results.json (file mode)."),
    ] = None,
    account_id: Annotated[
        str | None,
        typer.Option("--account-id", help="dbt Cloud account id (cloud mode)."),
    ] = None,
    job_id: Annotated[
        str | None,
        typer.Option("--job-id", help="dbt Cloud job id (cloud mode)."),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run-id",
            help="dbt Cloud run id (cloud mode). If omitted, uses the job's latest run.",
        ),
    ] = None,
    base_url: Annotated[
        str,
        typer.Option(
            "--base-url",
            help="dbt Cloud base URL (e.g. https://cloud.getdbt.com or a region-specific host).",
        ),
    ] = artifacts.DEFAULT_BASE_URL,
    token: Annotated[
        str | None,
        typer.Option(
            "--token",
            help="dbt Cloud API token. Prefer setting DBT_CLOUD_TOKEN in the environment.",
            envvar="DBT_CLOUD_TOKEN",
        ),
    ] = None,
    fmt: Annotated[
        Format, typer.Option("--format", "-f", help="Output format.")
    ] = Format.TABLE,
    top: Annotated[
        int,
        typer.Option("--top", "-n", help="Show only the top N longest paths. Use 0 for all."),
    ] = 10,
    show_path: Annotated[
        bool,
        typer.Option(
            "--show-path",
            help="Render the full chain of node ids in the table (table format only).",
        ),
    ] = False,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write output to a file instead of stdout."),
    ] = None,
) -> None:
    """Compute longest-running paths through your dbt DAG."""
    try:
        data = _load(
            manifest=manifest,
            run_results=run_results,
            account_id=account_id,
            job_id=job_id,
            run_id=run_id,
            base_url=base_url,
            token=token,
        )
        dag = build_dag(data)
        results = longest_paths_from_each_source(dag)
    except DbtDagOptError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    top_value: int | None = top if top > 0 else None
    rendered = render(
        results, fmt, top=top_value, show_full_path=show_path, weights=dag.weights
    )

    if output is not None:
        output.write_text(rendered, encoding="utf-8")
        typer.echo(f"Wrote {len(results)} path result(s) to {output}")
    else:
        sys.stdout.write(rendered)
        if not rendered.endswith("\n"):
            sys.stdout.write("\n")


@app.command("replay")
def replay(
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", help="Path to manifest.json (file mode)."),
    ] = None,
    run_results: Annotated[
        Path | None,
        typer.Option("--run-results", help="Path to run_results.json (file mode)."),
    ] = None,
    account_id: Annotated[
        str | None,
        typer.Option("--account-id", help="dbt Cloud account id (cloud mode)."),
    ] = None,
    job_id: Annotated[
        str | None,
        typer.Option("--job-id", help="dbt Cloud job id (cloud mode)."),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(
            "--run-id",
            help="dbt Cloud run id (cloud mode). If omitted, uses the job's latest run.",
        ),
    ] = None,
    base_url: Annotated[
        str,
        typer.Option("--base-url", help="dbt Cloud base URL."),
    ] = artifacts.DEFAULT_BASE_URL,
    token: Annotated[
        str | None,
        typer.Option(
            "--token",
            help="dbt Cloud API token. Prefer setting DBT_CLOUD_TOKEN in the environment.",
            envvar="DBT_CLOUD_TOKEN",
        ),
    ] = None,
    fmt: Annotated[
        ReplayFormat, typer.Option("--format", "-f", help="Output format.")
    ] = ReplayFormat.TEXT,
    top_idle_gaps: Annotated[
        int,
        typer.Option(
            "--top-idle-gaps",
            help="Number of idle gaps to surface. Use 0 to suppress.",
        ),
    ] = 10,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Write output to a file instead of stdout."),
    ] = None,
) -> None:
    """Replay an observed run: per-thread utilization, critical path, and idle-gap attribution."""
    try:
        data = _load(
            manifest=manifest,
            run_results=run_results,
            account_id=account_id,
            job_id=job_id,
            run_id=run_id,
            base_url=base_url,
            token=token,
        )
        limit = top_idle_gaps if top_idle_gaps > 0 else 0
        report = build_replay(data, top_idle_gaps_limit=limit)
    except DbtDagOptError as exc:
        typer.secho(f"error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    rendered = render_replay(report, fmt)

    if output is not None:
        output.write_text(rendered, encoding="utf-8")
        typer.echo(f"Wrote replay report to {output}")
    else:
        sys.stdout.write(rendered)
        if not rendered.endswith("\n"):
            sys.stdout.write("\n")


def _load(
    *,
    manifest: Path | None,
    run_results: Path | None,
    account_id: str | None,
    job_id: str | None,
    run_id: str | None,
    base_url: str,
    token: str | None,
) -> DagArtifacts:
    file_mode = manifest is not None or run_results is not None
    cloud_mode = account_id is not None or job_id is not None

    if file_mode and cloud_mode:
        raise typer.BadParameter(
            "Use either file mode (--manifest/--run-results) or cloud mode, not both."
        )

    if file_mode:
        if manifest is None or run_results is None:
            raise typer.BadParameter("File mode requires both --manifest and --run-results.")
        return artifacts.load_from_files(manifest, run_results)

    if cloud_mode:
        if not account_id or not job_id:
            raise typer.BadParameter("Cloud mode requires both --account-id and --job-id.")
        effective_token = token or os.environ.get("DBT_CLOUD_TOKEN")
        if not effective_token:
            raise typer.BadParameter(
                "Cloud mode requires a token — pass --token or set DBT_CLOUD_TOKEN."
            )
        return artifacts.load_from_cloud(
            account_id=account_id,
            job_id=job_id,
            token=effective_token,
            base_url=base_url,
            run_id=run_id,
        )

    raise typer.BadParameter(
        "No input specified. Use either --manifest/--run-results (file mode) "
        "or --account-id/--job-id (cloud mode)."
    )
