# dbt-dag-opt

[![CI](https://github.com/trouze/dbt-dag-opt/actions/workflows/ci.yml/badge.svg)](https://github.com/trouze/dbt-dag-opt/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dbt-dag-opt.svg)](https://pypi.org/project/dbt-dag-opt/)
[![Python](https://img.shields.io/pypi/pyversions/dbt-dag-opt.svg)](https://pypi.org/project/dbt-dag-opt/)

**Find the longest-running paths through your dbt DAG — the models that actually make your pipeline slow.**

When you pay for compute by the second (Snowflake, Databricks, Redshift), your dbt job's wall-clock cost is bounded by the *critical path* through the DAG: the longest cumulative chain of model execution times. Optimizing a slow model on a short branch saves you nothing if a longer branch was already the bottleneck. `dbt-dag-opt` tells you which paths to cut first.

## Install

```bash
pip install dbt-dag-opt
```

## Quickstart

### From local artifacts

```bash
dbt-dag-opt analyze \
  --manifest target/manifest.json \
  --run-results target/run_results.json \
  --format table \
  --top 10
```

### From dbt Cloud

```bash
export DBT_CLOUD_TOKEN=dbtu_...
dbt-dag-opt analyze \
  --account-id 12345 \
  --job-id 67890 \
  --base-url https://cloud.getdbt.com \
  --format table
```

Add `--run-id <id>` to pull artifacts from a specific historical run instead of the job's latest.

## Sample output

```
                       Longest paths by total execution time
┏━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃ # ┃ Source                    ┃ End of path            ┃ Length ┃ Total time (s) ┃
┡━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│ 1 │ source.demo.raw.orders    │ model.demo.fact_orders │      4 │          35.00 │
│ 2 │ source.demo.raw.customers │ model.demo.fact_orders │      4 │          32.00 │
└───┴───────────────────────────┴────────────────────────┴────────┴────────────────┘
```

## CLI reference

```
dbt-dag-opt analyze [OPTIONS]

  --manifest PATH              Path to manifest.json (file mode)
  --run-results PATH           Path to run_results.json (file mode)
  --account-id TEXT            dbt Cloud account id (cloud mode)
  --job-id TEXT                dbt Cloud job id (cloud mode)
  --run-id TEXT                dbt Cloud run id; omit for the job's latest run
  --base-url TEXT              dbt Cloud base URL  [default: https://cloud.getdbt.com]
  --token TEXT                 dbt Cloud API token  [env: DBT_CLOUD_TOKEN]
  -f, --format [json|jsonl|table]  Output format  [default: table]
  -n, --top INTEGER            Show only top N paths (0 = all)  [default: 10]
  -o, --output PATH            Write output to a file instead of stdout
```

### Output formats

- `table` — rich terminal table (default; what you want in a shell).
- `json` — one object keyed by source: `{source_id: {path, distance, length}}`. Valid JSON, safe to pipe through `jq`.
- `jsonl` — one JSON object per line. Nice for streaming into a log aggregator.

## How it works

1. **Load** `manifest.json` and `run_results.json` (from disk or dbt Cloud's Admin API).
2. **Build** a weighted DAG: nodes are `model.*` / `source.*` / `seed.*` / `snapshot.*` ids; each node's weight is its `execution_time` in seconds.
3. **Compute** the longest path from each source using an iterative DP over topological order (O(V + E)).
4. **Sort** paths by total distance and surface the heaviest ones.

Distances sum the execution time of every node along the path — that's the warehouse-seconds you'd save by zeroing out that chain.

## What this is / isn't

It **is** a CLI tool that points at the slowest chains in your DAG.

It **isn't** (yet):
- A scheduler simulator. If your dbt `threads` setting is low, total wall-clock is bounded by parallelism *and* the critical path; v0.2 will surface both. For now, treat the critical-path distance as a lower bound.
- A cost model. Multiplying distance × your warehouse rate is on you — a `--warehouse-size` flag is planned for v0.3.

## Development

```bash
uv sync --all-extras
uv run ruff check .
uv run mypy src
uv run pytest
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
