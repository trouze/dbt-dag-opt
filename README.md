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

### `analyze` — critical path through the DAG

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
  --show-path                  Render the full chain of node ids (table format)
  -o, --output PATH            Write output to a file instead of stdout
```

The table includes a **Bottleneck** column that names the slowest model on each path. First-order optimization target: the bottleneck model on the #1 path. Watch for a bottleneck that repeats across multiple paths — that's shared-node leverage (optimizing one model helps several paths at once).

### `replay` — what actually happened

`analyze` is theoretical — it reports the DAG-structural lower bound on wall-clock. `replay` reads the *observed* schedule. Every result in `run_results.json` carries a `thread_id` and per-phase `timing` with start/end timestamps, so we can reconstruct:

- **Per-thread utilization** — how much of the run each worker was busy vs. idle.
- **Observed critical path** — the chain of nodes that actually determined wall-clock, walked backwards from the last-completing node.
- **Idle-gap attribution** — for every stretch of idle time, which upstream node's completion unblocked the thread. Gaps with no blocker are scheduler overhead, not DAG blocking.

```
dbt-dag-opt replay [OPTIONS]

  --manifest PATH              Path to manifest.json (file mode)
  --run-results PATH           Path to run_results.json (file mode)
  --account-id / --job-id      dbt Cloud mode (same as analyze)
  -f, --format [text|json]     Output format  [default: text]
  --top-idle-gaps INTEGER      How many idle gaps to surface  [default: 10]
  --warehouse-size TEXT        Snowflake size (XS, S, M, L, XL, 2XL…6XL); triggers cost overlay
  --credits-per-hour FLOAT     Raw credits/hour for non-Snowflake adapters
  --rate-per-credit FLOAT      USD per credit  [default: 2.0 (Standard On-Demand)]
  --no-minimum-billing         Skip the 60s Snowflake minimum-billing floor
  -o, --output PATH            Write output to a file instead of stdout
```

#### Cost overlay

Pass `--warehouse-size` to translate wall-clock into dollars:

```bash
dbt-dag-opt replay \
  --manifest target/manifest.json \
  --run-results target/run_results.json \
  --warehouse-size L
```

Four numbers frame the output:

- **Run cost** — what this run actually billed (wall-clock × warehouse rate, with the 60s floor applied).
- **Critical-path floor** — the irreducible cost of your slowest dependency chain. You can't beat this without making individual models faster.
- **Headroom** — `run − floor`. The prize for better parallelization: what you could save if threads never sat idle.
- **Idle cost** — the $ equivalent of thread-idle warehouse-seconds. Distinct from headroom: idle cost includes time spent waiting on long-tail critical-path models that can't be parallelized away.

Defaults to $2.00/credit (Snowflake Standard On-Demand). Override with `--rate-per-credit` (Enterprise ≈ 3.0, Business Critical ≈ 4.0; check your contract). Non-Snowflake adapters: pass `--credits-per-hour N` instead of `--warehouse-size`.

### Output formats

- `table` — rich terminal table (default for `analyze`).
- `text` — rich-rendered summary (default for `replay`): run summary, per-thread utilization, observed critical path, top idle gaps.
- `json` — `analyze` emits `{source_id: {path, distance, length}}`; `replay` emits the full replay report. Both are `jq`-friendly.
- `jsonl` — one JSON object per line (`analyze` only).

## How it works

1. **Load** `manifest.json` and `run_results.json` (from disk or dbt Cloud's Admin API).
2. **Build** a weighted DAG: nodes are `model.*` / `source.*` / `seed.*` / `snapshot.*` ids; each node's weight is its `execution_time` in seconds.
3. **Compute** the longest path from each source using an iterative DP over topological order (O(V + E)).
4. **Sort** paths by total distance and surface the heaviest ones.

Distances sum the execution time of every node along the path — that's the warehouse-seconds you'd save by zeroing out that chain.

## What this is / isn't

It **is** a CLI tool that points at the slowest chains in your DAG, reconstructs the observed schedule those chains produced (`replay`), and — with `--warehouse-size` — translates that schedule into dollars.

It **isn't** (yet):
- A predictive scheduler simulator. `replay` reconstructs what already happened; it doesn't yet project what would happen under a different `--threads N` or if you sped up a specific model. That "what-if" loop is planned next, and will diff two cost reports to show projected $ savings.

## Development

```bash
uv sync --all-extras
uv run ruff check .
uv run mypy src
uv run pytest
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
