"""Snowflake-style cost model layered on top of a ``ReplayReport``.

Translates wall-clock, thread-idleness, and the critical-path floor into
dollar amounts the user can reason about. Kept intentionally small and
primitive-driven so a future ``whatif`` simulator can call ``compute_cost``
twice (baseline + simulated) and diff the two ``CostReport``s without
having to fabricate a full replay.
"""

from __future__ import annotations

from dataclasses import dataclass

from dbt_dag_opt.errors import InvalidArtifactError
from dbt_dag_opt.replay import ReplayReport

SNOWFLAKE_CREDITS_PER_HOUR: dict[str, float] = {
    "XS": 1.0,
    "S": 2.0,
    "M": 4.0,
    "L": 8.0,
    "XL": 16.0,
    "2XL": 32.0,
    "3XL": 64.0,
    "4XL": 128.0,
    "5XL": 256.0,
    "6XL": 512.0,
}

DEFAULT_RATE_PER_CREDIT_USD: float = 2.0
MIN_BILLING_SECONDS: float = 60.0

_SIZE_ALIASES: dict[str, str] = {
    "XSMALL": "XS",
    "SMALL": "S",
    "MEDIUM": "M",
    "LARGE": "L",
    "XLARGE": "XL",
    "2XLARGE": "2XL",
    "3XLARGE": "3XL",
    "4XLARGE": "4XL",
    "5XLARGE": "5XL",
    "6XLARGE": "6XL",
}


@dataclass(frozen=True)
class CostInputs:
    """Primitive inputs to the cost model.

    Decoupled from ``ReplayReport`` so ``whatif`` can pass simulated numbers
    without going through the full parse pipeline.
    """

    wall_clock_seconds: float
    total_cpu_seconds: float
    thread_count: int
    critical_path_seconds: float
    credits_per_hour: float
    rate_per_credit_usd: float
    apply_minimum_billing: bool = True
    warehouse_size_label: str | None = None


@dataclass(frozen=True)
class CostReport:
    inputs: CostInputs
    billed_seconds: float
    rate_per_second_usd: float
    run_cost_usd: float
    floor_cost_usd: float
    headroom_usd: float
    idle_thread_seconds: float
    waste_fraction: float
    idle_cost_usd: float
    min_billing_applied: bool


def credits_per_hour_for(size: str) -> float:
    """Resolve a Snowflake warehouse size alias to credits/hour.

    Case-insensitive; strips ``-`` and whitespace. Accepts both shorthand
    (``"L"``, ``"2XL"``) and long form (``"Large"``, ``"2X-Large"``).
    """
    normalized = size.upper().replace("-", "").replace(" ", "").replace("_", "")
    if normalized in SNOWFLAKE_CREDITS_PER_HOUR:
        return SNOWFLAKE_CREDITS_PER_HOUR[normalized]
    if normalized in _SIZE_ALIASES:
        return SNOWFLAKE_CREDITS_PER_HOUR[_SIZE_ALIASES[normalized]]
    raise InvalidArtifactError(
        f"unknown warehouse size: {size!r}. "
        f"Expected one of {sorted(SNOWFLAKE_CREDITS_PER_HOUR)}."
    )


def compute_cost(inputs: CostInputs) -> CostReport:
    rate_per_second = inputs.credits_per_hour * inputs.rate_per_credit_usd / 3600.0

    raw_wall = max(0.0, inputs.wall_clock_seconds)
    if inputs.apply_minimum_billing and raw_wall < MIN_BILLING_SECONDS:
        billed = MIN_BILLING_SECONDS
        min_billing_applied = True
    else:
        billed = raw_wall
        min_billing_applied = False

    run_cost = billed * rate_per_second

    raw_floor = max(0.0, inputs.critical_path_seconds)
    if inputs.apply_minimum_billing:
        floor_seconds = max(raw_floor, MIN_BILLING_SECONDS)
    else:
        floor_seconds = raw_floor
    floor_cost = floor_seconds * rate_per_second

    warehouse_seconds = raw_wall * max(1, inputs.thread_count)
    idle_thread_seconds = max(0.0, warehouse_seconds - max(0.0, inputs.total_cpu_seconds))
    waste_fraction = (
        idle_thread_seconds / warehouse_seconds if warehouse_seconds > 0 else 0.0
    )
    idle_cost = run_cost * waste_fraction
    headroom = max(0.0, run_cost - floor_cost)

    return CostReport(
        inputs=inputs,
        billed_seconds=billed,
        rate_per_second_usd=rate_per_second,
        run_cost_usd=run_cost,
        floor_cost_usd=floor_cost,
        headroom_usd=headroom,
        idle_thread_seconds=idle_thread_seconds,
        waste_fraction=waste_fraction,
        idle_cost_usd=idle_cost,
        min_billing_applied=min_billing_applied,
    )


def cost_inputs_from_replay(
    report: ReplayReport,
    *,
    warehouse_size: str | None,
    credits_per_hour: float | None,
    rate_per_credit_usd: float,
    apply_minimum_billing: bool,
) -> CostInputs:
    """Adapt a ``ReplayReport`` + user flags into ``CostInputs``.

    Exactly one of ``warehouse_size`` / ``credits_per_hour`` must be set;
    the caller (CLI) enforces this.
    """
    if warehouse_size is not None and credits_per_hour is not None:
        raise InvalidArtifactError(
            "pass either warehouse_size or credits_per_hour, not both"
        )
    if warehouse_size is not None:
        cph = credits_per_hour_for(warehouse_size)
        label = warehouse_size.upper().replace("-", "").replace(" ", "").replace("_", "")
        if label in _SIZE_ALIASES:
            label = _SIZE_ALIASES[label]
    elif credits_per_hour is not None:
        if credits_per_hour <= 0:
            raise InvalidArtifactError("credits_per_hour must be positive")
        cph = float(credits_per_hour)
        label = None
    else:
        raise InvalidArtifactError(
            "cost inputs require warehouse_size or credits_per_hour"
        )

    return CostInputs(
        wall_clock_seconds=report.wall_clock_seconds,
        total_cpu_seconds=report.total_cpu_seconds,
        thread_count=report.thread_count,
        critical_path_seconds=report.critical_path_seconds,
        credits_per_hour=cph,
        rate_per_credit_usd=rate_per_credit_usd,
        apply_minimum_billing=apply_minimum_billing,
        warehouse_size_label=label,
    )
