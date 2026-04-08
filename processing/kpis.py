"""Compute executive KPIs for creator and stream decision-making.

These KPIs are designed to answer three business questions:
1. Who is performing best overall?
2. Which creators are driving the strongest stream impact?
3. Who has the most forward momentum right now?
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd


def calculate_kpis(unified: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute creator-level KPIs and the latest snapshot used for decisioning.

    KPI definitions:
    - creator_performance_score: audience size + engagement efficiency + growth
    - stream_impact_score: reach + watch time + conversation generated
    - trend_momentum_index: near-term forward motion from engagement, growth,
      sentiment, and spike intensity
    """
    latest_snapshot = (
        unified.sort_values(["creator_id", "timestamp"])
        .groupby("creator_id", as_index=False)
        .tail(1)
        .copy()
    )

    creator_rollup = (
        unified.groupby(["creator_id", "creator_name", "category", "region"], as_index=False)
        .agg(
            followers_total=("followers_total", "last"),
            followers_gained=("followers_gained", "sum"),
            views=("views", "sum"),
            watch_time_minutes=("watch_time_minutes", "sum"),
            mentions=("mentions", "sum"),
            avg_sentiment=("sentiment_score", "mean"),
            avg_engagement_rate=("engagement_rate", "mean"),
            avg_growth_rate=("growth_rate", "mean"),
            peak_spike_score=("spike_score", "max"),
        )
    )

    max_followers = creator_rollup["followers_total"].max()
    max_views = creator_rollup["views"].max()
    max_mentions = creator_rollup["mentions"].max()

    # Creator Performance Score:
    # Weighted blend of normalized audience scale, engagement efficiency, and follower growth.
    creator_rollup["creator_performance_score"] = (
        45 * (creator_rollup["followers_total"] / max_followers)
        + 35 * (creator_rollup["avg_engagement_rate"] / creator_rollup["avg_engagement_rate"].max())
        + 20 * (creator_rollup["avg_growth_rate"] / creator_rollup["avg_growth_rate"].max())
    ).round(2)

    # Stream Impact Score:
    # Weighted mix of reach, time spent, and conversation volume generated per creator.
    creator_rollup["stream_impact_score"] = (
        50 * (creator_rollup["views"] / max_views)
        + 30 * (
            creator_rollup["watch_time_minutes"]
            / creator_rollup["watch_time_minutes"].max()
        )
        + 20 * (creator_rollup["mentions"] / max_mentions)
    ).round(2)

    # Trend Momentum Index:
    # Combines latest engagement, growth, sentiment, and recent spike intensity to capture forward motion.
    latest_snapshot = latest_snapshot.merge(
        creator_rollup[
            [
                "creator_id",
                "creator_performance_score",
                "stream_impact_score",
            ]
        ],
        on="creator_id",
        how="left",
    )
    latest_snapshot["trend_momentum_index"] = (
        latest_snapshot["engagement_rate"] * 300
        + latest_snapshot["growth_rate"] * 1500
        + latest_snapshot["sentiment_score"] * 20
        + latest_snapshot["spike_score"] * 10
    ).round(2)

    creator_rollup = creator_rollup.merge(
        latest_snapshot[["creator_id", "trend_momentum_index"]],
        on="creator_id",
        how="left",
    )

    return creator_rollup.sort_values(
        ["creator_performance_score", "trend_momentum_index"], ascending=False
    ).reset_index(drop=True), latest_snapshot.reset_index(drop=True)


def write_kpi_tables(
    creator_kpis: pd.DataFrame, latest_snapshot: pd.DataFrame, processed_dir: Path
) -> Tuple[Path, Path]:
    """Persist KPI outputs for reporting and auditing."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    creator_path = processed_dir / "creator_kpis.csv"
    snapshot_path = processed_dir / "latest_snapshot.csv"
    creator_kpis.to_csv(creator_path, index=False)
    latest_snapshot.to_csv(snapshot_path, index=False)
    return creator_path, snapshot_path
