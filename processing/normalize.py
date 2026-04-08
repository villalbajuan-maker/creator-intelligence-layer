"""Normalize fragmented source data into a unified analytics model.

The normalization layer aligns creator growth, stream performance, and social
conversation into a single creator-hour table so KPI and signal logic can rely
on one consistent contract.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd


def _read_json(path: Path) -> pd.DataFrame:
    """Load a JSON list payload into a DataFrame."""
    with path.open("r", encoding="utf-8") as handle:
        return pd.DataFrame(json.load(handle))


def load_raw_sources(raw_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load all simulated raw data files."""
    return {
        "creator_metrics": _read_json(raw_dir / "creator_metrics.json"),
        "stream_metrics": _read_json(raw_dir / "stream_metrics.json"),
        "social_listening": _read_json(raw_dir / "social_listening.json"),
    }


def normalize_sources(raw_sources: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Clean raw source frames and build a unified creator-hour fact table.

    Derived metrics created here:
    - engagement_rate: share of viewers taking an action on stream content
    - growth_rate: hourly follower gain relative to the previous follower base
    - spike_score: z-score of views versus a recent rolling baseline
    """
    creators = raw_sources["creator_metrics"].copy()
    streams = raw_sources["stream_metrics"].copy()
    social = raw_sources["social_listening"].copy()

    for frame in (creators, streams, social):
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)

    creators = creators.sort_values(["creator_id", "timestamp"])
    streams = streams.sort_values(["creator_id", "timestamp"])
    social = social.sort_values(["creator_id", "timestamp"])

    stream_rollup = (
        streams.groupby(["creator_id", "timestamp"], as_index=False)
        .agg(
            views=("views", "sum"),
            watch_time_minutes=("watch_time_minutes", "sum"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum"),
        )
    )
    stream_rollup["engagement_actions"] = (
        stream_rollup["likes"] + stream_rollup["comments"] + stream_rollup["shares"]
    )
    stream_rollup["engagement_rate"] = np.where(
        stream_rollup["views"] > 0,
        stream_rollup["engagement_actions"] / stream_rollup["views"],
        0.0,
    )

    social_rollup = (
        social.groupby(["creator_id", "timestamp"], as_index=False)
        .agg(
            mentions=("mentions", "sum"),
            sentiment_score=("sentiment_score", "mean"),
        )
    )

    unified = creators.merge(stream_rollup, on=["creator_id", "timestamp"], how="left")
    unified = unified.merge(social_rollup, on=["creator_id", "timestamp"], how="left")
    unified = unified.fillna(
        {
            "views": 0,
            "watch_time_minutes": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "engagement_actions": 0,
            "engagement_rate": 0.0,
            "mentions": 0,
            "sentiment_score": 0.0,
        }
    )

    unified["growth_rate"] = np.where(
        unified["followers_total"] - unified["followers_gained"] > 0,
        unified["followers_gained"] / (unified["followers_total"] - unified["followers_gained"]),
        0.0,
    )

    grouped_views = unified.groupby("creator_id")["views"]
    baseline_mean = grouped_views.transform(lambda series: series.rolling(6, min_periods=3).mean())
    baseline_std = grouped_views.transform(lambda series: series.rolling(6, min_periods=3).std())
    unified["spike_score"] = (
        (unified["views"] - baseline_mean) / baseline_std.replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return unified.sort_values(["timestamp", "creator_id"]).reset_index(drop=True)


def write_processed_dataset(dataset: pd.DataFrame, processed_dir: Path) -> Path:
    """Persist the normalized dataset for downstream inspection."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_path = processed_dir / "unified_metrics.csv"
    dataset.to_csv(output_path, index=False)
    return output_path
