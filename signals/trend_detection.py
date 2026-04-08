"""Detect creator signals that should trigger action.

The signal engine converts raw metric shifts into operating signals such as
engagement spikes, abnormal growth, conversation surges, and emerging creators.
These signals feed the decision report rather than being exposed as raw stats.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd


def detect_signals(unified: pd.DataFrame, latest_snapshot: pd.DataFrame) -> pd.DataFrame:
    """Return human-readable signal events for the most recent period.

    Detection logic uses:
    - rolling lift against a recent 12-hour baseline
    - thresholded engagement and mention changes
    - trend momentum to surface smaller creators entering breakout territory
    """
    latest_ts = unified["timestamp"].max()
    recent = unified[unified["timestamp"] >= latest_ts - pd.Timedelta(hours=11)].copy()

    signal_rows: List[dict] = []
    for creator_id, creator_frame in recent.groupby("creator_id"):
        creator_frame = creator_frame.sort_values("timestamp")
        current = creator_frame.iloc[-1]
        historical = creator_frame.iloc[:-1]
        baseline_views = historical["views"].mean() if not historical.empty else current["views"]
        baseline_mentions = (
            historical["mentions"].mean() if not historical.empty else current["mentions"]
        )
        baseline_growth = (
            historical["followers_gained"].mean()
            if not historical.empty
            else current["followers_gained"]
        )

        view_lift = (
            ((current["views"] - baseline_views) / baseline_views) * 100 if baseline_views else 0.0
        )
        mention_lift = (
            ((current["mentions"] - baseline_mentions) / baseline_mentions) * 100
            if baseline_mentions
            else 0.0
        )
        growth_lift = (
            ((current["followers_gained"] - baseline_growth) / baseline_growth) * 100
            if baseline_growth
            else 0.0
        )

        if current["engagement_rate"] >= 0.075 and (current["spike_score"] >= 0.9 or view_lift >= 50):
            signal_rows.append(
                {
                    "creator_id": creator_id,
                    "creator_name": current["creator_name"],
                    "signal_type": "engagement_spike",
                    "severity": "high" if current["spike_score"] >= 2.5 or view_lift >= 80 else "medium",
                    "metric_value": round(current["engagement_rate"], 4),
                    "signal_summary": (
                        f"{current['creator_name']} posted a {view_lift:.0f}% lift in views "
                        f"with engagement rate at {current['engagement_rate']:.1%}."
                    ),
                }
            )

        if growth_lift >= 25:
            signal_rows.append(
                {
                    "creator_id": creator_id,
                    "creator_name": current["creator_name"],
                    "signal_type": "abnormal_growth",
                    "severity": "high" if growth_lift >= 80 else "medium",
                    "metric_value": round(growth_lift, 2),
                    "signal_summary": (
                        f"{current['creator_name']} follower gains are running {growth_lift:.0f}% "
                        "above the trailing 12-hour baseline."
                    ),
                }
            )

        if mention_lift >= 35 and current["sentiment_score"] > 0.45:
            signal_rows.append(
                {
                    "creator_id": creator_id,
                    "creator_name": current["creator_name"],
                    "signal_type": "conversation_surge",
                    "severity": "medium",
                    "metric_value": round(mention_lift, 2),
                    "signal_summary": (
                        f"{current['creator_name']} social mentions are up {mention_lift:.0f}% "
                        f"with positive sentiment at {current['sentiment_score']:.2f}."
                    ),
                }
            )

    emerging = latest_snapshot.sort_values("trend_momentum_index", ascending=False).head(2)
    for _, row in emerging.iterrows():
        if row["followers_total"] < latest_snapshot["followers_total"].median():
            signal_rows.append(
                {
                    "creator_id": row["creator_id"],
                    "creator_name": row["creator_name"],
                    "signal_type": "emerging_creator",
                    "severity": "medium",
                    "metric_value": round(row["trend_momentum_index"], 2),
                    "signal_summary": (
                        f"{row['creator_name']} is an emerging creator with strong momentum "
                        f"despite a smaller base, posting a momentum index of {row['trend_momentum_index']:.2f}."
                    ),
                }
            )

    if not signal_rows:
        return pd.DataFrame(
            columns=[
                "creator_id",
                "creator_name",
                "signal_type",
                "severity",
                "metric_value",
                "signal_summary",
            ]
        )

    signals = pd.DataFrame(signal_rows).drop_duplicates(subset=["creator_id", "signal_type"])
    severity_order = {"high": 0, "medium": 1, "low": 2}
    type_order = {
        "engagement_spike": 0,
        "abnormal_growth": 1,
        "conversation_surge": 2,
        "emerging_creator": 3,
    }
    signals["severity_rank"] = signals["severity"].map(severity_order).fillna(3)
    signals["type_rank"] = signals["signal_type"].map(type_order).fillna(4)
    return (
        signals.sort_values(["severity_rank", "type_rank", "metric_value"], ascending=[True, True, False])
        .drop(columns=["severity_rank", "type_rank"])
        .reset_index(drop=True)
    )


def write_signals(signals: pd.DataFrame, processed_dir: Path) -> Path:
    """Persist the detected signals for downstream consumers."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    output_path = processed_dir / "detected_signals.csv"
    signals.to_csv(output_path, index=False)
    return output_path
