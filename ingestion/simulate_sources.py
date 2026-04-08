"""Create realistic multi-source input feeds for the decision system.

This module simulates the kind of fragmented analytics inputs an internal
creator operations team would receive from audience-growth tools, stream
telemetry systems, and social-listening platforms.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List


CREATORS = [
    {
        "creator_id": "cr_001",
        "creator_name": "Ava Blaze",
        "category": "Gaming",
        "region": "NA",
        "base_followers": 180_000,
        "baseline_mentions": 280,
        "sentiment_bias": 0.22,
    },
    {
        "creator_id": "cr_002",
        "creator_name": "Kai North",
        "category": "Lifestyle",
        "region": "EU",
        "base_followers": 240_000,
        "baseline_mentions": 160,
        "sentiment_bias": 0.08,
    },
    {
        "creator_id": "cr_003",
        "creator_name": "Mila Orbit",
        "category": "Music",
        "region": "LATAM",
        "base_followers": 125_000,
        "baseline_mentions": 210,
        "sentiment_bias": 0.31,
    },
    {
        "creator_id": "cr_004",
        "creator_name": "Noah Vector",
        "category": "Tech",
        "region": "APAC",
        "base_followers": 95_000,
        "baseline_mentions": 110,
        "sentiment_bias": 0.12,
    },
    {
        "creator_id": "cr_005",
        "creator_name": "Zuri Flux",
        "category": "Sports",
        "region": "NA",
        "base_followers": 72_000,
        "baseline_mentions": 90,
        "sentiment_bias": 0.18,
    },
]


def _build_time_index(hours: int) -> List[datetime]:
    """Return an hourly time index ending at the current UTC hour."""
    end = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=hours - 1)
    return [start + timedelta(hours=offset) for offset in range(hours)]


def _creator_metrics_records(hours: int) -> Iterable[Dict]:
    """Simulate creator-account metrics from an internal growth system."""
    for creator_index, creator in enumerate(CREATORS):
        followers = creator["base_followers"]
        for hour_index, ts in enumerate(_build_time_index(hours)):
            growth_multiplier = 1 + ((hour_index + creator_index) % 5) * 0.04
            net_new_followers = int(40 + creator_index * 8 + growth_multiplier * 14)

            # Give one creator an accelerating adoption curve to surface as emerging.
            if creator["creator_id"] == "cr_005" and hour_index >= hours - 10:
                net_new_followers += 55 + (hour_index - (hours - 10)) * 6

            followers += net_new_followers
            yield {
                "timestamp": ts.isoformat(),
                "creator_id": creator["creator_id"],
                "creator_name": creator["creator_name"],
                "category": creator["category"],
                "region": creator["region"],
                "followers_total": followers,
                "followers_gained": net_new_followers,
            }


def _stream_metrics_records(hours: int) -> Iterable[Dict]:
    """Simulate stream-level telemetry from a streaming analytics platform."""
    for creator_index, creator in enumerate(CREATORS):
        for hour_index, ts in enumerate(_build_time_index(hours)):
            views = 4_200 + creator_index * 850 + (hour_index % 6) * 320
            watch_time_minutes = views * (2.8 + creator_index * 0.15)
            likes = int(views * (0.045 + creator_index * 0.002))
            comments = int(views * (0.011 + creator_index * 0.001))
            shares = int(views * (0.006 + creator_index * 0.0012))

            # Inject a concentrated performance spike for executive reporting.
            if creator["creator_id"] == "cr_001" and hour_index >= hours - 4:
                views = int(views * 2.05)
                watch_time_minutes = int(watch_time_minutes * 2.2)
                likes = int(likes * 2.6)
                comments = int(comments * 3.0)
                shares = int(shares * 2.8)

            if creator["creator_id"] == "cr_003" and hour_index in {hours - 7, hours - 6}:
                views = int(views * 1.55)
                watch_time_minutes = int(watch_time_minutes * 1.7)
                comments = int(comments * 1.9)

            yield {
                "timestamp": ts.isoformat(),
                "stream_id": f"st_{creator['creator_id']}_{hour_index:03d}",
                "creator_id": creator["creator_id"],
                "views": views,
                "watch_time_minutes": watch_time_minutes,
                "likes": likes,
                "comments": comments,
                "shares": shares,
            }


def _social_listening_records(hours: int) -> Iterable[Dict]:
    """Simulate a social-listening feed with mentions and aggregate sentiment."""
    for creator_index, creator in enumerate(CREATORS):
        for hour_index, ts in enumerate(_build_time_index(hours)):
            mentions = creator["baseline_mentions"] + creator_index * 14 + (hour_index % 8) * 9
            sentiment = 0.42 + creator["sentiment_bias"] - (hour_index % 4) * 0.015

            if creator["creator_id"] == "cr_001" and hour_index >= hours - 4:
                mentions = int(mentions * 2.4)
                sentiment += 0.11

            if creator["creator_id"] == "cr_005" and hour_index >= hours - 8:
                mentions = int(mentions * 1.75)
                sentiment += 0.08

            sentiment = max(-1.0, min(1.0, round(sentiment, 3)))
            yield {
                "timestamp": ts.isoformat(),
                "creator_id": creator["creator_id"],
                "platform": "social_listening",
                "mentions": mentions,
                "sentiment_score": sentiment,
            }


def write_simulated_sources(raw_dir: Path, hours: int = 24) -> Dict[str, Path]:
    """Generate JSON source files that behave like upstream platform payloads."""
    raw_dir.mkdir(parents=True, exist_ok=True)

    payloads = {
        "creator_metrics.json": list(_creator_metrics_records(hours)),
        "stream_metrics.json": list(_stream_metrics_records(hours)),
        "social_listening.json": list(_social_listening_records(hours)),
    }

    output_paths: Dict[str, Path] = {}
    for filename, records in payloads.items():
        path = raw_dir / filename
        path.write_text(json.dumps(records, indent=2), encoding="utf-8")
        output_paths[filename] = path

    return output_paths
