# Creator Intelligence Layer — Decision Support System for Creator & Stream Analytics

Transforms fragmented analytics into real-time execution decisions.

## Overview
Creator businesses rarely suffer from a lack of data. They suffer from too many disconnected tools.
Growth dashboards, stream analytics, and social listening platforms all describe different parts of the picture, but they do not tell teams what actually matters right now.
This system unifies those inputs, identifies meaningful signals, classifies creator momentum, and produces an executive report that tells a team what to do next.

## What This System Does
- Ingests multi-source data from simulated creator growth, stream performance, and social listening feeds
- Normalizes fragmented metrics into one creator-hour analytics model
- Detects signals such as engagement spikes, abnormal growth, and conversation surges
- Classifies creators into operating states like `breakout`, `spike`, and `stable`
- Generates executive decision reports with prioritized, time-sensitive actions

## Why This Matters
This is not a dashboard.

This is a decision-support layer that translates data into action.

Instead of asking a team to interpret five charts and debate what changed, the system identifies the signal, explains the business meaning, and recommends what to do in the next 24 hours, the next 3 days, and over the longer term.

## System Architecture
- `Ingestion`
  Creates realistic source payloads that mimic internal creator metrics, stream telemetry, and social listening feeds.
- `Normalization`
  Standardizes those inputs into one clean creator-hour table with reusable derived metrics such as engagement rate, growth rate, and spike score.
- `KPI Layer`
  Calculates business-facing scores that summarize creator performance, stream impact, and near-term momentum.
- `Signal Detection`
  Detects meaningful shifts in creator behavior using rolling baselines, thresholds, and momentum logic.
- `Decision / Reporting`
  Converts signal combinations into an executive report with clear prioritization and recommended actions.

## Example Output

```md
# Creator Intelligence Executive Decision Report

## Executive Summary
- Zuri Flux is the most important creator to act on right now. They have moved beyond routine growth and into a breakout moment that can be scaled.
- This matters because Zuri Flux combines momentum (53.61) with material stream impact (91.42), creating a timely opportunity to increase reach, sponsor visibility, and audience capture while attention is still building.
- Ava Blaze is the secondary priority: the signal is less about scale today and more about acting before the current window closes.

## What Changed (Last 12 Hours)
- Ava Blaze: engagement has broken above normal range, creating a narrow amplification window where extra distribution can turn creator momentum into broader reach.
- Zuri Flux: viewer energy is building on top of an already strong base, which points to a rising creator rather than a one-off traffic bump.
- Zuri Flux: audience growth is converting unusually well, which means current attention is sticking and increasing the creator's long-term value.
- Ava Blaze: public conversation is surging around the creator, raising the odds that well-timed clips or reposts will travel beyond the existing audience.
- Zuri Flux: market attention is widening, which lowers distribution risk and improves the case for giving this creator more surface area.

## Why It Matters
- Zuri Flux is in a breakout phase. This is the point where the business can gain disproportionate upside by investing before pricing, sponsorship demand, and internal competition rise.
- Ava Blaze is in a spike phase. The opportunity is immediate, but so is the risk of waiting: if action slips, the company captures the noise but not the value.
- Noah Vector is in a stable phase. This is a lower-urgency asset that supports predictable packaging, steadier ROI, and cleaner planning decisions.

## Recommended Actions
### Immediate (0-24h)
- Priority 1 Breakout: Zuri Flux -> Expand distribution for Zuri Flux immediately across owned channels and clip surfaces while the current demand window is still open.
- Priority 2 Spike: Ava Blaze -> Expand distribution for Ava Blaze immediately across owned channels and clip surfaces while the current demand window is still open.
- Priority 1 Breakout: Zuri Flux -> Flag Zuri Flux for audience capture now by tightening follow prompts, end cards, or cross-promotion while conversion is running above normal.

### Short-term (1-3 days)
- Priority 1 Breakout: Zuri Flux -> Build a follow-on content package around Zuri Flux over the next 1-3 days so the current surge becomes a sustained viewing cycle.
- Priority 2 Spike: Ava Blaze -> Build a follow-on content package around Ava Blaze over the next 1-3 days so the current surge becomes a sustained viewing cycle.
- Priority 1 Breakout: Zuri Flux -> Review Zuri Flux's recent programming mix and repeat the elements that are clearly turning attention into audience retention.

### Strategic (Longer-term)
- Priority 1 Breakout: Zuri Flux -> Add Zuri Flux to the rapid-response priority list so future spikes trigger distribution, sales, and programming decisions without delay.
- Priority 2 Spike: Ava Blaze -> Add Ava Blaze to the rapid-response priority list so future spikes trigger distribution, sales, and programming decisions without delay.
```

## How to Run

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Run the pipeline:

```bash
python main.py
```

3. Optional verification:

```bash
python -m unittest discover -s tests
```

4. Review outputs:
- Executive report: `outputs/creator_report.md`
- Processed analytics tables: `data/processed/`
- Simulated raw source files: `data/raw/`

## Project Structure

```text
creator_intelligence_layer/
├── data/
│   ├── processed/
│   │   ├── creator_kpis.csv
│   │   ├── detected_signals.csv
│   │   ├── latest_snapshot.csv
│   │   └── unified_metrics.csv
│   └── raw/
│       ├── creator_metrics.json
│       ├── social_listening.json
│       └── stream_metrics.json
├── ingestion/
│   └── simulate_sources.py
├── outputs/
│   └── creator_report.md
├── processing/
│   ├── kpis.py
│   └── normalize.py
├── reporting/
│   └── generate_report.py
├── signals/
│   └── trend_detection.py
├── tests/
│   └── test_pipeline.py
├── main.py
├── README.md
└── requirements.txt
```

## Design Philosophy
Most analytics systems stop at dashboards.
This system focuses on translating signals into decisions.

It is intentionally small, but it is structured like an internal product:
- clear pipeline stages
- reusable KPI logic
- action-oriented reporting
- realistic source simulation
- outputs that look usable by a content, growth, or partnerships team

The point is not visual complexity. The point is operational clarity.
