# Creator Intelligence Layer — Decision Support System for Creator & Stream Analytics

Transforms fragmented analytics into real-time execution decisions.

## Project Snapshot
This repository simulates the kind of internal intelligence layer a streaming or creator-media company would use to unify audience growth, stream performance, and social listening into one operating view.

Instead of stopping at dashboards, it identifies what changed, why it matters, and what a content or growth team should do next.

## Why I Built This
Most analytics projects are technically correct but operationally weak. They show metrics, but they do not help a team decide.

I wanted this project to feel closer to a real internal system:
- multiple fragmented inputs
- a clean normalization layer
- business-facing KPIs
- signal detection
- executive reporting that translates data into action

The goal was not to build a UI. The goal was to build the backend logic that makes good decisions possible.

## What This System Does
- Ingests multi-source data from simulated creator growth, stream performance, and social listening feeds
- Normalizes fragmented metrics into one creator-hour analytics model
- Computes decision-facing KPIs such as `creator_performance_score`, `stream_impact_score`, and `trend_momentum_index`
- Detects operating signals such as engagement spikes, abnormal growth, conversation surges, and emerging creators
- Classifies creators into practical states like `breakout`, `spike`, and `stable`
- Generates an executive decision report with prioritized actions

## Why This Matters
This is not a dashboard.

This is a decision-support layer that translates data into action.

In real companies, analytics fragmentation slows down teams. Different tools describe different slices of reality, and someone still has to interpret what matters. This system reduces that gap by converting raw telemetry into timing, priority, and recommended action.

## Demo Outputs
When the pipeline runs, it generates:
- Simulated raw source files in `data/raw/`
- Processed analytics tables in `data/processed/`
- A final executive report in `outputs/creator_report.md`

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

## System Architecture
- `Ingestion`
  Simulates the upstream systems that feed creator operations teams.
- `Normalization`
  Creates one clean creator-hour model from fragmented inputs.
- `Metrics Layer`
  Produces reusable KPIs for scale, impact, and momentum.
- `Signal Detection`
  Detects meaningful changes instead of exposing only raw time-series movement.
- `Reporting Layer`
  Translates signal combinations into an executive decision brief.

## What Makes This Interesting
- It is framed like an internal business system, not a school project
- It prioritizes action and timing over charting
- It shows data engineering, analytics design, and product thinking in one repo
- It is modular enough to evolve into real connectors, orchestration, or storage later

## Engineering Decisions
- Used simple Python modules instead of frameworks to keep the system legible and extensible
- Kept the pipeline file-based so the project is easy to run locally and easy to inspect
- Simulated raw sources so the repo always produces realistic output without external dependencies
- Used Pandas and NumPy for clarity and speed in normalization and KPI logic
- Separated signals from reporting so decision logic can evolve without rewriting the data pipeline

## What I Learned
- Good analytics systems are as much about prioritization as computation
- Reporting quality changes how technical work is perceived by non-technical stakeholders
- Small structural decisions, like separating KPIs from signals, make the system easier to extend
- Portfolio projects are much stronger when they communicate product judgment, not just implementation

## Current Status
- Strong today:
  The pipeline is fully runnable, generates realistic demo data, calculates KPIs, detects signals, and writes a decision report.
- Still true and improving:
  The system is intentionally lightweight, but the architecture is ready for stronger anomaly logic, real API ingestion, and scheduled execution.

## Repository Structure

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

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

## Outputs
- Executive report: `outputs/creator_report.md`
- Processed tables: `data/processed/`
- Raw source payloads: `data/raw/`

## Repository Engine
The repository is designed around one simple idea:

raw inputs -> normalized metrics -> KPI layer -> signal layer -> executive decisions

That structure keeps the project easy to read while still reflecting how a real internal analytics service would separate concerns.

## Design Philosophy
Most analytics systems stop at dashboards.
This system focuses on translating signals into decisions.

The value is not visual complexity. The value is operational clarity.

## Roadmap
- Replace simulated JSON feeds with connector-style ingestion modules
- Add configurable run windows and report parameters
- Introduce stronger anomaly detection and confidence scoring
- Add historical report snapshots for trend comparison over time
- Expose the pipeline through a lightweight API or internal service layer
