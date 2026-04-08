"""CLI entrypoint for the Creator Intelligence Layer pipeline."""

from __future__ import annotations

from pathlib import Path

from ingestion.simulate_sources import write_simulated_sources
from processing.kpis import calculate_kpis, write_kpi_tables
from processing.normalize import load_raw_sources, normalize_sources, write_processed_dataset
from reporting.generate_report import generate_markdown_report
from signals.trend_detection import detect_signals, write_signals


def _log_stage(stage: str, detail: str) -> None:
    """Print clean, pipeline-style progress logs for local runs."""
    print(f"[creator-intelligence] {stage:<12} {detail}")


def main() -> None:
    """Run ingestion, processing, signal detection, and executive reporting."""
    project_root = Path(__file__).resolve().parent
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    output_dir = project_root / "outputs"

    _log_stage("ingestion", "Generating simulated source feeds")
    write_simulated_sources(raw_dir=raw_dir, hours=24)

    _log_stage("processing", "Loading raw sources")
    raw_sources = load_raw_sources(raw_dir)

    _log_stage("processing", "Normalizing creator, stream, and social data")
    unified = normalize_sources(raw_sources)

    _log_stage("kpis", "Calculating performance, impact, and momentum scores")
    creator_kpis, latest_snapshot = calculate_kpis(unified)

    _log_stage("signals", "Detecting spikes, growth shifts, and breakout creators")
    signals = detect_signals(unified, latest_snapshot)

    write_processed_dataset(unified, processed_dir)
    write_kpi_tables(creator_kpis, latest_snapshot, processed_dir)
    write_signals(signals, processed_dir)

    _log_stage("reporting", "Generating executive decision report")
    report_path = output_dir / "creator_report.md"
    generate_markdown_report(creator_kpis, signals, report_path)

    _log_stage("complete", f"Report written to {report_path}")


if __name__ == "__main__":
    main()
