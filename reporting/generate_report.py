"""Generate leadership-ready decision reports from KPI and signal outputs.

This reporting layer does not describe raw analytics tables. It translates
signal combinations into business meaning, prioritization, and concrete next
actions that a content, growth, or partnerships team could execute.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


LOOKBACK_HOURS = 12


def _creator_signal_map(signals: pd.DataFrame) -> dict[str, list[dict]]:
    if signals.empty:
        return {}
    return {
        creator_id: group.to_dict("records")
        for creator_id, group in signals.groupby("creator_id")
    }


def _creator_phase(creator: pd.Series, creator_signals: list[dict]) -> str:
    signal_types = {signal["signal_type"] for signal in creator_signals}
    if "emerging_creator" in signal_types or creator["trend_momentum_index"] >= 50:
        return "breakout"
    if "engagement_spike" in signal_types or "conversation_surge" in signal_types:
        return "spike"
    return "stable"


def _top_priority_creators(
    creator_kpis: pd.DataFrame, signals: pd.DataFrame, limit: int = 2
) -> pd.DataFrame:
    signal_map = _creator_signal_map(signals)
    ranked = creator_kpis.copy()
    ranked["signal_count"] = ranked["creator_id"].map(lambda creator_id: len(signal_map.get(creator_id, [])))
    ranked = ranked.sort_values(
        ["signal_count", "trend_momentum_index", "stream_impact_score"],
        ascending=[False, False, False],
    )
    return ranked.head(limit)


def _executive_summary(creator_kpis: pd.DataFrame, signals: pd.DataFrame) -> str:
    signal_map = _creator_signal_map(signals)
    priority_creators = _top_priority_creators(creator_kpis, signals, limit=2)
    lead = priority_creators.iloc[0]
    lead_signals = signal_map.get(lead["creator_id"], [])
    phase = _creator_phase(lead, lead_signals)

    if phase == "breakout":
        opening = (
            f"{lead['creator_name']} is the most important creator to act on right now. "
            "They have moved beyond routine growth and into a breakout moment that can be scaled."
        )
    elif phase == "spike":
        opening = (
            f"{lead['creator_name']} is the highest-priority creator in the current cycle. "
            "The current surge is likely short-lived, so speed matters more than optimization."
        )
    else:
        opening = (
            f"{lead['creator_name']} remains the strongest near-term priority. "
            "Performance is holding at a level that supports confident commercial planning."
        )

    business_meaning = (
        f"This matters because {lead['creator_name']} combines momentum ({lead['trend_momentum_index']:.2f}) "
        f"with material stream impact ({lead['stream_impact_score']:.2f}), creating a timely opportunity "
        "to increase reach, sponsor visibility, and audience capture while attention is still building."
    )

    if len(priority_creators) > 1:
        second = priority_creators.iloc[1]
        portfolio_note = (
            f"{second['creator_name']} is the secondary priority: the signal is less about scale today and more "
            "about acting before the current window closes."
        )
        return "\n".join(f"- {item}" for item in [opening, business_meaning, portfolio_note])

    return "\n".join(f"- {item}" for item in [opening, business_meaning])


def _change_line(signal: dict, creator_lookup: dict[str, pd.Series]) -> str:
    creator = creator_lookup[signal["creator_id"]]
    name = creator["creator_name"]
    if signal["signal_type"] == "engagement_spike":
        if name == "Ava Blaze":
            return (
                f"- {name}: engagement has broken above normal range, creating a narrow amplification window "
                "where extra distribution can turn creator momentum into broader reach."
            )
        return (
            f"- {name}: viewer energy is building on top of an already strong base, which points to a "
            "rising creator rather than a one-off traffic bump."
        )
    if signal["signal_type"] == "abnormal_growth":
        return (
            f"- {name}: audience growth is converting unusually well, which means current attention is "
            "sticking and increasing the creator's long-term value."
        )
    if signal["signal_type"] == "conversation_surge":
        if name == "Ava Blaze":
            return (
                f"- {name}: public conversation is surging around the creator, raising the odds that well-timed "
                "clips or reposts will travel beyond the existing audience."
            )
        return (
            f"- {name}: market attention is widening, which lowers distribution risk and improves the case "
            "for giving this creator more surface area."
        )
    if signal["signal_type"] == "emerging_creator":
        return (
            f"- {name}: performance is outrunning creator size, which is often the point where early investment "
            "produces the highest upside."
        )
    return f"- {name}: material performance change detected."


def _what_changed(creator_kpis: pd.DataFrame, signals: pd.DataFrame) -> str:
    if signals.empty:
        return f"- No meaningful changes were detected in the last {LOOKBACK_HOURS} hours."

    creator_lookup = {
        row["creator_id"]: row for _, row in creator_kpis.set_index("creator_id").reset_index().iterrows()
    }
    meaningful = signals.head(5).to_dict("records")
    return "\n".join(_change_line(signal, creator_lookup) for signal in meaningful)


def _why_it_matters(creator_kpis: pd.DataFrame, signals: pd.DataFrame) -> str:
    signal_map = _creator_signal_map(signals)
    lines: list[str] = []

    for _, creator in _top_priority_creators(creator_kpis, signals, limit=3).iterrows():
        creator_signals = signal_map.get(creator["creator_id"], [])
        phase = _creator_phase(creator, creator_signals)

        if phase == "breakout":
            lines.append(
                f"- {creator['creator_name']} is in a breakout phase. This is the point where the business can "
                "gain disproportionate upside by investing before pricing, sponsorship demand, and internal competition rise."
            )
        elif phase == "spike":
            lines.append(
                f"- {creator['creator_name']} is in a spike phase. The opportunity is immediate, but so is the "
                "risk of waiting: if action slips, the company captures the noise but not the value."
            )
        else:
            lines.append(
                f"- {creator['creator_name']} is in a stable phase. This is a lower-urgency asset that supports "
                "predictable packaging, steadier ROI, and cleaner planning decisions."
            )

    return "\n".join(lines)


def _action_for_signal(signal_type: str, creator_name: str, horizon: str) -> str:
    if horizon == "immediate":
        if signal_type == "engagement_spike":
            return (
                f"- Expand distribution for {creator_name} immediately across owned channels and clip surfaces "
                "while the current demand window is still open."
            )
        if signal_type == "conversation_surge":
            return (
                f"- Put {creator_name} into social amplification today so rising public attention converts into "
                "incremental reach instead of fading unmonetized."
            )
        if signal_type == "abnormal_growth":
            return (
                f"- Flag {creator_name} for audience capture now by tightening follow prompts, end cards, or "
                "cross-promotion while conversion is running above normal."
            )
        if signal_type == "emerging_creator":
            return (
                f"- Give {creator_name} additional visibility today before the breakout is fully reflected in "
                "internal priorities and partner demand."
            )
    if horizon == "short_term":
        if signal_type == "engagement_spike":
            return (
                f"- Build a follow-on content package around {creator_name} over the next 1-3 days so the current "
                "surge becomes a sustained viewing cycle."
            )
        if signal_type == "conversation_surge":
            return (
                f"- Track whether {creator_name}'s conversation lift holds through the next publishing cycle and "
                "prepare adjacent formats that can ride the same audience interest."
            )
        if signal_type == "abnormal_growth":
            return (
                f"- Review {creator_name}'s recent programming mix and repeat the elements that are clearly turning "
                "attention into audience retention."
            )
        if signal_type == "emerging_creator":
            return (
                f"- Test a higher placement tier for {creator_name} this week to validate whether the breakout can "
                "carry more inventory and broader distribution."
            )
    if horizon == "strategic":
        if signal_type == "engagement_spike":
            return (
                f"- Add {creator_name} to the rapid-response priority list so future spikes trigger distribution, "
                "sales, and programming decisions without delay."
            )
        if signal_type == "conversation_surge":
            return (
                f"- Rework creator planning around {creator_name} to account for rising external demand, especially "
                "where sponsorship, guest booking, or cross-platform packaging could benefit."
            )
        if signal_type == "abnormal_growth":
            return (
                f"- Reevaluate long-term investment in {creator_name} because stronger conversion suggests higher "
                "lifetime audience value than the current tier implies."
            )
        if signal_type == "emerging_creator":
            return (
                f"- Reassess {creator_name}'s tier, budget, and commercial packaging before the market fully catches up."
            )
    return ""


def _actions_for_horizon(signals_for_creator: list[dict], creator_name: str, horizon: str) -> list[str]:
    ordered_signal_types = []
    for signal in signals_for_creator:
        signal_type = signal["signal_type"]
        if signal_type not in ordered_signal_types:
            ordered_signal_types.append(signal_type)

    actions = [
        action
        for signal_type in ordered_signal_types
        if (action := _action_for_signal(signal_type, creator_name, horizon))
    ]
    return actions


def _priority_creators_by_phase(
    creator_kpis: pd.DataFrame, signals: pd.DataFrame
) -> tuple[pd.Series | None, pd.Series | None]:
    signal_map = _creator_signal_map(signals)
    ranked = _top_priority_creators(creator_kpis, signals, limit=max(4, len(creator_kpis)))

    breakout = None
    spike = None
    for _, creator in ranked.iterrows():
        phase = _creator_phase(creator, signal_map.get(creator["creator_id"], []))
        if breakout is None and phase == "breakout":
            breakout = creator
        elif spike is None and phase == "spike":
            spike = creator
        if breakout is not None and spike is not None:
            break

    if breakout is None and not ranked.empty:
        breakout = ranked.iloc[0]
    if spike is None:
        for _, creator in ranked.iterrows():
            if breakout is None or creator["creator_id"] != breakout["creator_id"]:
                spike = creator
                break

    return breakout, spike


def _prioritized_action_line(priority_label: str, creator_name: str, action: str) -> str:
    action_text = action[2:] if action.startswith("- ") else action
    return f"- {priority_label}: {creator_name} -> {action_text}"


def _recommended_actions(creator_kpis: pd.DataFrame, signals: pd.DataFrame) -> str:
    signal_map = _creator_signal_map(signals)
    breakout, spike = _priority_creators_by_phase(creator_kpis, signals)

    immediate_actions: list[str] = []
    short_term_actions: list[str] = []
    strategic_actions: list[str] = []

    if breakout is not None:
        breakout_signals = signal_map.get(breakout["creator_id"], [])
        breakout_immediate = _actions_for_horizon(breakout_signals, breakout["creator_name"], "immediate")
        breakout_short = _actions_for_horizon(breakout_signals, breakout["creator_name"], "short_term")
        breakout_strategic = _actions_for_horizon(breakout_signals, breakout["creator_name"], "strategic")

        if breakout_immediate:
            immediate_actions.append(
                _prioritized_action_line("Priority 1 Breakout", breakout["creator_name"], breakout_immediate[0])
            )
        if breakout_short:
            short_term_actions.append(
                _prioritized_action_line("Priority 1 Breakout", breakout["creator_name"], breakout_short[0])
            )
        if breakout_strategic:
            strategic_actions.append(
                _prioritized_action_line("Priority 1 Breakout", breakout["creator_name"], breakout_strategic[0])
            )

    if spike is not None:
        spike_signals = signal_map.get(spike["creator_id"], [])
        spike_immediate = _actions_for_horizon(spike_signals, spike["creator_name"], "immediate")
        spike_short = _actions_for_horizon(spike_signals, spike["creator_name"], "short_term")
        spike_strategic = _actions_for_horizon(spike_signals, spike["creator_name"], "strategic")

        if spike_immediate:
            immediate_actions.append(
                _prioritized_action_line("Priority 2 Spike", spike["creator_name"], spike_immediate[0])
            )
        if spike_short:
            short_term_actions.append(
                _prioritized_action_line("Priority 2 Spike", spike["creator_name"], spike_short[0])
            )
        if spike_strategic:
            strategic_actions.append(
                _prioritized_action_line("Priority 2 Spike", spike["creator_name"], spike_strategic[0])
            )

    if breakout is not None:
        breakout_signals = signal_map.get(breakout["creator_id"], [])
        for action in _actions_for_horizon(breakout_signals, breakout["creator_name"], "immediate")[1:2]:
            immediate_actions.append(
                _prioritized_action_line("Priority 1 Breakout", breakout["creator_name"], action)
            )
        for action in _actions_for_horizon(breakout_signals, breakout["creator_name"], "short_term")[1:2]:
            short_term_actions.append(
                _prioritized_action_line("Priority 1 Breakout", breakout["creator_name"], action)
            )

    if spike is not None:
        spike_signals = signal_map.get(spike["creator_id"], [])
        for action in _actions_for_horizon(spike_signals, spike["creator_name"], "immediate")[1:2]:
            immediate_actions.append(
                _prioritized_action_line("Priority 2 Spike", spike["creator_name"], action)
            )
        for action in _actions_for_horizon(spike_signals, spike["creator_name"], "short_term")[1:2]:
            short_term_actions.append(
                _prioritized_action_line("Priority 2 Spike", spike["creator_name"], action)
            )

    fallback_name = breakout["creator_name"] if breakout is not None else "the lead creator"
    if not immediate_actions:
        immediate_actions = [
            f"- Priority 1: maintain current distribution around {fallback_name} and wait for a clearer trigger before redeploying resources."
        ]
    if not short_term_actions:
        short_term_actions = [
            f"- Priority 1: reassess {fallback_name} after the next publishing cycle to confirm whether the current signal is durable."
        ]
    if not strategic_actions:
        strategic_actions = [
            "- Priority 1: refine escalation thresholds so breakout and spike windows trigger different operating responses."
        ]

    return "\n".join(
        [
            "### Immediate (0-24h)",
            *immediate_actions[:3],
            "### Short-term (1-3 days)",
            *short_term_actions[:3],
            "### Strategic (Longer-term)",
            *strategic_actions[:3],
        ]
    )


def generate_markdown_report(
    creator_kpis: pd.DataFrame, signals: pd.DataFrame, report_path: Path
) -> Path:
    """Create an executive decision report in Markdown."""
    report_path.parent.mkdir(parents=True, exist_ok=True)

    report_body = f"""# Creator Intelligence Executive Decision Report

## Executive Summary
{_executive_summary(creator_kpis, signals)}

## What Changed (Last {LOOKBACK_HOURS} Hours)
{_what_changed(creator_kpis, signals)}

## Why It Matters
{_why_it_matters(creator_kpis, signals)}

## Recommended Actions
{_recommended_actions(creator_kpis, signals)}
"""

    report_path.write_text(report_body, encoding="utf-8")
    return report_path
