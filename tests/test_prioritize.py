from __future__ import annotations

import datetime

import pytest

from auto.models import Decoder, PassInfo, Satellite
from auto.pass_predictor import (
    ELEVATION_SCORE_DIVISOR,
    MIN_RECORDING_WINDOW,
    prioritize,
)


T0 = datetime.datetime(2026, 1, 1, 12, 0, 0)


def _sat(name: str, priority: int) -> Satellite:
    return Satellite(
        name=name,
        norad="0",
        frequency=137e6,
        bandwidth=50e3,
        sample_rate=192e3,
        priority=priority,
        decoder=[Decoder(container="ghcr.io/x/y:latest")],
    )


def _pi(start_min: float, duration_min: float, max_elev: float) -> PassInfo:
    start = T0 + datetime.timedelta(minutes=start_min)
    end = start + datetime.timedelta(minutes=duration_min)
    mid = start + datetime.timedelta(minutes=duration_min / 2)
    return PassInfo(
        start_time=start,
        max_time=mid,
        end_time=end,
        start_elevation=5,
        max_elevation=max_elev,
        end_elevation=5,
        start_azimuth=0,
        max_azimuth=90,
        end_azimuth=180,
        duration_minutes=duration_min,
        tle1="t1",
        tle2="t2",
    )


def _names(picked):
    return [s.name for s, _ in picked]


def test_pri10_5deg_beats_pri6_60deg():
    a = (_sat("IMAGE", 10), _pi(0, 10, 5))
    b = (_sat("AX100", 6), _pi(0, 10, 60))
    picked = prioritize([a, b])
    assert _names(picked) == ["IMAGE"]


def test_pri10_60deg_beats_pri6_80deg():
    a = (_sat("IMAGE", 10), _pi(0, 10, 60))
    b = (_sat("AX100", 6), _pi(0, 10, 80))
    picked = prioritize([a, b])
    assert _names(picked) == ["IMAGE"]


def test_pri7_5deg_loses_to_pri6_80deg():
    a = (_sat("LOW", 7), _pi(0, 10, 5))
    b = (_sat("HIGH", 6), _pi(0, 10, 80))
    picked = prioritize([a, b])
    assert _names(picked) == ["HIGH"]


def test_pri7_40deg_beats_pri6_40deg_tie():
    a = (_sat("PRI7", 7), _pi(0, 10, 40))
    b = (_sat("PRI6", 6), _pi(0, 10, 40))
    picked = prioritize([a, b])
    assert _names(picked) == ["PRI7"]


def test_same_priority_elevation_decides():
    a = (_sat("LOW_EL", 6), _pi(0, 10, 10))
    b = (_sat("HIGH_EL", 6), _pi(0, 10, 50))
    picked = prioritize([a, b])
    assert _names(picked) == ["HIGH_EL"]


def test_transitive_chain_does_not_lose_far_high_priority():
    # A overlaps B, B overlaps C, A doesn't overlap C: pre-fix this skipped C.
    a = (_sat("A_PRI10", 10), _pi(0, 9, 30))
    b = (_sat("B_PRI7", 7), _pi(5, 9, 30))
    c = (_sat("C_PRI10", 10), _pi(9, 8, 50))
    picked = prioritize(sorted([a, b, c], key=lambda t: t[1].start_time))
    names = _names(picked)
    assert "A_PRI10" in names
    assert "C_PRI10" in names
    assert "B_PRI7" not in names


def test_sub_two_minute_remainder_dropped():
    a = (_sat("BIG", 10), _pi(0, 10, 30))
    b = (_sat("SMALL", 6), _pi(5, 2, 30))
    picked = prioritize([a, b])
    assert _names(picked) == ["BIG"]


def test_partial_window_keeps_tail():
    a = (_sat("SHORT_HIGH", 10), _pi(0, 5, 30))
    b = (_sat("LONG_LOW", 6), _pi(0, 15, 30))
    picked = prioritize([a, b])
    names = _names(picked)
    assert names == ["SHORT_HIGH", "LONG_LOW"]
    long_low_pi = next(pi for s, pi in picked if s.name == "LONG_LOW")
    assert long_low_pi.recording_start_override == T0 + datetime.timedelta(minutes=5)
    assert long_low_pi.recording_end_override == T0 + datetime.timedelta(minutes=14)


def test_partial_window_keeps_lead():
    a = (_sat("LATE_HIGH", 10), _pi(10, 5, 30))
    b = (_sat("LONG_LOW", 6), _pi(0, 15, 30))
    picked = prioritize(sorted([a, b], key=lambda t: t[1].start_time))
    names = _names(picked)
    assert "LATE_HIGH" in names
    assert "LONG_LOW" in names
    long_low_pi = next(pi for s, pi in picked if s.name == "LONG_LOW")
    assert long_low_pi.recording_start_override == T0 + datetime.timedelta(minutes=1)
    assert long_low_pi.recording_end_override == T0 + datetime.timedelta(minutes=10)


def test_useless_edge_dropped_when_only_first_minute_free():
    # Winner covers minute 1 onward of the candidate; only the candidate's
    # first minute is free, but that's the useless-edge minute → drop.
    a = (_sat("WINNER", 10), _pi(1, 10, 30))
    b = (_sat("LOSER", 6), _pi(0, 6, 30))
    picked = prioritize([a, b])
    assert _names(picked) == ["WINNER"]


def test_short_pass_dropped_for_lacking_useful_window():
    a = (_sat("WINNER", 10), _pi(0, 30, 30))
    b = (_sat("TINY", 6), _pi(5, 3, 30))
    picked = prioritize([a, b])
    assert _names(picked) == ["WINNER"]


def test_no_overlap_passthrough():
    a = (_sat("A", 6), _pi(0, 5, 30))
    b = (_sat("B", 6), _pi(10, 5, 30))
    picked = prioritize([a, b])
    assert _names(picked) == ["A", "B"]


def test_min_window_constant_is_two_minutes():
    assert MIN_RECORDING_WINDOW == datetime.timedelta(minutes=2)


def test_score_divisor_value():
    assert ELEVATION_SCORE_DIVISOR == 30.0


def test_same_priority_offset_peaks_split_at_crossover():
    # Two pri-6 passes with the same elevation profile but offset peaks.
    # Each should get the half centred on its own max — the old algorithm
    # gave one of them everything and the other only the non-overlapping
    # tail. Both end up "trimmed" so USELESS_EDGE clips a minute off each
    # outside boundary too.
    a = (_sat("EARLY_PEAK", 6), _pi(0, 10, 80))   # peak at 5
    b = (_sat("LATE_PEAK", 6), _pi(5, 10, 80))    # peak at 10
    picked = prioritize(sorted([a, b], key=lambda t: t[1].start_time))
    names = _names(picked)
    assert "EARLY_PEAK" in names
    assert "LATE_PEAK" in names
    early = next(pi for s, pi in picked if s.name == "EARLY_PEAK")
    late = next(pi for s, pi in picked if s.name == "LATE_PEAK")
    # EARLY_PEAK keeps its peak; window is [start+1, ~crossover].
    assert early.recording_start_override == T0 + datetime.timedelta(minutes=1)
    assert early.recording_end_override is not None
    assert (
        T0 + datetime.timedelta(minutes=7) <= early.recording_end_override
        <= T0 + datetime.timedelta(minutes=8)
    )
    # LATE_PEAK keeps its peak; window is [~crossover, end-1].
    assert late.recording_start_override is not None
    assert late.recording_end_override == T0 + datetime.timedelta(minutes=14)
    assert (
        T0 + datetime.timedelta(minutes=7) <= late.recording_start_override
        <= T0 + datetime.timedelta(minutes=8)
    )


def test_score_handoff_aligns_passes():
    # Adjacent winning windows must abut at the score crossover so the
    # recorder hands the SDR over without an explicit kill — the loser's
    # end and the next winner's start should match.
    a = (_sat("FIRST_PEAK", 6), _pi(0, 10, 60))   # peak at 5
    b = (_sat("SECOND_PEAK", 6), _pi(6, 10, 60))  # peak at 11
    picked = prioritize(sorted([a, b], key=lambda t: t[1].start_time))
    first = next(pi for s, pi in picked if s.name == "FIRST_PEAK")
    second = next(pi for s, pi in picked if s.name == "SECOND_PEAK")
    assert first.recording_end_override is not None
    assert second.recording_start_override is not None
    # The two should align (within one slice = 30s)
    delta = abs(
        (first.recording_end_override - second.recording_start_override).total_seconds()
    )
    assert delta <= 30


def test_simulation_table(capsys):
    scenarios = [
        ("pri10/5° vs pri6/60°", _sat("A", 10), _pi(0, 10, 5), _sat("B", 6), _pi(0, 10, 60)),
        ("pri10/60° vs pri6/80°", _sat("A", 10), _pi(0, 10, 60), _sat("B", 6), _pi(0, 10, 80)),
        ("pri7/5° vs pri6/80°", _sat("A", 7), _pi(0, 10, 5), _sat("B", 6), _pi(0, 10, 80)),
        ("pri7/40° vs pri6/40°", _sat("A", 7), _pi(0, 10, 40), _sat("B", 6), _pi(0, 10, 40)),
        ("pri6/10° vs pri6/50°", _sat("A", 6), _pi(0, 10, 10), _sat("B", 6), _pi(0, 10, 50)),
        ("pri7/15° vs pri6/45°", _sat("A", 7), _pi(0, 10, 15), _sat("B", 6), _pi(0, 10, 45)),
        ("pri7/29° vs pri6/59°", _sat("A", 7), _pi(0, 10, 29), _sat("B", 6), _pi(0, 10, 59)),
        ("pri7/30° vs pri6/59°", _sat("A", 7), _pi(0, 10, 30), _sat("B", 6), _pi(0, 10, 59)),
    ]
    print(f"\n{'scenario':<28} {'winner (D=30)':<14}")
    print("-" * 44)
    for label, sa, pia, sb, pib in scenarios:
        picked = prioritize([(sa, pia), (sb, pib)])
        winner = picked[0][0].name
        winner_label = f"{sa.name}(p{sa.priority}/{pia.max_elevation:.0f}°)" if winner == sa.name else f"{sb.name}(p{sb.priority}/{pib.max_elevation:.0f}°)"
        print(f"{label:<28} {winner_label:<14}")
    captured = capsys.readouterr()
    assert "scenario" in captured.out
