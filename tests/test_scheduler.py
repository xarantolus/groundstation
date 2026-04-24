from __future__ import annotations

import asyncio
import datetime
from pathlib import Path

import pytest

from auto.bus import EventBus
from auto import events as E
from auto.decode_gate import DecodeGate
from auto.models import (
    Decoder,
    GroundstationConfig,
    Pass,
    PassInfo,
    PassStatus,
    Satellite,
)
from auto.scheduler import SchedulerService
from auto.state import StateStore


def _sat(name: str = "TESTSAT") -> Satellite:
    return Satellite(
        name=name,
        norad="99999",
        frequency=137e6,
        bandwidth=50e3,
        sample_rate=192e3,
        decoder=[Decoder(container="ghcr.io/x/y:latest")],
    )


def _pi(start: datetime.datetime, duration_min: float = 5.0) -> PassInfo:
    return PassInfo(
        start_time=start,
        max_time=start + datetime.timedelta(minutes=duration_min / 2),
        end_time=start + datetime.timedelta(minutes=duration_min),
        start_elevation=5,
        max_elevation=45,
        end_elevation=5,
        start_azimuth=0,
        max_azimuth=90,
        end_azimuth=180,
        duration_minutes=duration_min,
        tle1="t1",
        tle2="t2",
    )


def _cfg(tmp_path: Path) -> GroundstationConfig:
    return GroundstationConfig(
        satellites=[_sat()],
        nas_directory=str(tmp_path / "nas"),
        location_lat=52.0,
        location_lon=13.0,
        location_alt=50.0,
        update_interval_hours=1.0,
        decode_safety_minutes=15.0,
    )


class _FakePredictor:
    def __init__(self, results):
        self._results = results

    def predict_all(self, *_args, **_kwargs):
        return list(self._results)


@pytest.mark.asyncio
async def test_recovered_predicted_pass_gets_monitor(tmp_path: Path):
    """Bug regression: a PREDICTED pass loaded from state at boot must
    get a monitor spawned, otherwise PassStarted never fires and the
    pass is silently dropped."""
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    sat = cfg.satellites[0]
    start = datetime.datetime.now() + datetime.timedelta(hours=1)
    pi = _pi(start)
    p = Pass(id=Pass.make_id(sat, pi), satellite=sat, pass_info=pi, pass_dir="/tmp/x")
    passes: dict[str, Pass] = {p.id: p}

    predictor = _FakePredictor([])  # no fresh predictions — recovered pass is orphaned
    sched = SchedulerService(cfg, bus, state, predictor, gate, passes)

    await sched._tick()

    assert p.id in sched._monitors, "monitor not spawned for recovered PREDICTED pass"
    assert not sched._monitors[p.id].done()

    # cleanup
    sched._monitors[p.id].cancel()


@pytest.mark.asyncio
async def test_future_predicted_pass_not_pruned_when_missing_from_prediction(
    tmp_path: Path,
):
    """A transient TLE-fetch failure must not drop future recovered passes."""
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    sat = cfg.satellites[0]
    start = datetime.datetime.now() + datetime.timedelta(hours=3)
    p = Pass(id="fixed_pid", satellite=sat, pass_info=_pi(start), pass_dir="/tmp/x")
    passes = {p.id: p}

    sched = SchedulerService(cfg, bus, state, _FakePredictor([]), gate, passes)
    await sched._tick()

    assert p.id in passes, "future PREDICTED pass was incorrectly pruned"
    for t in sched._monitors.values():
        t.cancel()


@pytest.mark.asyncio
async def test_past_predicted_pass_is_pruned(tmp_path: Path):
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    sat = cfg.satellites[0]
    start = datetime.datetime.now() - datetime.timedelta(hours=2)
    p = Pass(id="old_pid", satellite=sat, pass_info=_pi(start), pass_dir="/tmp/x")
    passes = {p.id: p}

    sched = SchedulerService(cfg, bus, state, _FakePredictor([]), gate, passes)
    await sched._tick()

    assert p.id not in passes, "expired PREDICTED pass should be pruned"


@pytest.mark.asyncio
async def test_tle_shifted_prediction_does_not_duplicate_pass(tmp_path: Path):
    """A fresh TLE can shift the predicted start_time by a few seconds,
    producing a new pid for the same overpass. The scheduler must detect
    the time-window overlap with an existing non-terminal pass and reuse
    it, otherwise the UI shows the same overpass twice (old pid still
    PREDICTED/RECORDING next to a freshly-spawned PREDICTED one)."""
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    sat = cfg.satellites[0]
    start_old = datetime.datetime.now() + datetime.timedelta(hours=1)
    pi_old = _pi(start_old)
    p = Pass(
        id=Pass.make_id(sat, pi_old),
        satellite=sat,
        pass_info=pi_old,
        pass_dir="/tmp/x",
        status=PassStatus.RECORDING,
    )
    passes = {p.id: p}

    # Fresh prediction shifts start by 2 seconds → different pid for the
    # same overpass.
    pi_new = _pi(start_old + datetime.timedelta(seconds=2))
    predictor = _FakePredictor([(sat, pi_new)])
    sched = SchedulerService(cfg, bus, state, predictor, gate, passes)

    await sched._tick()

    assert list(passes.keys()) == [p.id], (
        f"expected the existing pass to be reused, got {list(passes.keys())}"
    )
    assert passes[p.id].status == PassStatus.RECORDING

    for t in sched._monitors.values():
        t.cancel()


@pytest.mark.asyncio
async def test_overlapping_prediction_refreshes_predicted_pass_info(tmp_path: Path):
    """When the new prediction overlaps an existing PREDICTED pass, the
    scheduler should adopt the fresher pass_info so the monitor (restarted)
    fires against the updated times. Already-in-flight passes are tested
    separately and must NOT have their pass_info overwritten."""
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    sat = cfg.satellites[0]
    start_old = datetime.datetime.now() + datetime.timedelta(hours=1)
    pi_old = _pi(start_old)
    p = Pass(
        id=Pass.make_id(sat, pi_old),
        satellite=sat,
        pass_info=pi_old,
        pass_dir="/tmp/x",
        status=PassStatus.PREDICTED,
    )
    passes = {p.id: p}

    new_start = start_old + datetime.timedelta(seconds=3)
    pi_new = _pi(new_start)
    predictor = _FakePredictor([(sat, pi_new)])
    sched = SchedulerService(cfg, bus, state, predictor, gate, passes)

    await sched._tick()

    assert list(passes.keys()) == [p.id]
    assert passes[p.id].pass_info.start_time == new_start, (
        "PREDICTED pass should adopt the fresher start_time on overlap"
    )

    for t in sched._monitors.values():
        t.cancel()


@pytest.mark.asyncio
async def test_predicted_pass_not_persisted_to_disk(tmp_path: Path):
    """PREDICTED passes live only in memory — the scheduler re-fetches
    them every tick, so save_pass must skip them."""
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    sat = cfg.satellites[0]
    start = datetime.datetime.now() + datetime.timedelta(hours=1)
    pi = _pi(start)
    predictor = _FakePredictor([(sat, pi)])
    passes: dict[str, Pass] = {}
    sched = SchedulerService(cfg, bus, state, predictor, gate, passes)

    await sched._tick()

    # Pass should be in memory…
    assert len(passes) == 1
    # …but not on disk.
    assert list(state.passes_dir.glob("*.json")) == []

    for t in sched._monitors.values():
        t.cancel()


@pytest.mark.asyncio
async def test_monitor_fires_pass_started_for_imminent_pass(tmp_path: Path):
    cfg = _cfg(tmp_path)
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    gate = DecodeGate(safety_minutes=15.0)

    # Pass starts in 300ms so the monitor fires within the test timeout.
    sat = cfg.satellites[0]
    start = datetime.datetime.now() + datetime.timedelta(milliseconds=300)
    pi = _pi(start, duration_min=0.1)  # 6s duration
    p = Pass(id=Pass.make_id(sat, pi), satellite=sat, pass_info=pi, pass_dir="/tmp/x")
    passes = {p.id: p}

    sub = bus.subscribe(E.PassStarted, name="test", queue_size=4)
    sched = SchedulerService(cfg, bus, state, _FakePredictor([]), gate, passes)
    await sched._tick()

    event = await asyncio.wait_for(sub.get(), timeout=2.0)
    assert isinstance(event, E.PassStarted)
    assert event.pass_.id == p.id

    for t in sched._monitors.values():
        t.cancel()
    sub.close()
