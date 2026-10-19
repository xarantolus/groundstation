from __future__ import annotations

import asyncio
import datetime
from pathlib import Path

import pytest

from auto import events as E
from auto.bus import EventBus
from auto.models import (
    Decoder,
    GroundstationConfig,
    Pass,
    PassInfo,
    PassStatus,
    Satellite,
)
from auto.recorder import RecorderService
from auto.state import StateStore


def _sat(priority: int) -> Satellite:
    return Satellite(
        name=f"SAT{priority}",
        norad="99999",
        frequency=437e6,
        bandwidth=50e3,
        sample_rate=192e3,
        priority=priority,
        decoder=[Decoder(container="ghcr.io/x/y:latest")],
    )


def _pass(sat: Satellite, tmp_path: Path) -> Pass:
    start = datetime.datetime.now()
    pi = PassInfo(
        start_time=start,
        max_time=start + datetime.timedelta(minutes=2),
        end_time=start + datetime.timedelta(minutes=5),
        start_elevation=5,
        max_elevation=45,
        end_elevation=5,
        start_azimuth=0,
        max_azimuth=90,
        end_azimuth=180,
        duration_minutes=5,
        omm={},
    )
    return Pass(
        id=Pass.make_id(sat, pi),
        satellite=sat,
        pass_info=pi,
        pass_dir=str(tmp_path / "pass"),
    )


def _cfg(tmp_path: Path, threshold: int | None) -> GroundstationConfig:
    return GroundstationConfig(
        satellites=[_sat(1)],
        nas_directory=str(tmp_path / "nas"),
        location_lat=52.0,
        location_lon=13.0,
        location_alt=50.0,
        always_record_priority=threshold,
    )


def _recorder(tmp_path: Path, threshold: int | None, waiting: int) -> RecorderService:
    return RecorderService(
        _cfg(tmp_path, threshold),
        EventBus(),
        StateStore(str(tmp_path / "state")),
        decoders_waiting=lambda: waiting,
    )


@pytest.mark.parametrize(
    "threshold,priority,waiting,skip",
    [
        (None, 1, 5, False),  # feature disabled
        (7, 61, 5, False),  # must-record satellite
        (7, 7, 5, False),  # threshold is inclusive
        (7, 6, 0, False),  # nothing waiting
        (7, 6, 1, True),
    ],
)
def test_skip_reason(tmp_path: Path, threshold, priority, waiting, skip):
    rec = _recorder(tmp_path, threshold, waiting)
    reason = rec._skip_reason(_pass(_sat(priority), tmp_path))
    assert (reason is not None) == skip


@pytest.mark.asyncio
async def test_skipped_pass_is_skipped_not_failed_and_leaves_recording_alone(tmp_path: Path):
    rec = _recorder(tmp_path, 7, waiting=3)
    sub = rec._bus.subscribe(E.PassStatusChanged, E.RecordingFailed, E.RecordingStarted)
    # Pretend another pass is currently recording.
    running_stop = asyncio.Event()
    running_sdr = asyncio.Event()
    rec._active_stop = running_stop
    rec._active_sdr_released = running_sdr

    p = _pass(_sat(2), tmp_path)
    rec._handle_pass_started(E.PassStarted(pass_=p))
    event = await asyncio.wait_for(sub.get(), timeout=2)

    assert isinstance(event, E.PassStatusChanged)
    assert event.pass_id == p.id and event.status == PassStatus.SKIPPED
    assert "3 decoder run(s) waiting" in (event.reason or "")
    assert p.status == PassStatus.SKIPPED and p.status_reason == event.reason
    # No recording events: the SDR (and the decode gate) are untouched.
    await asyncio.sleep(0.05)
    assert sub._sub.queue.empty()
    assert not running_stop.is_set(), "skip must not stop the running recording"
    assert rec._active_sdr_released is running_sdr
