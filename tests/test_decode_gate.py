from __future__ import annotations

import asyncio
import datetime

import pytest

from auto.decode_gate import DecodeGate


class FakeClock:
    def __init__(self, start: datetime.datetime) -> None:
        self.t = start

    def __call__(self) -> datetime.datetime:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += datetime.timedelta(seconds=seconds)


@pytest.mark.asyncio
async def test_gate_opens_when_no_pass_no_recording():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    assert g.is_open()
    assert "no upcoming pass" in g.last_reason


@pytest.mark.asyncio
async def test_gate_closes_during_recording():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.on_recording(True)
    assert not g.is_open()
    assert "recorder running" in g.last_reason
    g.on_recording(False)
    assert g.is_open()


@pytest.mark.asyncio
async def test_gate_closed_within_safety_margin():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    start = clock.t + datetime.timedelta(minutes=10)
    end = start + datetime.timedelta(minutes=8)
    g.on_next_pass(start, end)
    assert not g.is_open()
    assert "within" in g.last_reason


@pytest.mark.asyncio
async def test_gate_open_outside_safety_margin():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    start = clock.t + datetime.timedelta(minutes=30)
    end = start + datetime.timedelta(minutes=8)
    g.on_next_pass(start, end)
    assert g.is_open()
    assert "safe window" in g.last_reason


@pytest.mark.asyncio
async def test_gate_reopens_after_pass_ends():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    # Inside the safety window, gate closed
    start = clock.t + datetime.timedelta(minutes=5)
    end = start + datetime.timedelta(minutes=8)
    g.on_next_pass(start, end)
    assert not g.is_open()
    # Pretend the pass ended
    g.on_next_pass(None, None)
    assert g.is_open()


@pytest.mark.asyncio
async def test_gate_replaces_timer_when_closer_pass_arrives():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.on_next_pass(clock.t + datetime.timedelta(hours=2), clock.t + datetime.timedelta(hours=2, minutes=8))
    assert g.is_open()
    first_timer = g._timer
    g.on_next_pass(clock.t + datetime.timedelta(minutes=5), clock.t + datetime.timedelta(minutes=13))
    assert not g.is_open()
    assert first_timer is not g._timer
    # The old timer was cancelled, the new one scheduled
    await asyncio.sleep(0)
    assert first_timer.cancelled() or first_timer.done()


@pytest.mark.asyncio
async def test_wait_open_unblocks():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.on_recording(True)

    async def waiter():
        await g.wait_open()
        return clock.t

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert not task.done()
    g.on_recording(False)
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result == clock.t


@pytest.mark.asyncio
async def test_on_change_callback_fires_on_state_change():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    events = []
    g = DecodeGate(
        safety_minutes=15,
        clock=clock,
        on_change=lambda open_, reason: events.append((open_, reason)),
    )
    # initial state is "no upcoming pass" (open)
    assert events[-1] == (True, "no upcoming pass")
    g.on_recording(True)
    assert events[-1] == (False, "recorder running")
    g.on_recording(False)
    assert events[-1] == (True, "no upcoming pass")


@pytest.mark.asyncio
async def test_gate_fails_open_on_exception():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))

    class Boom:
        def __sub__(self, other):
            raise RuntimeError("boom")
        def __rsub__(self, other):
            raise RuntimeError("boom")

    g = DecodeGate(safety_minutes=15, clock=clock)
    # Force an exception path by injecting a bad "next_start"
    g._next_start = Boom()  # type: ignore
    g._reevaluate()
    # Should fail open rather than stay stuck
    assert g.is_open()
    assert "fail open" in g.last_reason
