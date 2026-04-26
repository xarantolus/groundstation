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
async def test_gate_closed_until_predictor_ready():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    assert not g.is_open()
    assert "awaiting first pass prediction" in g.last_reason


@pytest.mark.asyncio
async def test_gate_opens_when_no_pass_no_recording():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.mark_ready()
    assert g.is_open()
    assert "no upcoming pass" in g.last_reason


@pytest.mark.asyncio
async def test_gate_closes_during_recording():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.mark_ready()
    g.on_recording(True)
    assert not g.is_open()
    assert "recorder running" in g.last_reason
    g.on_recording(False)
    assert g.is_open()


@pytest.mark.asyncio
async def test_gate_closed_within_safety_margin():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.mark_ready()
    start = clock.t + datetime.timedelta(minutes=10)
    end = start + datetime.timedelta(minutes=8)
    g.on_next_pass(start, end)
    assert not g.is_open()
    assert "within" in g.last_reason


@pytest.mark.asyncio
async def test_gate_open_outside_safety_margin():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.mark_ready()
    start = clock.t + datetime.timedelta(minutes=30)
    end = start + datetime.timedelta(minutes=8)
    g.on_next_pass(start, end)
    assert g.is_open()
    assert "safe window" in g.last_reason


@pytest.mark.asyncio
async def test_gate_reopens_after_pass_ends():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=15, clock=clock)
    g.mark_ready()
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
    g.mark_ready()
    g.on_next_pass(clock.t + datetime.timedelta(hours=2), clock.t + datetime.timedelta(hours=2, minutes=8))
    assert g.is_open()
    first_timer = g._timer
    assert first_timer is not None
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
    g.mark_ready()
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
    # initial state is closed until the first prediction lands
    assert events[-1] == (False, "awaiting first pass prediction")
    g.mark_ready()
    assert events[-1] == (True, "no upcoming pass")
    g.on_recording(True)
    assert events[-1] == (False, "recorder running")
    g.on_recording(False)
    assert events[-1] == (True, "no upcoming pass")


@pytest.mark.asyncio
async def test_cpu_slot_min_safety_defers_when_pass_is_close():
    # Default gate safety = 3 min. Compression asks for 10 min — at 5 min
    # before pass, the gate is open for decoders but compression must wait.
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=3, clock=clock)
    g.mark_ready()
    pass_start = clock.t + datetime.timedelta(minutes=5)
    pass_end = pass_start + datetime.timedelta(minutes=5)
    g.on_next_pass(pass_start, pass_end)
    assert g.is_open()  # 5 min > 3 min safety, decoder slot would proceed

    entered = asyncio.Event()

    async def take_compression_slot():
        async with g.cpu_slot(min_safety_minutes=10.0):
            entered.set()

    task = asyncio.create_task(take_compression_slot())
    await asyncio.sleep(0.05)
    assert not entered.is_set(), "compression must not start with pass within 10 min"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_cpu_slot_min_safety_runs_when_pass_is_far():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=3, clock=clock)
    g.mark_ready()
    pass_start = clock.t + datetime.timedelta(minutes=30)
    pass_end = pass_start + datetime.timedelta(minutes=10)
    g.on_next_pass(pass_start, pass_end)

    async def take_slot():
        async with g.cpu_slot(min_safety_minutes=10.0):
            return True

    result = await asyncio.wait_for(take_slot(), timeout=1.0)
    assert result is True


@pytest.mark.asyncio
async def test_gate_close_kills_active_slot():
    # Active CPU work must die when the gate closes for any reason — not
    # just when the recorder actually starts. Otherwise compression keeps
    # burning cores during the safety window.
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))
    g = DecodeGate(safety_minutes=3, clock=clock)
    g.mark_ready()
    pass_start = clock.t + datetime.timedelta(minutes=30)
    pass_end = pass_start + datetime.timedelta(minutes=10)
    g.on_next_pass(pass_start, pass_end)
    assert g.is_open()

    killed = asyncio.Event()

    async def hold_slot():
        async with g.cpu_slot() as kill:
            await asyncio.wait_for(kill.wait(), timeout=1.0)
            killed.set()

    task = asyncio.create_task(hold_slot())
    await asyncio.sleep(0.01)
    # Move the predicted pass closer so the gate transitions open→closed.
    g.on_next_pass(clock.t + datetime.timedelta(minutes=2), pass_end)
    assert not g.is_open()
    await asyncio.wait_for(task, timeout=1.0)
    assert killed.is_set()


@pytest.mark.asyncio
async def test_gate_fails_open_on_exception():
    clock = FakeClock(datetime.datetime(2026, 1, 1, 10, 0, 0))

    class Boom:
        def __sub__(self, other):
            raise RuntimeError("boom")
        def __rsub__(self, other):
            raise RuntimeError("boom")

    g = DecodeGate(safety_minutes=15, clock=clock)
    g.mark_ready()
    # Force an exception path by injecting a bad "next_start"
    g._next_start = Boom()  # type: ignore
    g._reevaluate()
    # Should fail open rather than stay stuck
    assert g.is_open()
    assert "fail open" in g.last_reason
