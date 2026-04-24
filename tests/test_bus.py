from __future__ import annotations

import asyncio

import pytest

from auto import events as E
from auto.bus import EventBus, run_subscriber


@pytest.mark.asyncio
async def test_publish_fanout_to_all_subscribers():
    bus = EventBus()
    a = bus.subscribe(E.LogMessage, name="a")
    b = bus.subscribe(E.LogMessage, name="b")
    await bus.publish(E.LogMessage(level="INFO", logger="x", message="hi"))
    ev_a = await asyncio.wait_for(a.get(), 1.0)
    ev_b = await asyncio.wait_for(b.get(), 1.0)
    assert ev_a.message == "hi"
    assert ev_b.message == "hi"
    a.close()
    b.close()


@pytest.mark.asyncio
async def test_subscriber_type_filter():
    bus = EventBus()
    sub = bus.subscribe(E.LogMessage, name="a")
    await bus.publish(E.LogMessage(level="INFO", logger="x", message="yes"))
    await bus.publish(E.ShutdownRequested())
    first = await asyncio.wait_for(sub.get(), 1.0)
    assert first.type == "log_message"
    assert sub._sub.queue.empty()  # type: ignore[attr-defined]
    sub.close()


@pytest.mark.asyncio
async def test_drop_oldest_overflow():
    bus = EventBus()
    sub = bus.subscribe(E.LogMessage, queue_size=2, overflow="drop_oldest", name="a")
    for i in range(5):
        await bus.publish(E.LogMessage(level="INFO", logger="x", message=str(i)))
    remaining = []
    while not sub._sub.queue.empty():  # type: ignore[attr-defined]
        remaining.append(await sub.get())
    assert [r.message for r in remaining] == ["3", "4"]
    sub.close()


@pytest.mark.asyncio
async def test_run_subscriber_swallows_handler_exceptions():
    bus = EventBus()
    sub = bus.subscribe(E.LogMessage, name="a")
    calls = []

    async def handler(event):
        calls.append(event.message)
        if event.message == "boom":
            raise RuntimeError("handler bug")

    task = asyncio.create_task(run_subscriber(sub, handler, "a"))
    await bus.publish(E.LogMessage(level="INFO", logger="x", message="one"))
    await bus.publish(E.LogMessage(level="INFO", logger="x", message="boom"))
    await bus.publish(E.LogMessage(level="INFO", logger="x", message="three"))
    await asyncio.sleep(0.05)
    sub.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert calls == ["one", "boom", "three"]
