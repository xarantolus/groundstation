from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator, Iterable, Literal, Tuple, Type

from .events import Event

logger = logging.getLogger("groundstation.bus")

Overflow = Literal["drop_oldest", "block"]


class _Subscriber:
    __slots__ = ("types", "queue", "overflow", "name", "dropped")

    def __init__(
        self,
        types: Tuple[Type[Event], ...],
        queue: asyncio.Queue,
        overflow: Overflow,
        name: str,
    ):
        self.types = types
        self.queue = queue
        self.overflow = overflow
        self.name = name
        self.dropped = 0


class EventBus:
    def __init__(self) -> None:
        self._subs: list[_Subscriber] = []
        self._lock = asyncio.Lock()

    async def publish(self, event: Event) -> None:
        for sub in list(self._subs):
            if sub.types and not isinstance(event, sub.types):
                continue
            if sub.overflow == "drop_oldest":
                while True:
                    try:
                        sub.queue.put_nowait(event)
                        break
                    except asyncio.QueueFull:
                        try:
                            sub.queue.get_nowait()
                            sub.dropped += 1
                            if sub.dropped == 1 or sub.dropped % 100 == 0:
                                logger.warning(
                                    "subscriber %s dropped %d events (backpressure)",
                                    sub.name,
                                    sub.dropped,
                                )
                        except asyncio.QueueEmpty:
                            pass
            else:
                await sub.queue.put(event)

    def subscribe(
        self,
        *types: Type[Event],
        queue_size: int = 256,
        overflow: Overflow = "block",
        name: str = "anon",
    ) -> Subscription:
        sub = _Subscriber(
            tuple(types),
            asyncio.Queue(maxsize=queue_size),
            overflow,
            name,
        )
        self._subs.append(sub)
        return Subscription(self, sub)

    def _detach(self, sub: _Subscriber) -> None:
        try:
            self._subs.remove(sub)
        except ValueError:
            pass


_CLOSE_SENTINEL = object()


class Subscription:
    def __init__(self, bus: EventBus, sub: _Subscriber) -> None:
        self._bus = bus
        self._sub = sub
        self._closed = False

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Event]:
        try:
            while True:
                event = await self._sub.queue.get()
                if event is _CLOSE_SENTINEL:
                    break
                yield event  # type: ignore[misc]
        finally:
            self.close()

    async def get(self) -> Event:
        return await self._sub.queue.get()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._bus._detach(self._sub)
        # Wake any consumer parked on queue.get(). If the queue is full we
        # drop the oldest item to make room — callers are shutting down and
        # won't miss the event.
        q = self._sub.queue
        while True:
            try:
                q.put_nowait(_CLOSE_SENTINEL)
                return
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    return


async def run_subscriber(
    sub: Subscription,
    handle: "callable",
    name: str,
) -> None:
    """Drain a subscription, calling `handle(event)` per event. Catches
    every exception so a buggy handler cannot starve other subscribers."""
    async for event in sub:
        try:
            result = handle(event)
            if asyncio.iscoroutine(result):
                await result
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("subscriber %s failed handling event", name)


def event_types() -> Iterable[Type[Event]]:
    """Convenience for tests and UIs that want to subscribe to everything."""
    from . import events as _e

    return (
        _e.PassPredicted,
        _e.PassesTableChanged,
        _e.PassUpcoming,
        _e.PassStarted,
        _e.PassEnded,
        _e.RecordingStarted,
        _e.RecordingLog,
        _e.RecordingCompleted,
        _e.RecordingFailed,
        _e.DecodeQueued,
        _e.DecodeStarted,
        _e.DecodeLog,
        _e.DecodeCompleted,
        _e.DecodeFailed,
        _e.DecodeGateStateChanged,
        _e.TransferQueued,
        _e.TransferStarted,
        _e.TransferProgress,
        _e.TransferCompleted,
        _e.TransferFailed,
        _e.LogMessage,
        _e.ShutdownRequested,
    )
