from __future__ import annotations

import asyncio
import datetime
import logging
import threading

from . import events as E
from .bus import EventBus


class BusLogHandler(logging.Handler):
    """Turns every log record into a LogMessage event on the bus.

    Safe to call from any thread — scheduling the publish uses
    ``call_soon_threadsafe`` with the loop captured at construction time.
    Failures to publish are swallowed so logging stays silent on errors,
    per Python's handler contract. A re-entrancy guard stops the handler
    from feeding itself if bus.publish ever logs.
    """

    def __init__(self, bus: EventBus, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__()
        self._bus = bus
        self._loop = loop
        self._in_emit = threading.local()

    def emit(self, record: logging.LogRecord) -> None:
        if getattr(self._in_emit, "flag", False):
            return
        self._in_emit.flag = True
        try:
            if record.name.startswith("groundstation.bus"):
                return  # avoid reflecting bus-internal logs back through the bus
            event = E.LogMessage(
                level=record.levelname,
                logger=record.name,
                message=record.getMessage(),
                ts=datetime.datetime.fromtimestamp(record.created),
            )
            if self._loop.is_closed():
                return
            self._loop.call_soon_threadsafe(self._schedule_publish, event)
        except Exception:
            pass
        finally:
            self._in_emit.flag = False

    def _schedule_publish(self, event: E.LogMessage) -> None:
        try:
            self._loop.create_task(self._bus.publish(event))
        except Exception:
            pass
