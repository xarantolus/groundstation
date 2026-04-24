from __future__ import annotations

import asyncio
import datetime
import logging
from typing import Callable, Optional

logger = logging.getLogger("groundstation.gate")


class DecodeGate:
    """Permits decoding only when the groundstation is idle AND the next
    pass is further than ``safety_minutes`` away. Event-driven: a single
    timer task re-evaluates at the next boundary (safe_until or end_time).
    Consumers park on :meth:`wait_open` — no polling sleeps.

    Gate may fail-open on internal errors so a buggy callback cannot strand
    the decoder queue indefinitely. The worst case is a decoder overlapping
    with a pass, which is recoverable; a permanently-closed gate is not.
    """

    def __init__(
        self,
        safety_minutes: float = 3.0,
        clock: Callable[[], datetime.datetime] = datetime.datetime.now,
        on_change: Optional[Callable[[bool, str], None]] = None,
    ) -> None:
        self._open = asyncio.Event()
        self._safety = datetime.timedelta(minutes=safety_minutes)
        self._clock = clock
        self._on_change = on_change

        self._recording = False
        self._next_start: Optional[datetime.datetime] = None
        self._next_end: Optional[datetime.datetime] = None

        self._timer: Optional[asyncio.Task] = None
        self._last_open: Optional[bool] = None
        self._last_reason: str = ""

        self._reevaluate()

    async def wait_open(self) -> None:
        await self._open.wait()

    def is_open(self) -> bool:
        return self._open.is_set()

    @property
    def last_reason(self) -> str:
        return self._last_reason

    def on_recording(self, recording: bool) -> None:
        self._recording = recording
        self._reevaluate()

    def on_next_pass(
        self,
        start_time: Optional[datetime.datetime],
        end_time: Optional[datetime.datetime],
    ) -> None:
        self._next_start = start_time
        self._next_end = end_time
        self._reevaluate()

    def close(self) -> None:
        self._cancel_timer()

    def _cancel_timer(self) -> None:
        if self._timer and not self._timer.done():
            self._timer.cancel()
        self._timer = None

    def _set(self, open_: bool, reason: str) -> None:
        changed = open_ != self._last_open or reason != self._last_reason
        self._last_open = open_
        self._last_reason = reason
        if open_:
            self._open.set()
        else:
            self._open.clear()
        if changed and self._on_change:
            try:
                self._on_change(open_, reason)
            except Exception:
                logger.exception("gate on_change callback failed")

    def _reevaluate(self) -> None:
        try:
            self._do_reevaluate()
        except Exception:
            logger.exception("gate _reevaluate failed — failing open to avoid stranding decoder")
            self._cancel_timer()
            self._set(True, "gate error — fail open")

    def _do_reevaluate(self) -> None:
        self._cancel_timer()
        if self._recording:
            self._set(False, "recorder running")
            return
        if self._next_start is None:
            self._set(True, "no upcoming pass")
            return

        now = self._clock()
        safe_until = self._next_start - self._safety

        if now >= safe_until:
            self._set(False, f"within {int(self._safety.total_seconds() / 60)}min of next pass")
            wake_at = self._next_end if self._next_end else (self._next_start + datetime.timedelta(minutes=30))
            self._timer = asyncio.create_task(self._wake_at(wake_at))
        else:
            self._set(True, "safe window")
            self._timer = asyncio.create_task(self._wake_at(safe_until))

    async def _wake_at(self, when: datetime.datetime) -> None:
        try:
            delta = (when - self._clock()).total_seconds()
            if delta > 0:
                await asyncio.sleep(delta)
            # Schedule reevaluate via call_soon so the current task completes
            # cleanly before _cancel_timer would otherwise try to cancel it.
            asyncio.get_running_loop().call_soon(self._reevaluate)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("gate wake task failed")
            self._set(True, "gate error — fail open")
