from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
from typing import AsyncIterator, Callable, Optional

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
        safety_minutes: float = 1.0,
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
        self._predictor_ready = False

        self._timer: Optional[asyncio.Task] = None
        self._last_open: Optional[bool] = None
        self._last_reason: str = ""

        self._cpu_lock = asyncio.Lock()
        self._closed = False
        self._active_kill: Optional[asyncio.Event] = None

        self._reevaluate()

    async def wait_open(self) -> None:
        await self._open.wait()

    @contextlib.asynccontextmanager
    async def cpu_slot(
        self, min_safety_minutes: Optional[float] = None
    ) -> AsyncIterator[asyncio.Event]:
        """Wait for the gate to open, then take the single CPU slot —
        decoders/compressors all serialise on this so they don't fight for
        the Pi's cores. Re-checks the gate after acquiring the lock in case
        a recording or upcoming pass arrived while we were queueing.

        ``min_safety_minutes`` overrides the gate's default safety for this
        acquirer: e.g. compression (~10 min) passes 10.0 to avoid being
        killed mid-run by a pass the gate would still consider "safe enough"
        for a quick decoder. If the next pass is sooner than that, the slot
        is released and we wait for the pass to end before retrying.

        Yields a kill ``asyncio.Event`` that gets set when the recorder
        starts mid-run — callers should pass it through to their subprocess
        so that compression / decoding can be terminated and re-queued."""
        acquired = False
        kill = asyncio.Event()
        extra_safety = (
            datetime.timedelta(minutes=min_safety_minutes)
            if min_safety_minutes is not None
            else None
        )
        try:
            while True:
                await self._open.wait()
                await self._cpu_lock.acquire()
                acquired = True
                if self._closed:
                    break
                if not self._open.is_set():
                    self._cpu_lock.release()
                    acquired = False
                    continue
                if extra_safety is not None and self._next_start is not None:
                    now = self._clock()
                    if self._next_start - now < extra_safety:
                        self._cpu_lock.release()
                        acquired = False
                        wait_until = self._next_end or (
                            self._next_start + datetime.timedelta(minutes=30)
                        )
                        delta = (wait_until - now).total_seconds()
                        if delta > 0:
                            await asyncio.sleep(delta)
                        continue
                break
            self._active_kill = kill
            yield kill
        finally:
            self._active_kill = None
            if acquired:
                self._cpu_lock.release()

    def is_open(self) -> bool:
        return self._open.is_set()

    @property
    def last_reason(self) -> str:
        return self._last_reason

    def on_recording(self, recording: bool) -> None:
        self._recording = recording
        if recording and self._active_kill is not None:
            self._active_kill.set()
        self._reevaluate()

    def on_next_pass(
        self,
        start_time: Optional[datetime.datetime],
        end_time: Optional[datetime.datetime],
    ) -> None:
        self._next_start = start_time
        self._next_end = end_time
        self._reevaluate()

    def mark_ready(self) -> None:
        """Called after the scheduler has finished its first prediction tick.
        Until then the gate stays closed so decoders/compressors don't sneak
        past based on stale or missing pass info at boot."""
        if self._predictor_ready:
            return
        self._predictor_ready = True
        self._reevaluate()

    def close(self) -> None:
        self._cancel_timer()
        self._closed = True
        # Force-open so anyone awaiting wait_open() unblocks during shutdown.
        self._open.set()

    def _cancel_timer(self) -> None:
        if self._timer and not self._timer.done():
            self._timer.cancel()
        self._timer = None

    def _set(self, open_: bool, reason: str) -> None:
        changed = open_ != self._last_open or reason != self._last_reason
        was_open = self._last_open
        self._last_open = open_
        self._last_reason = reason
        if open_:
            self._open.set()
        else:
            self._open.clear()
            # Closing the gate kills active CPU work so the recorder has
            # the cores by the safety boundary, not just at AOS-30s.
            if was_open is True and self._active_kill is not None:
                self._active_kill.set()
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
        if not self._predictor_ready:
            self._set(False, "awaiting first pass prediction")
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
