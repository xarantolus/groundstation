from __future__ import annotations

import asyncio
import datetime
import logging
import tempfile
from typing import Dict, List, Optional, Tuple

from . import events as E
from .bus import EventBus, Subscription, run_subscriber
from .decode_gate import DecodeGate
from .models import GroundstationConfig, Pass, PassInfo, PassStatus, Satellite
from .pass_predictor import PassPredictor
from .state import StateStore

logger = logging.getLogger("groundstation.scheduler")


def _build_pass_dir(pass_id: str) -> str:
    base = tempfile.gettempdir()
    return f"{base}/recorder-{pass_id}"


class SchedulerService:
    """Predicts upcoming passes and emits pass lifecycle events.

    Also owns the DecodeGate and keeps it informed of:
      - whether the recorder is currently running (via RecordingStarted/Completed/Failed)
      - the soonest upcoming pass (so the gate can enforce the safety margin)
    """

    def __init__(
        self,
        cfg: GroundstationConfig,
        bus: EventBus,
        state: StateStore,
        predictor: PassPredictor,
        gate: DecodeGate,
        passes: Dict[str, Pass],
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._state = state
        self._predictor = predictor
        self._gate = gate

        # Shared Pass registry — scheduler adds entries; recorder & decoder
        # read them (and mutate the same objects, which is safe because event
        # delivery is in-process without serialization).
        self._known_passes: Dict[str, Pass] = passes
        self._monitors: Dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._next_pass_id: Optional[str] = None

    async def run(self) -> None:
        sub = self._bus.subscribe(
            E.RecordingStarted,
            E.RecordingCompleted,
            E.RecordingFailed,
            E.PassEnded,
            name="scheduler.recording",
            queue_size=64,
        )
        sub_task = asyncio.create_task(
            run_subscriber(sub, self._on_event, "scheduler.recording")
        )
        try:
            while not self._stop.is_set():
                try:
                    await self._tick()
                except Exception:
                    logger.exception("scheduler tick failed")
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self._cfg.update_interval_hours * 3600,
                    )
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            sub.close()
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass
            for t in list(self._monitors.values()):
                t.cancel()
            self._monitors.clear()

    def stop(self) -> None:
        self._stop.set()

    async def _tick(self) -> None:
        logger.info("updating pass predictions")
        loop = asyncio.get_running_loop()
        horizon_hours = max(
            self._cfg.update_interval_hours * 1.25, self._cfg.update_interval_hours + 2
        )
        predicted: List[Tuple[Satellite, PassInfo]] = await loop.run_in_executor(
            None,
            self._predictor.predict_all,
            self._cfg.satellites,
            self._cfg.location_lat,
            self._cfg.location_lon,
            self._cfg.location_alt,
            self._cfg.pass_elevation_threshold_deg,
            self._cfg.pass_start_elevation_threshold_deg,
            horizon_hours,
        )

        now = datetime.datetime.now()
        window_start = now - datetime.timedelta(minutes=90)
        predicted = [(s, p) for s, p in predicted if p.end_time > window_start]
        predicted.sort(key=lambda t: t[1].start_time)

        current_ids = set()
        current_passes: List[Pass] = []
        for sat, pi in predicted:
            pid = Pass.make_id(sat, pi)
            current_ids.add(pid)
            existing = self._known_passes.get(pid)
            if existing:
                current_passes.append(existing)
                continue
            p = Pass(
                id=pid,
                satellite=sat,
                pass_info=pi,
                pass_dir=_build_pass_dir(pid),
                status=PassStatus.PREDICTED,
            )
            self._known_passes[pid] = p
            self._state.save_pass(p)
            current_passes.append(p)
            await self._bus.publish(E.PassPredicted(pass_=p))
            self._monitors[pid] = asyncio.create_task(self._monitor_pass(p))

        # Prune passes that dropped out of the prediction horizon. Only
        # remove PREDICTED (nothing has touched them yet) or terminal
        # (DONE/FAILED) passes. Anything mid-flight is being worked on by
        # another service that still needs the Pass object.
        terminal = {
            PassStatus.DONE,
            PassStatus.FAILED,
            PassStatus.PREDICTED,
            PassStatus.RECORDED_PARTIAL,
        }
        for dead_id in list(self._known_passes.keys()):
            if dead_id in current_ids:
                continue
            p = self._known_passes[dead_id]
            if p.status in terminal:
                self._known_passes.pop(dead_id, None)
                task = self._monitors.pop(dead_id, None)
                if task and not task.done():
                    task.cancel()
                if p.status == PassStatus.PREDICTED:
                    self._state.delete_pass(p.id)

        await self._bus.publish(E.PassesTableChanged(passes=current_passes))
        self._update_gate_next_pass()

    async def _monitor_pass(self, p: Pass) -> None:
        try:
            now = datetime.datetime.now()
            upcoming_at = p.pass_info.start_time - datetime.timedelta(seconds=60)
            delay = (upcoming_at - now).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
                await self._bus.publish(
                    E.PassUpcoming(
                        pass_=p,
                        seconds_until_start=(p.pass_info.start_time - datetime.datetime.now()).total_seconds(),
                    )
                )

            now = datetime.datetime.now()
            delay = (p.pass_info.start_time - now).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            if p.pass_info.end_time <= datetime.datetime.now():
                logger.warning("pass %s already ended by start_time — skipping", p.id)
                return

            if self._known_passes.get(p.id) is not p:
                return
            await self._bus.publish(E.PassStarted(pass_=p))

            delay = (p.pass_info.end_time - datetime.datetime.now()).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            await self._bus.publish(E.PassEnded(pass_=p))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("monitor_pass(%s) failed", p.id)

    async def _on_event(self, event: E.Event) -> None:
        if isinstance(event, E.RecordingStarted):
            self._gate.on_recording(True)
        elif isinstance(event, (E.RecordingCompleted, E.RecordingFailed)):
            self._gate.on_recording(False)
        elif isinstance(event, E.PassEnded):
            self._update_gate_next_pass()

    def _update_gate_next_pass(self) -> None:
        now = datetime.datetime.now()
        upcoming = [
            p for p in self._known_passes.values() if p.pass_info.start_time > now
        ]
        upcoming.sort(key=lambda p: p.pass_info.start_time)
        if not upcoming:
            self._next_pass_id = None
            self._gate.on_next_pass(None, None)
            return
        head = upcoming[0]
        self._next_pass_id = head.id
        self._gate.on_next_pass(head.pass_info.start_time, head.pass_info.end_time)
