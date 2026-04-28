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


# Start the recorder this many seconds before the official pass start time
# (when the satellite crosses the elevation threshold) so the podman container
# is up by the time the satellite is in view. Recorder.py records for the
# matching trail past end_time, see recorder.RECORDING_TRAIL_SECONDS.
RECORDING_LEAD_SECONDS = 30


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

        # Seed the gate from any recovered passes so the boot-time decoder
        # queue and transfer compression can't fire before the first _tick().
        if passes:
            self._update_gate_next_pass()

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
                tick_failed = False
                try:
                    await self._tick()
                except Exception:
                    tick_failed = True
                    logger.exception("scheduler tick failed")
                # Even on tick failure, mark the gate ready — leaving it
                # closed forever on a transient prediction error would
                # strand the decoder/compressor queues. The gate's other
                # checks (recording, upcoming pass) still apply.
                self._gate.mark_ready()
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
                self._ensure_monitor(existing)
                continue
            # A TLE refresh can shift start_time by seconds → new pid for
            # the same overpass. Dedup by time-window overlap instead of pid.
            overlap = self._find_overlapping_pass(sat, pi)
            if overlap is not None:
                if overlap.status == PassStatus.PREDICTED:
                    overlap.pass_info = pi
                    self._restart_monitor(overlap)
                current_ids.add(overlap.id)
                current_passes.append(overlap)
                self._ensure_monitor(overlap)
                continue
            p = Pass(
                id=pid,
                satellite=sat,
                pass_info=pi,
                pass_dir=_build_pass_dir(pid),
                status=PassStatus.PREDICTED,
            )
            self._known_passes[pid] = p
            current_passes.append(p)
            await self._bus.publish(E.PassPredicted(pass_=p))
            self._ensure_monitor(p)

        # Prune past/terminal passes. Future PREDICTED passes that don't
        # appear in the current prediction (e.g. a TLE fetch temporarily
        # failed) are KEPT so we don't drop scheduled work on a transient
        # network blip. Mid-flight passes (RECORDING, DECODING, …) are also
        # kept — other services are still working on them.
        for dead_id in list(self._known_passes.keys()):
            p = self._known_passes[dead_id]
            should_drop = False
            if p.status in (PassStatus.DONE, PassStatus.FAILED, PassStatus.RECORDED_PARTIAL):
                should_drop = True
            elif p.status == PassStatus.PREDICTED and p.pass_info.end_time <= now:
                # end_time already passed and we never recorded it — expire.
                should_drop = True
            if not should_drop:
                continue
            self._known_passes.pop(dead_id, None)
            task = self._monitors.pop(dead_id, None)
            if task and not task.done():
                task.cancel()
            # Always delete the on-disk JSON for dropped passes. Terminal
            # passes (DONE/FAILED/RECORDED_PARTIAL) were previously left on
            # disk, slowly accumulating one file per completed pass.
            # Transfers outlive the pass record (they have their own queue
            # keyed by req.id), so dropping the pass JSON is safe even if
            # the NAS upload hasn't finished.
            self._state.delete_pass(p.id)

        # Every still-future PREDICTED pass (including recovered ones whose
        # pid doesn't match the fresh prediction) must have a live monitor.
        for p in self._known_passes.values():
            if p.status == PassStatus.PREDICTED and p.pass_info.end_time > now:
                self._ensure_monitor(p)

        await self._bus.publish(E.PassesTableChanged(passes=current_passes))
        self._update_gate_next_pass()

    def _find_overlapping_pass(self, sat: Satellite, pi: PassInfo) -> Optional[Pass]:
        for p in self._known_passes.values():
            if p.satellite.name != sat.name:
                continue
            if p.status in (PassStatus.DONE, PassStatus.FAILED, PassStatus.RECORDED_PARTIAL):
                continue
            if pi.start_time < p.pass_info.end_time and p.pass_info.start_time < pi.end_time:
                return p
        return None

    def _ensure_monitor(self, p: Pass) -> None:
        """Spawn a monitor task for `p` if one isn't already running.
        Idempotent — safe to call from every tick."""
        existing_task = self._monitors.get(p.id)
        if existing_task is not None and not existing_task.done():
            return
        self._monitors[p.id] = asyncio.create_task(self._monitor_pass(p))

    def _restart_monitor(self, p: Pass) -> None:
        old = self._monitors.pop(p.id, None)
        if old is not None and not old.done():
            old.cancel()
        self._monitors[p.id] = asyncio.create_task(self._monitor_pass(p))

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

            if p.pass_info.recording_start_override is not None:
                record_at = p.pass_info.recording_start_override
            else:
                record_at = p.pass_info.start_time - datetime.timedelta(seconds=RECORDING_LEAD_SECONDS)
            now = datetime.datetime.now()
            delay = (record_at - now).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            if p.pass_info.end_time <= datetime.datetime.now():
                logger.warning("pass %s already ended by recorder-start time — skipping", p.id)
                return

            if self._known_passes.get(p.id) is not p:
                return
            sched_offset = (datetime.datetime.now() - record_at).total_seconds()
            logger.info(
                "publishing PassStarted for %s (sched offset %+.2fs)",
                p.id,
                sched_offset,
            )
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
        prev_id = self._next_pass_id
        if not upcoming:
            self._next_pass_id = None
            self._gate.on_next_pass(None, None)
            if prev_id is not None:
                logger.info("no upcoming passes")
            return
        head = upcoming[0]
        self._next_pass_id = head.id
        self._gate.on_next_pass(head.pass_info.start_time, head.pass_info.end_time)
        if head.id != prev_id:
            delta = head.pass_info.start_time - now
            mins = max(0, int(delta.total_seconds() // 60))
            logger.info(
                "next pass: %s at %s (in %dm, max el %.1f°)",
                head.satellite.name,
                head.pass_info.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                mins,
                head.pass_info.max_elevation,
            )
