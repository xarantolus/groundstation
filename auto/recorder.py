from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import time
import uuid
from typing import Dict, Optional

from . import events as E
from .bus import EventBus, run_subscriber
from .doppler import write_doppler_file, write_zero_doppler_file
from .models import GroundstationConfig, Pass, PassStatus, TransferRequest
from .podman import run_recorder
from .scheduler import RECORDING_LEAD_SECONDS
from .state import StateStore

logger = logging.getLogger("groundstation.recorder")

# Keep recording this many seconds past pass end_time. Combined with
# scheduler.RECORDING_LEAD_SECONDS, gives ~30s of margin on each side of the
# official pass window.
RECORDING_TRAIL_SECONDS = 30

# Run the doppler ephemeris computation this many seconds before the planned
# recorder launch (= pass_start - RECORDING_LEAD_SECONDS). Big enough that the
# write finishes well before podman launches, so _record never blocks waiting
# on it.
DOPPLER_PRECOMPUTE_LEAD_SECONDS = 180


class RecorderService:
    """One recording at a time. Subscribes to PassStarted; runs the podman
    recorder; emits recording lifecycle events; queues the IQ pre-upload
    so data lands on the NAS in parallel with decoding."""

    def __init__(
        self,
        cfg: GroundstationConfig,
        bus: EventBus,
        state: StateStore,
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._state = state
        self._stop = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()
        # Stop signal for the currently-running run_recorder (if any). A new
        # PassStarted sets this to make the prior recorder exit eagerly so the
        # SDR is free for the next pass.
        self._active_stop: Optional[asyncio.Event] = None
        # Set inside _record's finally as soon as run_recorder returns, i.e.
        # as soon as the SDR is free. The next pass's _chain_record awaits
        # this — *not* the full prev task — so save_pass + transfer_put for
        # the prior pass don't gate the next podman launch.
        self._active_sdr_released: Optional[asyncio.Event] = None
        # Per-pass background tasks that pre-write doppler.txt before the
        # recorder launches. _record awaits the matching task before podman
        # so the GR flowgraph never opens a missing file.
        self._doppler_tasks: Dict[str, asyncio.Task] = {}
        self._sub = None

    async def run(self) -> None:
        self._sub = self._bus.subscribe(
            E.PassStarted, E.PassPredicted, name="recorder", queue_size=64
        )
        try:
            async for event in self._sub:
                if self._stop.is_set():
                    break
                try:
                    if isinstance(event, E.PassStarted):
                        self._handle_pass_started(event)
                    elif isinstance(event, E.PassPredicted):
                        self._handle_pass_predicted(event)
                except Exception:
                    logger.exception("recorder failed handling %s", type(event).__name__)
        finally:
            self._sub.close()
            for t in list(self._tasks):
                if not t.done():
                    t.cancel()
            for t in list(self._tasks):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            for t in list(self._doppler_tasks.values()):
                if not t.done():
                    t.cancel()
            for t in list(self._doppler_tasks.values()):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            self._doppler_tasks.clear()

    def stop(self) -> None:
        self._stop.set()
        if self._sub is not None:
            self._sub.close()
        for t in list(self._tasks):
            if not t.done():
                t.cancel()
        for t in list(self._doppler_tasks.values()):
            if not t.done():
                t.cancel()

    def _handle_pass_started(self, event: E.PassStarted) -> None:
        p = event.pass_
        logger.info("recorder: PassStarted received for %s", p.id)
        prev_sdr = self._active_sdr_released
        if prev_sdr is not None and not prev_sdr.is_set():
            if self._active_stop is not None and not self._active_stop.is_set():
                logger.info(
                    "recorder: signalling prev recording to stop so %s can launch", p.id
                )
                self._active_stop.set()
        new_sdr = asyncio.Event()
        self._active_sdr_released = new_sdr
        task = asyncio.create_task(self._chain_record(p, prev_sdr, new_sdr))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _handle_pass_predicted(self, event: E.PassPredicted) -> None:
        p = event.pass_
        if p.id in self._doppler_tasks:
            return
        task = asyncio.create_task(self._precompute_doppler(p), name=f"doppler-{p.id}")
        self._doppler_tasks[p.id] = task
        task.add_done_callback(lambda _t, pid=p.id: self._doppler_tasks.pop(pid, None))

    async def _precompute_doppler(self, p: Pass) -> None:
        """Sleep until shortly before the recorder launches, then write
        doppler.txt off the event loop. _record awaits this so podman never
        races the file."""
        target = (
            p.pass_info.start_time
            - datetime.timedelta(seconds=RECORDING_LEAD_SECONDS + DOPPLER_PRECOMPUTE_LEAD_SECONDS)
        )
        delay = (target - datetime.datetime.now()).total_seconds()
        if delay > 0:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
        if datetime.datetime.now() >= p.pass_info.end_time:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._write_doppler_file, p)
        except Exception:
            logger.exception("precompute doppler for %s failed; will retry inline at record time", p.id)

    async def _chain_record(
        self,
        p: Pass,
        prev_sdr: Optional[asyncio.Event],
        my_sdr: asyncio.Event,
    ) -> None:
        # Wait only for the SDR to be free — *not* for the prev pass's
        # post-recording bookkeeping (save_pass, RecordingCompleted publish,
        # transfer_puts). Those run concurrently in the prev _record task.
        # _record's own late-start guard handles the case where waiting
        # pushed us past the new pass's window.
        if prev_sdr is not None and not prev_sdr.is_set():
            wait_t0 = time.monotonic()
            logger.info("recorder: %s awaiting prev SDR release", p.id)
            try:
                await prev_sdr.wait()
            except asyncio.CancelledError:
                raise
            logger.info(
                "recorder: %s SDR free after %.2fs", p.id, time.monotonic() - wait_t0
            )
        try:
            await self._record(p, my_sdr)
        finally:
            # Belt-and-braces: if _record bailed before reaching its own
            # finally (e.g. cancellation in setup), still release the SDR
            # so a subsequent pass isn't blocked forever.
            if not my_sdr.is_set():
                my_sdr.set()

    async def _record(self, p: Pass, sdr_released: asyncio.Event) -> None:
        record_t0 = time.monotonic()
        logger.info("recorder: _record entered for %s", p.id)
        os.makedirs(p.pass_dir, exist_ok=True)
        self._write_info_json(p)

        # The precompute task should already be done by now (it fires
        # ~3 minutes before record_at). Await it anyway so podman never
        # races the doppler file write — a missing/partial doppler.txt
        # crashes the GR flowgraph immediately.
        precompute = self._doppler_tasks.pop(p.id, None)
        if precompute is not None and not precompute.done():
            logger.info("waiting for doppler precompute to finish for %s", p.id)
            try:
                await precompute
            except (asyncio.CancelledError, Exception):
                pass

        if not self._doppler_file_complete(p):
            logger.info("doppler precompute missing for %s — writing inline", p.id)
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(None, self._write_doppler_file, p)
            except Exception as e:
                logger.exception("could not write doppler file for %s", p.id)
                sdr_released.set()
                await self._fail(p, f"doppler file generation failed: {e}")
                return

        p.status = PassStatus.RECORDING
        save_t0 = time.monotonic()
        await self._state.save_pass_async(p)
        publish_t0 = time.monotonic()
        await self._bus.publish(E.RecordingStarted(pass_id=p.id))
        logger.info(
            "recorder: setup for %s took %.2fs (save_pass %.2fs, publish %.2fs)",
            p.id,
            time.monotonic() - record_t0,
            publish_t0 - save_t0,
            time.monotonic() - publish_t0,
        )

        stop_event = asyncio.Event()
        self._active_stop = stop_event

        loop = asyncio.get_running_loop()

        def on_log(line: str) -> None:
            stripped = line.rstrip("\n")
            if "doppler_correction" in stripped and "set time" in stripped:
                return
            if stripped:
                logger.info("Recorder: %s", stripped)
            loop.create_task(
                self._bus.publish(E.RecordingLog(pass_id=p.id, line=line))
            )

        now = datetime.datetime.now()
        if p.pass_info.recording_end_override is not None:
            record_until = p.pass_info.recording_end_override
        else:
            record_until = p.pass_info.end_time + datetime.timedelta(seconds=RECORDING_TRAIL_SECONDS)
        record_minutes = (record_until - now).total_seconds() / 60
        if record_minutes < 0.5:
            sdr_released.set()
            await self._fail(p, "pass already ended at start")
            return

        path: Optional[str] = None
        try:
            try:
                path = await run_recorder(
                    p.satellite,
                    record_minutes,
                    p.pass_dir,
                    log_callback=on_log,
                    stop_event=stop_event,
                )
            except FileNotFoundError as e:
                await self._fail(p, f"recorder produced no output: {e}")
                return
            except asyncio.CancelledError:
                await self._fail(p, "recording cancelled")
                raise
            except Exception as e:
                await self._fail(p, f"recorder crashed: {e}")
                return
        finally:
            # Release the SDR the moment run_recorder unwinds, before the
            # post-recording bookkeeping below — that's the whole point of
            # the per-pass sdr_released event.
            sdr_released.set()
            if self._active_stop is stop_event:
                self._active_stop = None

        if path is None:
            return

        p.recording_path = path
        p.status = PassStatus.RECORDED
        await self._state.save_pass_async(p)
        await self._bus.publish(E.RecordingCompleted(pass_id=p.id, path=path))

        # IQ upload happens post-decode in DecoderService._after_all_decoders.
        uploads = [("info.json", "info")]
        if p.satellite.doppler_correction:
            uploads.append(("doppler.txt", "doppler"))
        else:
            uploads.append(("doppler_reference.txt", "doppler reference"))
        for name, label in uploads:
            path = os.path.join(p.pass_dir, name)
            if not os.path.isfile(path):
                continue
            req = TransferRequest(
                id=str(uuid.uuid4()),
                source_path=path,
                destination_path=self._nas_path(p, name),
                keep_source=True,
                compress=False,
                pass_id=p.id,
                label=f"{p.satellite.name} {label}",
            )
            await self._state.transfer_put_async(req)
            await self._bus.publish(E.TransferQueued(request=req))

    async def _fail(self, p: Pass, reason: str) -> None:
        p.status = PassStatus.FAILED
        await self._state.save_pass_async(p)
        logger.error("recorder failed for %s: %s", p.id, reason)
        await self._bus.publish(E.RecordingFailed(pass_id=p.id, error=reason))

    def _doppler_file_complete(self, p: Pass) -> bool:
        path = os.path.join(p.pass_dir, "doppler.txt")
        try:
            return os.path.getsize(path) > 0
        except OSError:
            return False

    def _write_doppler_file(self, p: Pass) -> None:
        # Doppler timestamps are seconds-relative to `anchor`, which we fix at
        # the planned recorder launch time so the same value is used whether
        # this runs at precompute time (T-3min) or as the inline fallback at
        # PassStarted. The GR flowgraph anchors rx_time at flowgraph __init__
        # which lands within ~10s of this anchor, well within doppler tolerance.
        path = os.path.join(p.pass_dir, "doppler.txt")
        buffer_s = 60
        anchor = p.pass_info.start_time - datetime.timedelta(seconds=RECORDING_LEAD_SECONDS)
        start = p.pass_info.start_time - datetime.timedelta(
            seconds=RECORDING_LEAD_SECONDS + buffer_s
        )
        end = p.pass_info.end_time + datetime.timedelta(
            seconds=RECORDING_TRAIL_SECONDS + buffer_s
        )
        if not p.satellite.doppler_correction:
            write_zero_doppler_file(
                output_path=path, anchor=anchor, start=start, end=end
            )
            # Also keep the predicted doppler track for reference / offline
            # post-processing, under a different filename so the flowgraph
            # still picks up the zero stub.
            write_doppler_file(
                tle1=p.pass_info.tle1,
                tle2=p.pass_info.tle2,
                sat_name=p.satellite.name,
                lat=self._cfg.location_lat,
                lon=self._cfg.location_lon,
                alt_m=self._cfg.location_alt,
                f_carrier=p.satellite.frequency,
                anchor=anchor,
                start=start,
                end=end,
                output_path=os.path.join(p.pass_dir, "doppler_reference.txt"),
            )
            return
        write_doppler_file(
            tle1=p.pass_info.tle1,
            tle2=p.pass_info.tle2,
            sat_name=p.satellite.name,
            lat=self._cfg.location_lat,
            lon=self._cfg.location_lon,
            alt_m=self._cfg.location_alt,
            f_carrier=p.satellite.frequency,
            anchor=anchor,
            start=start,
            end=end,
            output_path=path,
        )

    def _write_info_json(self, p: Pass, extra: Optional[dict] = None) -> None:
        data = {
            "satellite": p.satellite.model_dump(mode="json"),
            "pass": p.pass_info.model_dump(mode="json"),
            "pass_id": p.id,
        }
        if extra:
            data.update(extra)
        path = os.path.join(p.pass_dir, "info.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
        except OSError:
            logger.exception("could not write info.json for %s", p.id)

    def _nas_path(self, p: Pass, *parts: str) -> str:
        prefix = os.path.join(
            self._cfg.nas_directory,
            p.pass_info.start_time.strftime("%Y"),
            p.pass_info.start_time.strftime("%Y-%m-%d"),
            f"{p.satellite.name}_{p.pass_info.start_time.strftime('%Y-%m-%d_%H-%M-%S')}",
        )
        return os.path.join(prefix, *parts)
