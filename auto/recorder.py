from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import uuid
from typing import Optional

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
        # Most recently scheduled recording task. Each new task chains itself
        # behind the previous so back-to-back partial-window passes serialise
        # cleanly without dropping events on the floor.
        self._active: Optional[asyncio.Task] = None
        self._tasks: set[asyncio.Task] = set()
        self._sub = None

    async def run(self) -> None:
        self._sub = self._bus.subscribe(E.PassStarted, name="recorder", queue_size=32)
        try:
            async for event in self._sub:
                if self._stop.is_set():
                    break
                try:
                    self._handle_pass_started(event)  # type: ignore[arg-type]
                except Exception:
                    logger.exception("recorder failed handling PassStarted")
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

    def stop(self) -> None:
        self._stop.set()
        if self._sub is not None:
            self._sub.close()
        for t in list(self._tasks):
            if not t.done():
                t.cancel()

    def _handle_pass_started(self, event: E.PassStarted) -> None:
        p = event.pass_
        prev = self._active
        task = asyncio.create_task(self._chain_record(p, prev))
        self._active = task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _chain_record(self, p: Pass, prev: Optional[asyncio.Task]) -> None:
        # Wait for the previous recording to finish so SDR/podman teardown
        # completes before we retune. _record's own late-start guard handles
        # the case where waiting pushed us past the new pass's window.
        if prev is not None and not prev.done():
            try:
                await prev
            except (asyncio.CancelledError, Exception):
                pass
        await self._record(p)

    async def _record(self, p: Pass) -> None:
        os.makedirs(p.pass_dir, exist_ok=True)
        self._write_info_json(p)
        try:
            self._write_doppler_file(p)
        except Exception as e:
            logger.exception("could not write doppler file for %s", p.id)
            await self._fail(p, f"doppler file generation failed: {e}")
            return

        p.status = PassStatus.RECORDING
        self._state.save_pass(p)
        await self._bus.publish(E.RecordingStarted(pass_id=p.id))

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
            await self._fail(p, "pass already ended at start")
            return

        try:
            path = await run_recorder(
                p.satellite,
                record_minutes,
                p.pass_dir,
                log_callback=on_log,
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

        p.recording_path = path
        p.status = PassStatus.RECORDED
        self._state.save_pass(p)
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
            self._state.transfer_put(req)
            await self._bus.publish(E.TransferQueued(request=req))

    async def _fail(self, p: Pass, reason: str) -> None:
        p.status = PassStatus.FAILED
        self._state.save_pass(p)
        logger.error("recorder failed for %s: %s", p.id, reason)
        await self._bus.publish(E.RecordingFailed(pass_id=p.id, error=reason))

    def _write_doppler_file(self, p: Pass) -> None:
        # Doppler timestamps are seconds-relative to `anchor` (≈ now), so the
        # same file works during a later replay of recording.bin: the doppler
        # block's sample-count timeline starts at 0 in both cases. Skew between
        # this `now` and when the recorder actually opens the SDR is ~1-2s
        # which is irrelevant given how smooth LEO doppler curves are.
        path = os.path.join(p.pass_dir, "doppler.txt")
        buffer_s = 60
        anchor = datetime.datetime.now()
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
