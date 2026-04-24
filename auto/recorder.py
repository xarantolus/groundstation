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
from .models import GroundstationConfig, Pass, PassStatus, TransferRequest
from .podman import run_recorder
from .state import StateStore

logger = logging.getLogger("groundstation.recorder")


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
        self._active: Optional[asyncio.Task] = None

    async def run(self) -> None:
        sub = self._bus.subscribe(E.PassStarted, name="recorder", queue_size=32)
        try:
            async for event in sub:
                if self._stop.is_set():
                    break
                try:
                    await self._handle_pass_started(event)  # type: ignore[arg-type]
                except Exception:
                    logger.exception("recorder failed handling PassStarted")
        finally:
            sub.close()
            if self._active and not self._active.done():
                self._active.cancel()
                try:
                    await self._active
                except asyncio.CancelledError:
                    pass

    def stop(self) -> None:
        self._stop.set()

    async def _handle_pass_started(self, event: E.PassStarted) -> None:
        p = event.pass_
        if self._active and not self._active.done():
            logger.warning(
                "recorder busy — skipping %s (overlap with previous pass)", p.id
            )
            return
        self._active = asyncio.create_task(self._record(p))
        try:
            await self._active
        finally:
            self._active = None

    async def _record(self, p: Pass) -> None:
        os.makedirs(p.pass_dir, exist_ok=True)
        self._write_info_json(p)

        p.status = PassStatus.RECORDING
        self._state.save_pass(p)
        await self._bus.publish(E.RecordingStarted(pass_id=p.id))

        loop = asyncio.get_running_loop()

        def on_log(line: str) -> None:
            loop.create_task(
                self._bus.publish(E.RecordingLog(pass_id=p.id, line=line))
            )

        now = datetime.datetime.now()
        record_minutes = (p.pass_info.end_time - now).total_seconds() / 60
        if record_minutes < 0.5:
            await self._fail(p, "pass already ended at start")
            return

        try:
            path = await run_recorder(
                p.satellite,
                record_minutes + 1,
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

        # IQ pre-upload: fire a TransferQueued event so the transfer service
        # starts shipping the raw IQ to the NAS immediately — in parallel with
        # decoding. keep_source=True means the decoder still reads from the
        # local file; compress=True turns it into .bin.zst on the way.
        # IQ upload happens post-decode: see DecoderService._after_all_decoders.
        # Decoders read recording.bin directly; once they're all done the
        # decoder service compresses it to .zst, deletes the .bin locally, and
        # queues the much-smaller .zst for the NAS. This keeps peak disk usage
        # low and frees the .bin as soon as its last reader finishes.

        # Upload the info.json so the NAS-side folder is self-describing even
        # before decoder outputs arrive.
        info_path = os.path.join(p.pass_dir, "info.json")
        if os.path.isfile(info_path):
            info_req = TransferRequest(
                id=str(uuid.uuid4()),
                source_path=info_path,
                destination_path=self._nas_path(p, "info.json"),
                keep_source=True,
                compress=False,
                pass_id=p.id,
                label=f"{p.satellite.name} info",
            )
            self._state.transfer_put(info_req)
            await self._bus.publish(E.TransferQueued(request=info_req))

    async def _fail(self, p: Pass, reason: str) -> None:
        p.status = PassStatus.FAILED
        self._state.save_pass(p)
        logger.error("recorder failed for %s: %s", p.id, reason)
        await self._bus.publish(E.RecordingFailed(pass_id=p.id, error=reason))

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
