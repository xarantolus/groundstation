from __future__ import annotations

import asyncio
import collections
import logging
import os
import uuid
from typing import Deque, Dict, List, Optional, Set, Tuple

from . import events as E
from .bus import EventBus, run_subscriber
from .decode_gate import DecodeGate
from .models import GroundstationConfig, IQ_DATA_FILE_EXTENSION, Pass, PassStatus, TransferRequest
from .podman import DECODER_KILLED_BY_STOP, DEFAULT_DECODER_TIMEOUT_MINUTES, filter_decoder_outputs, run_decoder
from .state import StateStore
from .transfer import compress_file

logger = logging.getLogger("groundstation.decoder")
_decoder_output_logger = logging.getLogger("groundstation.decoders.output")

MAX_DECODE_ATTEMPTS = 3
DECODE_RETRY_TIMEOUT_FACTOR = 0.3


class DecoderService:
    """One decoder at a time, gated by :class:`DecodeGate`. Maintains a
    persistent queue of (pass_id, decoder_index) work items.

    Between runs (including between decoders of the same pass) the gate is
    re-checked — so a closer pass emerging partway through a decoder chain
    cleanly defers the remaining runs without interrupting the active one.
    """

    def __init__(
        self,
        cfg: GroundstationConfig,
        bus: EventBus,
        state: StateStore,
        gate: DecodeGate,
        passes: Dict[str, Pass],
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._state = state
        self._gate = gate
        self._passes = passes
        self._queue: Deque[Tuple[str, int, int]] = collections.deque()
        self._in_queue: Set[Tuple[str, int]] = set()
        self._wakeup = asyncio.Event()
        self._stop = asyncio.Event()
        self._cleanup_tasks: Set[asyncio.Task] = set()
        self._cleanup_launched: Set[str] = set()

        # Active decoder tracking — set in _run_one; the RecordingStarted
        # handler signals _active_kill_event to terminate the decoder mid-run
        # so the recorder doesn't fight CPU/IO with it.
        self._active_key: Optional[Tuple[str, int, int]] = None
        self._active_kill_event: Optional[asyncio.Event] = None

        for item in state.load_decode_queue():
            self._enqueue_persisted(item)
        if self._in_queue:
            logger.info(
                "decoder: restored %d pending decode(s) from state", len(self._in_queue)
            )

    def _enqueue_persisted(self, item: Tuple[str, int, int]) -> None:
        key = (item[0], item[1])
        if key in self._in_queue:
            return
        self._in_queue.add(key)
        self._queue.append(item)
        self._wakeup.set()

    def _enqueue(self, item: Tuple[str, int, int], front: bool = False) -> None:
        key = (item[0], item[1])
        if key in self._in_queue:
            return
        self._in_queue.add(key)
        if front:
            self._queue.appendleft(item)
        else:
            self._queue.append(item)
        self._wakeup.set()

    async def run(self) -> None:
        for pass_id, idx in list(self._in_queue):
            await self._bus.publish(
                E.DecodeQueued(pass_id=pass_id, decoder_index=idx)
            )

        sub = self._bus.subscribe(
            E.RecordingCompleted,
            E.RecordingStarted,
            name="decoder.events",
            queue_size=128,
        )
        sub_task = asyncio.create_task(
            run_subscriber(sub, self._on_event, "decoder.events")
        )
        try:
            while not self._stop.is_set():
                if not self._queue:
                    self._wakeup.clear()
                    if self._queue:
                        continue
                    wakeup_task = asyncio.create_task(self._wakeup.wait())
                    stop_task = asyncio.create_task(self._stop.wait())
                    done, pending = await asyncio.wait(
                        {wakeup_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                    for t in pending:
                        t.cancel()
                    if self._stop.is_set():
                        break
                    continue

                item = self._queue.popleft()
                self._in_queue.discard((item[0], item[1]))

                gate_task = asyncio.create_task(self._gate.wait_open())
                stop_task = asyncio.create_task(self._stop.wait())
                await asyncio.wait(
                    {gate_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if not gate_task.done():
                    gate_task.cancel()
                if not stop_task.done():
                    stop_task.cancel()
                if self._stop.is_set():
                    break

                try:
                    await self._run_one(*item)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("decoder run failed: %s", item)
        finally:
            sub.close()
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass
            for t in list(self._cleanup_tasks):
                t.cancel()
            for t in list(self._cleanup_tasks):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()

    async def _on_event(self, event: E.Event) -> None:
        if isinstance(event, E.RecordingCompleted):
            p = self._passes.get(event.pass_id)
            if not p:
                logger.warning("decoder got RecordingCompleted for unknown pass %s", event.pass_id)
                return
            pending = list(range(len(p.satellite.decoder)))
            p.decoders_pending = pending
            p.status = PassStatus.RECORDED
            self._state.save_pass(p)
            for idx in pending:
                self._state.decode_put(p.id, idx, attempt=0)
                self._enqueue((p.id, idx, 0))
                await self._bus.publish(E.DecodeQueued(pass_id=p.id, decoder_index=idx))
        elif isinstance(event, E.RecordingStarted):
            if self._active_kill_event is not None and not self._active_kill_event.is_set():
                logger.info(
                    "recording starting — killing active decoder %s", self._active_key
                )
                self._active_kill_event.set()

    async def _run_one(self, pass_id: str, decoder_index: int, attempt: int) -> None:
        p = self._passes.get(pass_id)
        if not p:
            logger.warning("decoder: pass %s unknown — dropping index %d", pass_id, decoder_index)
            self._state.decode_tombstone(pass_id, decoder_index)
            return
        if decoder_index >= len(p.satellite.decoder):
            logger.warning("decoder: index %d out of range for %s", decoder_index, pass_id)
            self._state.decode_tombstone(pass_id, decoder_index)
            return

        decoder = p.satellite.decoder[decoder_index]
        decoder_name = decoder.name or f"decoder_{decoder_index}"
        output_dir = p.pass_dir if not decoder.name else os.path.join(p.pass_dir, decoder_name)

        if not p.recording_path or not os.path.isfile(p.recording_path):
            error = f"recording file missing at {p.recording_path}"
            logger.error("%s: %s", pass_id, error)
            p.decoders_failed.append(decoder_index)
            self._state.save_pass(p)
            self._state.decode_tombstone(pass_id, decoder_index)
            await self._bus.publish(
                E.DecodeFailed(pass_id=pass_id, decoder_index=decoder_index, error=error)
            )
            return

        p.status = PassStatus.DECODING
        self._state.save_pass(p)

        loop = asyncio.get_running_loop()

        def on_log(line: str) -> None:
            stripped = line.rstrip("\r\n")
            if stripped:
                _decoder_output_logger.info(
                    "[%s %s] %s", pass_id, decoder_name, stripped
                )
            loop.create_task(
                self._bus.publish(
                    E.DecodeLog(pass_id=pass_id, decoder_index=decoder_index, line=line)
                )
            )

        base_timeout = (
            decoder.timeout_minutes
            if decoder.timeout_minutes is not None
            else DEFAULT_DECODER_TIMEOUT_MINUTES
        )
        effective_timeout = base_timeout
        if base_timeout is not None and attempt > 0:
            effective_timeout = base_timeout * (DECODE_RETRY_TIMEOUT_FACTOR ** attempt)

        await self._bus.publish(
            E.DecodeStarted(
                pass_id=pass_id, decoder_index=decoder_index, decoder_name=decoder_name
            )
        )

        before = _snapshot_dir(output_dir)
        is_shared_dir = output_dir == p.pass_dir

        kill_event = asyncio.Event()
        self._active_key = (pass_id, decoder_index, attempt)
        self._active_kill_event = kill_event

        rc: int = -1
        crash_error: Optional[str] = None
        try:
            rc = await run_decoder(
                p.satellite,
                decoder,
                p.pass_dir,
                output_dir,
                log_callback=on_log,
                timeout_minutes=effective_timeout,
                stop_event=kill_event,
            )
        except asyncio.CancelledError:
            self._active_key = None
            self._active_kill_event = None
            _cleanup_new(output_dir, before, protect_shared=is_shared_dir)
            raise
        except Exception as e:
            crash_error = str(e)
        finally:
            self._active_key = None
            self._active_kill_event = None

        if rc == DECODER_KILLED_BY_STOP:
            _cleanup_new(output_dir, before, protect_shared=is_shared_dir)
            logger.info(
                "decoder %s/%s killed by recorder — re-queued at front (attempt %d unchanged)",
                pass_id,
                decoder_name,
                attempt,
            )
            self._enqueue((pass_id, decoder_index, attempt), front=True)
            return

        if crash_error is not None or rc != 0:
            _cleanup_new(output_dir, before, protect_shared=is_shared_dir)
            error_text = crash_error or f"decoder exited {rc}"
            next_attempt = attempt + 1
            if next_attempt < MAX_DECODE_ATTEMPTS:
                logger.warning(
                    "decoder %s/%s failed (%s) — retry %d/%d",
                    pass_id,
                    decoder_name,
                    error_text,
                    next_attempt,
                    MAX_DECODE_ATTEMPTS - 1,
                )
                self._state.decode_put(pass_id, decoder_index, attempt=next_attempt)
                self._enqueue((pass_id, decoder_index, next_attempt))
                await self._bus.publish(
                    E.DecodeFailed(
                        pass_id=pass_id, decoder_index=decoder_index, error=error_text
                    )
                )
                return
            p.decoders_failed.append(decoder_index)
            self._state.save_pass(p)
            self._state.decode_tombstone(pass_id, decoder_index)
            await self._bus.publish(
                E.DecodeFailed(pass_id=pass_id, decoder_index=decoder_index, error=error_text)
            )
            if self._all_decoders_settled(p) and p.id not in self._cleanup_launched:
                self._cleanup_launched.add(p.id)
                task = asyncio.create_task(self._after_all_decoders(p))
                self._cleanup_tasks.add(task)
                task.add_done_callback(self._cleanup_tasks.discard)
            return

        surviving = filter_decoder_outputs(output_dir, decoder, pass_dir=p.pass_dir)
        if not surviving:
            p.decoders_failed.append(decoder_index)
            self._state.save_pass(p)
            self._state.decode_tombstone(pass_id, decoder_index)
            await self._bus.publish(
                E.DecodeFailed(
                    pass_id=pass_id,
                    decoder_index=decoder_index,
                    error="decoder output below min_files/min_size_bytes",
                )
            )
        else:
            # Persist the transfer requests FIRST — before marking the
            # decoder done and tombstoning its queue entry. A crash in the
            # middle then leaves the decoder queue intact (work will re-run,
            # producing the same outputs) rather than dropping the outputs
            # entirely. Re-running a decoder is wasteful but idempotent;
            # losing outputs is not recoverable.
            reqs = []
            for path in surviving:
                rel = os.path.relpath(path, p.pass_dir)
                req = TransferRequest(
                    id=str(uuid.uuid4()),
                    source_path=path,
                    destination_path=self._nas_path(p, rel),
                    keep_source=False,
                    compress=path.endswith(IQ_DATA_FILE_EXTENSION),
                    pass_id=pass_id,
                    label=f"{p.satellite.name} {decoder_name}/{os.path.basename(path)}",
                )
                self._state.transfer_put(req)
                reqs.append(req)

            p.decoders_done.append(decoder_index)
            self._state.save_pass(p)
            self._state.decode_tombstone(pass_id, decoder_index)

            for req in reqs:
                await self._bus.publish(E.TransferQueued(request=req))
            rels = [os.path.relpath(out, p.pass_dir) for out in surviving]
            await self._bus.publish(
                E.DecodeCompleted(
                    pass_id=pass_id,
                    decoder_index=decoder_index,
                    outputs=rels,
                )
            )

        if self._all_decoders_settled(p) and p.id not in self._cleanup_launched:
            # Cleanup (incl. waiting up to 30 minutes for the IQ upload) must
            # not block the decoder main loop — otherwise a slow NAS stalls
            # every subsequent pass. Spawn it as a background task.
            self._cleanup_launched.add(p.id)
            task = asyncio.create_task(self._after_all_decoders(p))
            self._cleanup_tasks.add(task)
            task.add_done_callback(self._cleanup_tasks.discard)

    def _all_decoders_settled(self, p: Pass) -> bool:
        total = len(p.satellite.decoder)
        done = len(set(p.decoders_done) | set(p.decoders_failed))
        return done >= total

    async def _after_all_decoders(self, p: Pass) -> None:
        """Called exactly once per pass, when every decoder has settled.
        The last reader of recording.bin is now gone, so we can free it.
        If the pass's IQ is to be uploaded, compress it locally first
        (minutes) and queue the .zst; then remove the .bin. Otherwise
        just remove the .bin."""
        p.status = PassStatus.DECODED if p.decoders_done else PassStatus.FAILED
        self._state.save_pass(p)

        iq = p.recording_path
        if iq and os.path.isfile(iq):
            if p.satellite.skip_iq_upload or not p.satellite.decoder:
                try:
                    os.remove(iq)
                    logger.info("removed local IQ %s (skip_iq_upload)", iq)
                except OSError:
                    logger.exception("could not remove IQ %s", iq)
            else:
                try:
                    await self._gate.wait_open()
                    compressed = await compress_file(iq)
                    try:
                        os.remove(iq)
                        logger.info(
                            "compressed IQ %s -> %s; .bin removed", iq, compressed
                        )
                    except OSError:
                        logger.exception("could not remove IQ after compression: %s", iq)
                    req = TransferRequest(
                        id=str(uuid.uuid4()),
                        source_path=compressed,
                        destination_path=self._nas_path(p, "recording.bin.zst"),
                        keep_source=False,
                        compress=False,
                        pass_id=p.id,
                        label=f"{p.satellite.name} IQ",
                    )
                    self._state.transfer_put(req)
                    await self._bus.publish(E.TransferQueued(request=req))
                except Exception:
                    logger.exception(
                        "IQ compression failed for %s — leaving %s for 24h cleanup",
                        p.id,
                        iq,
                    )

        p.status = PassStatus.DONE if p.decoders_done else PassStatus.FAILED
        self._state.save_pass(p)

    def _nas_path(self, p: Pass, *parts: str) -> str:
        prefix = os.path.join(
            self._cfg.nas_directory,
            p.pass_info.start_time.strftime("%Y"),
            p.pass_info.start_time.strftime("%Y-%m-%d"),
            f"{p.satellite.name}_{p.pass_info.start_time.strftime('%Y-%m-%d_%H-%M-%S')}",
        )
        return os.path.join(prefix, *parts)


def _snapshot_dir(path: str) -> Set[str]:
    snap: Set[str] = set()
    if not os.path.isdir(path):
        return snap
    for root, dirs, files in os.walk(path):
        for f in files:
            snap.add(os.path.join(root, f))
        for d in dirs:
            snap.add(os.path.join(root, d))
    return snap


def _cleanup_new(path: str, before: Set[str], protect_shared: bool) -> None:
    """Remove anything created under `path` that wasn't there at snapshot time.
    `protect_shared=True` is set when output_dir == pass_dir; the recording/info
    files at that level are then explicitly preserved even if they somehow
    weren't snapshotted."""
    if not os.path.isdir(path):
        return
    after = _snapshot_dir(path)
    new = after - before
    if protect_shared:
        protected = {os.path.join(path, n) for n in ("recording.bin", "recording.bin.zst", "info.json")}
        new -= protected
    for p in sorted(new, key=lambda x: -x.count(os.sep)):
        try:
            if os.path.isfile(p):
                os.remove(p)
            elif os.path.isdir(p):
                os.rmdir(p)
        except OSError:
            pass
