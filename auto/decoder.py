from __future__ import annotations

import asyncio
import logging
import os
import uuid
from typing import Dict, List, Set, Tuple

from . import events as E
from .bus import EventBus, run_subscriber
from .decode_gate import DecodeGate
from .models import GroundstationConfig, IQ_DATA_FILE_EXTENSION, Pass, PassStatus, TransferRequest
from .podman import filter_decoder_outputs, run_decoder
from .state import StateStore
from .transfer import compress_file

logger = logging.getLogger("groundstation.decoder")


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
        self._queue: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
        self._in_queue: Set[Tuple[str, int]] = set()
        self._stop = asyncio.Event()
        self._cleanup_tasks: Set[asyncio.Task] = set()
        self._cleanup_launched: Set[str] = set()

        for key in state.load_decode_queue():
            self._enqueue_persisted(key)
        if self._in_queue:
            logger.info(
                "decoder: restored %d pending decode(s) from state", len(self._in_queue)
            )

    def _enqueue_persisted(self, key: Tuple[str, int]) -> None:
        if key in self._in_queue:
            return
        self._in_queue.add(key)
        self._queue.put_nowait(key)

    async def run(self) -> None:
        # Announce any items we restored from state at __init__ time so the
        # UI's pending-count is correct. This must happen after subscribers
        # (view, web) are running — main.py starts them before services.
        for pass_id, idx in list(self._in_queue):
            await self._bus.publish(
                E.DecodeQueued(pass_id=pass_id, decoder_index=idx)
            )

        sub = self._bus.subscribe(
            E.RecordingCompleted,
            name="decoder.events",
            queue_size=128,
        )
        sub_task = asyncio.create_task(
            run_subscriber(sub, self._on_event, "decoder.events")
        )
        try:
            while not self._stop.is_set():
                try:
                    key = await self._queue.get()
                except asyncio.CancelledError:
                    raise
                self._in_queue.discard(key)
                if self._stop.is_set() or key[0] == "__stop__":
                    break
                # Wait for the gate, but wake up on shutdown so we don't
                # block indefinitely when the gate happens to be closed.
                gate_task = asyncio.create_task(self._gate.wait_open())
                stop_task = asyncio.create_task(self._stop.wait())
                done, pending = await asyncio.wait(
                    {gate_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()
                if self._stop.is_set():
                    break
                try:
                    await self._run_one(*key)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("decoder run failed: %s", key)
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
        # Push a sentinel so the queue wait unblocks. We don't actually use
        # the value — the stop check above handles it.
        try:
            self._queue.put_nowait(("__stop__", -1))
        except asyncio.QueueFull:
            pass

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
                key = (p.id, idx)
                self._state.decode_put(p.id, idx)
                if key not in self._in_queue:
                    self._in_queue.add(key)
                    await self._queue.put(key)
                await self._bus.publish(E.DecodeQueued(pass_id=p.id, decoder_index=idx))

    async def _run_one(self, pass_id: str, decoder_index: int) -> None:
        if pass_id == "__stop__":
            return
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
        # Unnamed decoders write directly into pass_dir; named ones get their
        # own subdirectory. filter_decoder_outputs ignores the pass-level
        # recording/info files and sibling decoder subdirs.
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
            loop.create_task(
                self._bus.publish(
                    E.DecodeLog(pass_id=pass_id, decoder_index=decoder_index, line=line)
                )
            )

        await self._bus.publish(
            E.DecodeStarted(
                pass_id=pass_id, decoder_index=decoder_index, decoder_name=decoder_name
            )
        )

        try:
            rc = await run_decoder(
                p.satellite,
                decoder,
                p.pass_dir,
                output_dir,
                log_callback=on_log,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            p.decoders_failed.append(decoder_index)
            self._state.save_pass(p)
            self._state.decode_tombstone(pass_id, decoder_index)
            await self._bus.publish(
                E.DecodeFailed(pass_id=pass_id, decoder_index=decoder_index, error=str(e))
            )
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
                    error=(
                        f"decoder exited {rc} and produced no usable output"
                        if rc != 0
                        else "decoder output below min_files/min_size_bytes"
                    ),
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
