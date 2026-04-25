from __future__ import annotations

import asyncio
import collections
import errno
import logging
import os
import time
import uuid
from typing import Callable, Deque, Dict, List, Optional

from . import events as E
from .bus import EventBus
from .decode_gate import DecodeGate
from .models import GroundstationConfig, IQ_DATA_FILE_EXTENSION, TransferRequest
from .state import StateStore

logger = logging.getLogger("groundstation.transfer")

MAX_ATTEMPTS = 3

# Disk-full retries are handled separately from MAX_ATTEMPTS: the condition
# clears on its own once another upload completes and frees space, so we wait
# longer and don't burn through the regular attempt budget.
DISK_FULL_BACKOFF_INITIAL = 60
DISK_FULL_BACKOFF_MAX = 600
MAX_DISK_RETRIES = 60


def _is_disk_full(error: BaseException) -> bool:
    if isinstance(error, OSError) and error.errno == errno.ENOSPC:
        return True
    msg = str(error).lower()
    return "no space left on device" in msg or "enospc" in msg


class TransferService:
    """Event-driven transfer worker pool.

    Subscribes to TransferQueued, emits TransferStarted/Progress/Completed/Failed.
    Persists the queue as a JSONL log (via StateStore). Uploads resume in-place
    against partial NAS-side files — see :func:`copy_with_progress`.
    """

    def __init__(
        self,
        cfg: GroundstationConfig,
        bus: EventBus,
        state: StateStore,
        gate: Optional[DecodeGate] = None,
        num_workers: int = 4,
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._state = state
        self._gate = gate
        self._num_workers = num_workers
        self._queue: asyncio.Queue[TransferRequest] = asyncio.Queue()
        self._stop = asyncio.Event()
        self._workers: List[asyncio.Task] = []
        self._sub_task: Optional[asyncio.Task] = None

        self._active: Dict[str, E.TransferProgress] = {}
        self._completed_tail: Deque[E.TransferCompleted] = collections.deque(maxlen=50)
        self._active_lock = asyncio.Lock()

    @property
    def active(self) -> Dict[str, E.TransferProgress]:
        return self._active

    async def run(self) -> None:
        sub = self._bus.subscribe(E.TransferQueued, name="transfer.ingest", queue_size=256)

        async def ingest() -> None:
            async for event in sub:
                if isinstance(event, E.TransferQueued):
                    await self._queue.put(event.request)

        self._sub_task = asyncio.create_task(ingest())

        reloaded = 0
        for req in self._state.load_transfer_queue():
            await self._bus.publish(E.TransferQueued(request=req))
            reloaded += 1
        if reloaded:
            logger.info("transfer: queued %d recovered item(s)", reloaded)
        self._workers = [
            asyncio.create_task(self._worker(i)) for i in range(self._num_workers)
        ]
        try:
            await self._stop.wait()
        finally:
            for w in self._workers:
                w.cancel()
            if self._sub_task:
                self._sub_task.cancel()
            for t in [*self._workers, self._sub_task]:
                if t is None:
                    continue
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            sub.close()

    def stop(self) -> None:
        self._stop.set()

    async def _worker(self, worker_id: int) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                req = await self._queue.get()
            except asyncio.CancelledError:
                raise
            try:
                await self._process(req, loop)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("worker %d: unhandled error", worker_id)

    async def _process(self, req: TransferRequest, loop: asyncio.AbstractEventLoop) -> None:
        original_source = req.source_path
        if not os.path.exists(original_source):
            logger.warning("source missing, dropping transfer: %s", original_source)
            self._state.transfer_tombstone(req.id)
            await self._bus.publish(
                E.TransferFailed(
                    request_id=req.id,
                    source_path=original_source,
                    error="source file missing",
                    will_retry=False,
                )
            )
            return

        await self._bus.publish(
            E.TransferStarted(
                request_id=req.id,
                source_path=original_source,
                destination_path=req.destination_path,
                label=req.label,
            )
        )

        working_source = original_source
        working_dest = req.destination_path

        if req.compress and working_source.endswith(IQ_DATA_FILE_EXTENSION) and not req.keep_source:
            try:
                if self._gate is not None:
                    async with self._gate.cpu_slot():
                        if self._stop.is_set():
                            return
                        working_source = await compress_file(working_source)
                else:
                    working_source = await compress_file(working_source)
                if not working_dest.endswith(".zst"):
                    working_dest = working_dest + ".zst"
            except Exception as e:
                if _is_disk_full(e):
                    # Disk full: re-queue with long backoff. Falling through to
                    # raw upload would consume far more bandwidth and NAS space
                    # than waiting for an in-flight upload to free local space.
                    await self._handle_failure(
                        req, original_source, original_source, req.destination_path, e
                    )
                    return
                logger.error("compression failed for %s: %s — uploading raw", original_source, e)

        async def report(copied: int, total: int) -> None:
            progress = E.TransferProgress(
                request_id=req.id,
                source_path=original_source,
                copied=copied,
                total=total,
            )
            async with self._active_lock:
                self._active[req.id] = progress
            await self._bus.publish(progress)

        # Throttle progress updates to ~4 Hz plus terminal points. Without
        # this, a 1 GB file at 50 MB/s would fire 30k+ bus events.
        last_emit = [0.0]
        last_copied = [0]

        def throttled(copied: int, total: int) -> None:
            now = time.monotonic()
            is_last = total > 0 and copied >= total
            if not is_last and now - last_emit[0] < 0.25 and copied - last_copied[0] < (1 << 20):
                return
            last_emit[0] = now
            last_copied[0] = copied
            asyncio.run_coroutine_threadsafe(report(copied, total), loop)

        try:
            await loop.run_in_executor(
                None,
                _copy_with_progress_sync,
                working_source,
                working_dest,
                throttled,
            )
        except Exception as e:
            async with self._active_lock:
                self._active.pop(req.id, None)
            await self._handle_failure(req, original_source, working_source, working_dest, e)
            return

        if not req.keep_source:
            # If we compressed, working_source (.zst) AND original_source (.bin)
            # may both exist on disk. Clean up both.
            for path in {working_source, original_source}:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    logger.exception("could not remove %s after upload", path)

        async with self._active_lock:
            self._active.pop(req.id, None)
        self._state.transfer_tombstone(req.id)

        event = E.TransferCompleted(
            request_id=req.id,
            source_path=original_source,
            destination_path=working_dest,
            label=req.label,
        )
        self._completed_tail.append(event)
        self._state.record_completed(
            {
                "id": req.id,
                "source": original_source,
                "dest": working_dest,
                "label": req.label,
                "ts": event.ts.isoformat(),
            }
        )
        await self._bus.publish(event)

        try:
            parent = os.path.dirname(working_source)
            if parent and os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
        except OSError:
            pass

    async def _handle_failure(
        self,
        req: TransferRequest,
        original_source: str,
        working_source: str,
        working_dest: str,
        error: Exception,
    ) -> None:
        compressed = working_source != original_source and os.path.exists(working_source)

        if _is_disk_full(error):
            disk_retries = req.disk_retry_count + 1
            if disk_retries <= MAX_DISK_RETRIES:
                wait = min(
                    DISK_FULL_BACKOFF_INITIAL * (2 ** (disk_retries - 1)),
                    DISK_FULL_BACKOFF_MAX,
                )
                logger.warning(
                    "transfer hit disk-full (%s). retry %d/%d in %ds",
                    error,
                    disk_retries,
                    MAX_DISK_RETRIES,
                    wait,
                )
                await self._bus.publish(
                    E.TransferFailed(
                        request_id=req.id,
                        source_path=original_source,
                        error=str(error),
                        will_retry=True,
                    )
                )
                await asyncio.sleep(wait)
                retry = TransferRequest(
                    id=str(uuid.uuid4()),
                    source_path=working_source if compressed else original_source,
                    destination_path=working_dest if compressed else req.destination_path,
                    keep_source=req.keep_source,
                    compress=False if compressed else req.compress,
                    attempt=req.attempt,
                    disk_retry_count=disk_retries,
                    pass_id=req.pass_id,
                    label=req.label,
                )
                self._state.transfer_put(retry)
                self._state.transfer_tombstone(req.id)
                await self._bus.publish(E.TransferQueued(request=retry))
                return
            logger.error(
                "transfer failed permanently after %d disk-full retries: %s",
                MAX_DISK_RETRIES,
                original_source,
            )

        attempt = req.attempt + 1
        if attempt < MAX_ATTEMPTS:
            wait = 10 * attempt
            logger.warning(
                "transfer failed (%s). retry %d/%d in %ds",
                error,
                attempt,
                MAX_ATTEMPTS,
                wait,
            )
            await self._bus.publish(
                E.TransferFailed(
                    request_id=req.id,
                    source_path=original_source,
                    error=str(error),
                    will_retry=True,
                )
            )
            await asyncio.sleep(wait)
            # If we already compressed successfully, retry with the compressed
            # path directly so we don't try to re-compress a file we may have
            # (in earlier versions) already deleted. compress=False because
            # the work is done.
            retry = TransferRequest(
                id=str(uuid.uuid4()),
                source_path=working_source if compressed else original_source,
                destination_path=working_dest if compressed else req.destination_path,
                keep_source=req.keep_source,
                compress=False if compressed else req.compress,
                attempt=attempt,
                disk_retry_count=req.disk_retry_count,
                pass_id=req.pass_id,
                label=req.label,
            )
            # Put retry first, THEN tombstone the original. A crash between
            # these two lines leaves both entries in state — the retry wins
            # on reload, so the work is preserved (vs. the reverse order
            # which could silently drop the work).
            self._state.transfer_put(retry)
            self._state.transfer_tombstone(req.id)
            await self._bus.publish(E.TransferQueued(request=retry))
            return

        logger.error(
            "transfer failed permanently after %d attempts: %s", MAX_ATTEMPTS, original_source
        )
        self._state.transfer_tombstone(req.id)
        if not req.keep_source:
            for path in {working_source, original_source}:
                try:
                    if os.path.exists(path):
                        os.remove(path)
                except OSError:
                    pass
        await self._bus.publish(
            E.TransferFailed(
                request_id=req.id,
                source_path=original_source,
                error=str(error),
                will_retry=False,
            )
        )


def _copy_with_progress_sync(
    src: str,
    dst: str,
    report: Callable[[int, int], None],
    buffer_size: int = 64 * 1024,
) -> None:
    """Resumable byte copy. Picks up partial dst files to minimise re-transfer
    over slow NAS links — critical if the Pi reboots mid-upload."""
    total = os.path.getsize(src)
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    start_offset = 0
    if os.path.exists(dst):
        existing = os.path.getsize(dst)
        if existing == total:
            report(total, total)
            return
        if existing > total:
            raise OSError(
                f"destination larger than source ({existing} > {total}); refusing to overwrite"
            )
        if existing > 0:
            start_offset = existing
            logger.info(
                "resuming upload of %s from %.1f MB / %.1f MB",
                os.path.basename(src),
                start_offset / (1024 * 1024),
                total / (1024 * 1024),
            )

    copied = start_offset
    open_mode = "ab" if start_offset > 0 else "wb"
    with open(src, "rb") as fsrc, open(dst, open_mode) as fdst:
        if start_offset > 0:
            fsrc.seek(start_offset)
            report(copied, total)
        while True:
            buf = fsrc.read(buffer_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
            report(copied, total)


async def compress_file(source_path: str) -> str:
    """Stream-compresses ``source_path`` to ``.zst`` via the system ``zstd``
    binary.

    Writes to ``.zst.tmp`` and renames atomically on success — a crash
    mid-compression leaves only the partial ``.tmp`` (which boot recovery
    cleans up), never a partial ``.zst`` that would be mistaken for valid
    output and uploaded as-is.
    """
    compressed = source_path + ".zst"
    tmp = compressed + ".tmp"
    try:
        os.remove(tmp)
    except OSError:
        pass
    logger.info("compressing %s", source_path)
    proc = await asyncio.create_subprocess_exec(
        "zstd",
        "--no-progress",
        "-9",
        "-f",
        source_path,
        "-o",
        tmp,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise RuntimeError(
            f"zstd failed with code {proc.returncode}: {err.decode(errors='replace')}"
        )
    os.replace(tmp, compressed)
    return compressed
