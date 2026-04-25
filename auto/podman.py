from __future__ import annotations

import asyncio
import logging
import os
import platform
import shlex
from typing import Callable, List, Optional

from .models import Decoder, Satellite

logger = logging.getLogger("groundstation.podman")

RECORDER_IMAGE = "ghcr.io/xarantolus/groundstation/satellite-recorder:latest"
DEFAULT_DECODER_TIMEOUT_MINUTES: float | None = 30.0


def _cgroup_memory_available() -> bool:
    """True when the kernel/cgroup hierarchy exposes the memory controller.
    Without this, podman's ``--memory`` flag fails at container start with
    ``memory.max: No such file or directory``. On Raspberry Pi OS the memory
    controller needs ``cgroup_memory=1 cgroup_enable=memory`` in cmdline.txt."""
    try:
        with open("/sys/fs/cgroup/cgroup.controllers") as f:
            if "memory" in f.read().split():
                return True
    except OSError:
        pass
    return os.path.isdir("/sys/fs/cgroup/memory")


def _memory_limit_mb() -> Optional[int]:
    if platform.system() != "Linux":
        return None
    if not _cgroup_memory_available():
        logger.warning(
            "cgroup memory controller unavailable — skipping --memory limit; "
            "a runaway decoder can crash the host. "
            "On Pi OS add 'cgroup_memory=1 cgroup_enable=memory' to "
            "/boot/firmware/cmdline.txt and reboot."
        )
        return None
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return int(kb * 0.9 / 1024)
    except Exception:
        return None
    return None


_MEMORY_LIMIT_MB = _memory_limit_mb()

COMMON_FLAGS: List[str] = [
    "--pull=never",
    "-v",
    f"{os.path.expanduser('~')}/.config/gnuradio/prefs:/root/.config/gnuradio/prefs:z",
]

if _MEMORY_LIMIT_MB:
    COMMON_FLAGS.extend(["--memory", f"{_MEMORY_LIMIT_MB}m"])


def _is_root() -> bool:
    if platform.system() == "Windows":
        return False
    try:
        return os.getuid() == 0
    except AttributeError:
        return False


async def _read_stream(
    stream: asyncio.StreamReader,
    callback: Optional[Callable[[str], None]],
    prefix: str = "",
) -> None:
    if not callback:
        # still drain, otherwise the subprocess stalls when the pipe fills
        while await stream.read(4096):
            pass
        return

    # GNU Radio writes status chars (O, U, L, D for overflow/underflow/late/
    # dropped) to stderr without newlines. A pure newline-buffered reader
    # would hide them until the next \n arrives — sometimes never. Flush any
    # pending buffer if no new bytes arrive within this interval.
    flush_after = 1.0
    buffer = bytearray()
    while True:
        try:
            try:
                chunk = await asyncio.wait_for(stream.read(1024), timeout=flush_after)
            except asyncio.TimeoutError:
                if buffer:
                    callback(f"{prefix}{buffer.decode('utf-8', errors='replace')}")
                    buffer.clear()
                continue
            if not chunk:
                break
            for byte in chunk:
                if byte == 10 or byte == 13:
                    line = buffer.decode("utf-8", errors="replace")
                    if line:
                        callback(f"{prefix}{line}")
                    buffer.clear()
                else:
                    buffer.append(byte)
        except Exception:
            logger.exception("stream reader error")
            break

    if buffer:
        callback(f"{prefix}{buffer.decode('utf-8', errors='replace')}")


async def _terminate(process: asyncio.subprocess.Process) -> None:
    try:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=15)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
    except ProcessLookupError:
        pass


async def run_recorder(
    sat: Satellite,
    stop_after_minutes: float,
    out_dir: str,
    log_callback: Optional[Callable[[str], None]] = None,
) -> str:
    envs = [
        f"FREQUENCY={sat.frequency}",
        f"BANDWIDTH={sat.bandwidth}",
        f"SAMP_RATE={sat.sample_rate}",
        f"LO_OFFSET={sat.lo_offset}",
        "OUTPUT_FILE=/data/recording.bin",
    ]
    if sat.gain is not None:
        envs.append(f"GAIN={sat.gain}")
    cmd: List[str] = [
        "podman",
        "run",
        "--rm",
        "--read-only",
        *COMMON_FLAGS,
        *(["--userns=keep-id"] if not _is_root() else []),
        *sum([["-e", e] for e in envs], []),
        "--device",
        "/dev/bus/usb:/dev/bus/usb",
        "-v",
        f"{out_dir}:/data:z",
        RECORDER_IMAGE,
    ]
    logger.info("running recorder: %s", " ".join(cmd))

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    gathered = asyncio.gather(
        _read_stream(process.stdout, log_callback),
        _read_stream(process.stderr, log_callback, "ERR: "),
        process.wait(),
    )
    try:
        await asyncio.wait_for(gathered, timeout=stop_after_minutes * 60)
    except asyncio.TimeoutError:
        logger.info("recorder exceeded %s min — terminating", stop_after_minutes)
        await _terminate(process)
    except asyncio.CancelledError:
        await _terminate(process)
        raise

    out_file = os.path.join(out_dir, "recording.bin")
    if not os.path.isfile(out_file):
        raise FileNotFoundError("recorder did not produce recording.bin")
    return out_file


DECODER_KILLED_BY_STOP = -2


async def run_decoder(
    sat: Satellite,
    decoder: Decoder,
    pass_dir: str,
    output_dir: str,
    log_callback: Optional[Callable[[str], None]] = None,
    timeout_minutes: float | None = None,
    stop_event: Optional[asyncio.Event] = None,
) -> int:
    if timeout_minutes is not None:
        dec_timeout = timeout_minutes
    elif decoder.timeout_minutes is not None:
        dec_timeout = decoder.timeout_minutes
    else:
        dec_timeout = DEFAULT_DECODER_TIMEOUT_MINUTES
    if dec_timeout is not None and dec_timeout <= 0:
        dec_timeout = None

    os.makedirs(output_dir, exist_ok=True)

    envs = [
        "INPUT_FILE=/data/recording.bin",
        "OUTPUT_DIR=/output",
        f"FREQUENCY={sat.frequency}",
        f"BANDWIDTH={sat.bandwidth}",
        f"SAMP_RATE={sat.sample_rate}",
        f"LO_OFFSET={sat.lo_offset}",
    ]
    if sat.gain is not None:
        envs.append(f"GAIN={sat.gain}")
    for k, v in decoder.env.items():
        envs.append(f"{k}={v}")

    cmd: List[str] = [
        "podman",
        "run",
        "--rm",
        "--read-only",
        *COMMON_FLAGS,
        *(["--userns=keep-id"] if not _is_root() else []),
        *sum([["-e", e] for e in envs], []),
        *shlex.split(decoder.podman_args or ""),
        "-v",
        f"{pass_dir}:/data:z",
        "-v",
        f"{output_dir}:/output:z",
        decoder.container,
        *shlex.split(decoder.args or ""),
    ]
    logger.info("running decoder: %s", " ".join(cmd))

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    gathered = asyncio.gather(
        _read_stream(process.stdout, log_callback),
        _read_stream(process.stderr, log_callback, "ERR: "),
        process.wait(),
    )
    killed_by_stop = False
    main_task = asyncio.ensure_future(gathered)
    waiters: List[asyncio.Future] = [main_task]
    stop_task: Optional[asyncio.Task] = None
    if stop_event is not None:
        stop_task = asyncio.create_task(stop_event.wait())
        waiters.append(stop_task)

    try:
        timeout = dec_timeout * 60 if dec_timeout is not None else None
        done, _ = await asyncio.wait(
            waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not done:
            logger.warning("decoder exceeded %s min — terminating", dec_timeout)
            await _terminate(process)
            await asyncio.gather(main_task, return_exceptions=True)
        elif stop_task is not None and stop_task in done:
            logger.info("decoder stop signalled — terminating")
            killed_by_stop = True
            await _terminate(process)
            await asyncio.gather(main_task, return_exceptions=True)
    except asyncio.CancelledError:
        await _terminate(process)
        await asyncio.gather(main_task, return_exceptions=True)
        raise
    finally:
        if stop_task is not None and not stop_task.done():
            stop_task.cancel()

    if killed_by_stop:
        return DECODER_KILLED_BY_STOP
    return process.returncode if process.returncode is not None else -1


_PASS_DIR_PROTECTED = frozenset({"recording.bin", "recording.bin.zst", "info.json"})


def filter_decoder_outputs(
    output_dir: str, decoder: Decoder, pass_dir: Optional[str] = None
) -> List[str]:
    """Apply min_size_bytes/min_files policy. Returns the surviving file list.
    Enforces decoder.min_size_bytes / min_files to drop runs that produced
    nothing usable.

    When ``output_dir`` is the shared pass directory (unnamed decoder) the
    recording/info files and sibling decoder subdirectories are ignored, and
    the directory is never rmtree'd on failure."""
    if not os.path.isdir(output_dir):
        return []

    shared = pass_dir is not None and os.path.abspath(output_dir) == os.path.abspath(pass_dir)

    def iter_outputs():
        if shared:
            for name in os.listdir(output_dir):
                if name in _PASS_DIR_PROTECTED:
                    continue
                full = os.path.join(output_dir, name)
                if os.path.isfile(full):
                    yield full
        else:
            for root, _, filenames in os.walk(output_dir):
                for name in filenames:
                    yield os.path.join(root, name)

    if decoder.min_size_bytes > 0:
        for path in list(iter_outputs()):
            try:
                if os.path.getsize(path) < decoder.min_size_bytes:
                    os.remove(path)
                    logger.info("removed %s (< min_size_bytes=%d)", path, decoder.min_size_bytes)
            except OSError:
                pass

    files: List[str] = list(iter_outputs())

    if len(files) < decoder.min_files:
        if not shared:
            import shutil

            shutil.rmtree(output_dir, ignore_errors=True)
        return []
    return files
