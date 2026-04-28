from __future__ import annotations

import asyncio
import datetime
import logging
import os
import platform
import shlex
import uuid
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

def _host_timezone() -> Optional[str]:
    """IANA timezone name of the host, e.g. 'Europe/Berlin'. None if unknown.
    Used so containers can render local times the way the operator expects."""
    try:
        with open("/etc/timezone") as f:
            name = f.read().strip()
            if name:
                return name
    except (FileNotFoundError, IsADirectoryError, PermissionError, OSError):
        pass
    try:
        target = os.readlink("/etc/localtime")
        marker = "zoneinfo/"
        idx = target.find(marker)
        if idx >= 0:
            return target[idx + len(marker):]
    except OSError:
        pass
    return None


_HOST_TZ = _host_timezone()

COMMON_FLAGS: List[str] = [
    "--pull=never",
    "-v",
    f"{os.path.expanduser('~')}/.config/gnuradio/prefs:/root/.config/gnuradio/prefs:z",
    "-v",
    "/etc/localtime:/etc/localtime:ro",
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


async def _podman_kill(container_name: str) -> None:
    """SIGKILL a container by name. Required because SIGTERM to the
    `podman run` client process doesn't reliably propagate through bash
    entrypoints to the actual decoder process inside the container."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "podman", "kill", "--signal", "KILL", container_name,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=10)
    except Exception:
        logger.exception("podman kill %s failed", container_name)


async def _container_exists(name: str) -> bool:
    """True if podman knows about a container with this name (any state).
    Returns True on internal errors so a transient `podman` failure can't
    cause the watchdog to falsely shoot down a healthy decoder."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "podman", "container", "exists", name,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            rc = await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
            return True
        return rc == 0
    except Exception:
        logger.exception("podman container exists check failed for %s", name)
        return True


CONTAINER_WATCHDOG_STARTUP_S = 60.0
CONTAINER_WATCHDOG_POLL_S = 5.0
CONTAINER_WATCHDOG_GRACE_S = 15.0


async def _container_watchdog(
    container_name: str,
    process: asyncio.subprocess.Process,
) -> None:
    """Detect ``podman run`` hanging after the container has already exited.

    Symptom seen in the field: the container is gone (verified externally)
    but the `podman run` parent is still alive, so `process.wait()` never
    returns and the decoder appears stuck running until the full decoder
    timeout fires. This is a known podman/conmon race around `--rm` cleanup.

    This watchdog waits until the container becomes visible to podman, then
    polls until it disappears. Once gone, it gives the parent a grace window
    to unwind on its own (normal `--rm` cleanup is fast); if the parent is
    still alive after that, it gets SIGKILL so `process.wait()` returns and
    the run is marked failed via the standard retry path.
    """
    loop = asyncio.get_running_loop()

    startup_deadline = loop.time() + CONTAINER_WATCHDOG_STARTUP_S
    while True:
        if process.returncode is not None:
            return
        if await _container_exists(container_name):
            break
        if loop.time() >= startup_deadline:
            return
        await asyncio.sleep(1)

    while True:
        if process.returncode is not None:
            return
        if not await _container_exists(container_name):
            break
        try:
            await asyncio.wait_for(process.wait(), timeout=CONTAINER_WATCHDOG_POLL_S)
            return
        except asyncio.TimeoutError:
            pass

    try:
        await asyncio.wait_for(process.wait(), timeout=CONTAINER_WATCHDOG_GRACE_S)
        return
    except asyncio.TimeoutError:
        logger.warning(
            "container %s is gone but `podman run` still alive after %.0fs — SIGKILL'ing parent",
            container_name,
            CONTAINER_WATCHDOG_GRACE_S,
        )
        try:
            process.kill()
        except ProcessLookupError:
            pass


async def _terminate(process: asyncio.subprocess.Process, container_name: Optional[str] = None) -> None:
    if container_name:
        await _podman_kill(container_name)
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
    stop_event: Optional[asyncio.Event] = None,
) -> str:
    """Run the SDR recorder in podman. Returns the recording.bin path on
    normal completion *or* when ``stop_event`` is signalled (the partial
    recording is still useful — only an empty file raises FileNotFoundError).
    """
    envs = [
        f"FREQUENCY={sat.frequency}",
        f"BANDWIDTH={sat.bandwidth}",
        f"SAMP_RATE={sat.sample_rate}",
        f"LO_OFFSET={sat.lo_offset}",
        "OUTPUT_FILE=/data/recording.bin",
    ]
    if sat.gain is not None:
        envs.append(f"GAIN={sat.gain}")
    if sat.doppler_correction:
        envs.append("CORRECT_DOPPLER=1")
    container_name = f"gs-recorder-{uuid.uuid4().hex[:12]}"
    cmd: List[str] = [
        "podman",
        "run",
        "--rm",
        "--read-only",
        "--name", container_name,
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

    main_task = asyncio.ensure_future(asyncio.gather(
        _read_stream(process.stdout, log_callback),
        _read_stream(process.stderr, log_callback, "ERR: "),
        process.wait(),
    ))
    waiters: List[asyncio.Future] = [main_task]
    stop_task: Optional[asyncio.Task] = None
    if stop_event is not None:
        stop_task = asyncio.create_task(stop_event.wait())
        waiters.append(stop_task)

    try:
        timeout = stop_after_minutes * 60
        done, _ = await asyncio.wait(
            waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if not done:
            logger.info("recorder exceeded %s min — terminating", stop_after_minutes)
            await _terminate(process, container_name)
            await asyncio.gather(main_task, return_exceptions=True)
        elif stop_task is not None and stop_task in done:
            logger.info("recorder stop signalled — killing container %s", container_name)
            await _terminate(process, container_name)
            await asyncio.gather(main_task, return_exceptions=True)
    except asyncio.CancelledError:
        await _terminate(process, container_name)
        await asyncio.gather(main_task, return_exceptions=True)
        raise
    finally:
        if stop_task is not None and not stop_task.done():
            stop_task.cancel()

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
    pass_start: Optional[datetime.datetime] = None,
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
        f"SATELLITE_NAME={sat.name}",
    ]
    if _HOST_TZ:
        envs.append(f"TZ={_HOST_TZ}")
    if pass_start is not None:
        # Naive datetimes from pass_predictor are system local time
        # (ephem.localtime); .astimezone(utc) handles both naive and aware.
        pass_start_utc = pass_start.astimezone(datetime.timezone.utc)
        envs.append(
            f"PASS_START={pass_start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
    if sat.gain is not None:
        envs.append(f"GAIN={sat.gain}")
    for k, v in decoder.env.items():
        envs.append(f"{k}={v}")

    container_name = f"gs-decoder-{uuid.uuid4().hex[:12]}"
    cmd: List[str] = [
        "podman",
        "run",
        "--rm",
        "--read-only",
        "--name", container_name,
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

    watchdog_task = asyncio.create_task(
        _container_watchdog(container_name, process), name=f"watchdog-{container_name}"
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
            await _terminate(process, container_name)
            await asyncio.gather(main_task, return_exceptions=True)
        elif stop_task is not None and stop_task in done:
            logger.info("decoder stop signalled — terminating container %s", container_name)
            killed_by_stop = True
            await _terminate(process, container_name)
            await asyncio.gather(main_task, return_exceptions=True)
    except asyncio.CancelledError:
        await _terminate(process, container_name)
        await asyncio.gather(main_task, return_exceptions=True)
        raise
    finally:
        watchdog_task.cancel()
        try:
            await watchdog_task
        except (asyncio.CancelledError, Exception):
            pass
        if stop_task is not None and not stop_task.done():
            stop_task.cancel()

    if killed_by_stop:
        return DECODER_KILLED_BY_STOP
    return process.returncode if process.returncode is not None else -1


_PASS_DIR_PROTECTED = frozenset(
    {"recording.bin", "recording.bin.zst", "info.json", "doppler.txt"}
)


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
        action = "discarding output dir" if not shared else "discarding outputs"
        logger.info(
            "%s '%s' produced %d file(s), needs >= %d (decoder.min_files); %s",
            decoder.name or "decoder",
            output_dir,
            len(files),
            decoder.min_files,
            action,
        )
        if not shared:
            import shutil

            shutil.rmtree(output_dir, ignore_errors=True)
        return []
    return files
