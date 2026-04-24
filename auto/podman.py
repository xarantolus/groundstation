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


def _memory_limit_mb() -> Optional[int]:
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

    buffer = bytearray()
    while True:
        try:
            chunk = await stream.read(1024)
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


async def run_decoder(
    sat: Satellite,
    decoder: Decoder,
    pass_dir: str,
    output_dir: str,
    log_callback: Optional[Callable[[str], None]] = None,
    timeout_minutes: float | None = DEFAULT_DECODER_TIMEOUT_MINUTES,
) -> int:
    dec_timeout = decoder.timeout_minutes if decoder.timeout_minutes is not None else timeout_minutes
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
    try:
        if dec_timeout is None:
            await gathered
        else:
            await asyncio.wait_for(gathered, timeout=dec_timeout * 60)
    except asyncio.TimeoutError:
        logger.warning("decoder exceeded %s min — terminating", dec_timeout)
        await _terminate(process)
    except asyncio.CancelledError:
        await _terminate(process)
        raise

    return process.returncode if process.returncode is not None else -1


def filter_decoder_outputs(output_dir: str, decoder: Decoder) -> List[str]:
    """Apply min_size_bytes/min_files policy. Returns the surviving file list.
    Mirrors the existing external.py behavior so decoder contracts stay identical."""
    if not os.path.isdir(output_dir):
        return []
    if decoder.min_size_bytes > 0:
        for root, _, files in os.walk(output_dir):
            for name in files:
                p = os.path.join(root, name)
                try:
                    if os.path.getsize(p) < decoder.min_size_bytes:
                        os.remove(p)
                        logger.info("removed %s (< min_size_bytes=%d)", p, decoder.min_size_bytes)
                except OSError:
                    pass

    files: List[str] = []
    for root, _, filenames in os.walk(output_dir):
        for name in filenames:
            files.append(os.path.join(root, name))

    if len(files) < decoder.min_files:
        import shutil

        shutil.rmtree(output_dir, ignore_errors=True)
        return []
    return files
