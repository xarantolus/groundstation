import logging
import os
import shlex
import asyncio
import shutil
from typing import List, Callable, Optional
from common import Decoder, Satellite

RECORDER_IMAGE = "ghcr.io/xarantolus/groundstation/satellite-recorder:latest"

COMMON_FLAGS = [
    # Mount /root/.config/gnuradio/prefs to home directory
    "-v",
    f"{os.path.expanduser('~')}/.config/gnuradio/prefs:/root/.config/gnuradio/prefs:z",
]


def is_root() -> bool:
    import platform

    if platform.system() == "Windows":
        return False  # podman on windows usually isn't "root" in the same way
    return os.getuid() == 0


async def _read_stream(
    stream: asyncio.StreamReader,
    callback: Optional[Callable[[str], None]] = None,
    prefix: str = "",
):
    """
    Reads from a stream and calls the callback for every line.
    Handles \r as a newline to support progress bars.
    """
    if not callback:
        return

    buffer = bytearray()
    while True:
        try:
            # excessive logic to support \r (e.g. for progress bars) as regex newline
            chunk = await stream.read(1024)
            if not chunk:
                break

            # Process chunk byte by byte to handle \r and \n correctly
            for byte in chunk:
                if byte == 10:  # \n
                    line = buffer.decode("utf-8", errors="replace")
                    callback(f"{prefix}{line}")
                    buffer.clear()
                elif byte == 13:  # \r
                    line = buffer.decode("utf-8", errors="replace")
                    callback(f"{prefix}{line}")
                    buffer.clear()
                else:
                    buffer.append(byte)

        except Exception as e:
            logging.error(f"Stream reader error: {e}")
            break

    # Flush remaining buffer
    if buffer:
        line = buffer.decode("utf-8", errors="replace")
        callback(f"{prefix}{line}")


async def run_recorder(
    sat_conf: Satellite,
    stop_after: float,
    out_dir: str,
    log_callback: Optional[Callable[[str], None]] = None,
) -> str:
    """
    Runs a podman container to record the pass. Returns path to output file.

    sat_conf: Dictionary with satellite configuration
    stop_after: Time in minutes to stop recording after the pass starts
    out_dir: Directory where the recording will be saved
    log_callback: Optional callback for streaming logs
    """
    envs: List[str] = [
        f"FREQUENCY={sat_conf['frequency']}",
        f"BANDWIDTH={sat_conf['bandwidth']}",
        f"SAMP_RATE={sat_conf['sample_rate']}",
        f"LO_OFFSET={sat_conf.get('lo_offset', 0)}",
        "OUTPUT_FILE=/data/recording.bin",
    ]
    cmd: List[str] = [
        "podman",
        "run",
        "--rm",
        "--read-only",
        *COMMON_FLAGS,
        *(["--userns=keep-id"] if not is_root() else []),
        *sum([["-e", e] for e in envs], []),
        "--device",
        "/dev/bus/usb:/dev/bus/usb",
        "-v",
        f"{out_dir}:/data:z",
        RECORDER_IMAGE,
    ]
    logging.info(f"Running recorder: {' '.join(cmd)}")

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    try:
        await asyncio.wait_for(
            asyncio.gather(
                _read_stream(process.stdout, log_callback),
                _read_stream(process.stderr, log_callback, "ERR: "),
                process.wait(),
            ),
            timeout=stop_after * 60,
        )
    except asyncio.TimeoutError:
        logging.info(
            f"Recorder process exceeded timeout of {stop_after} minutes. Terminating..."
        )
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=15)
        except Exception:
            logging.error("Recorder process didn't terminate properly. Killing...")
            try:
                process.kill()
            except ProcessLookupError:
                pass  # Process already dead

    out_file: str = os.path.join(out_dir, "recording.bin")
    if not os.path.isfile(out_file):
        raise FileNotFoundError("Recorder did not produce recording.bin")
    return out_file


async def run_decoder(
    sat_conf: Satellite,
    decoder: Decoder,
    pass_dir: str,
    log_callback: Optional[Callable[[str], None]] = None,
):
    """
    Runs a podman container to decode the recording.
    Returns a list of absolute paths to new files created by the decoder.

    Args:
        sat_conf: Dictionary with satellite configuration
        pass_dir: Directory where the recording is stored and where outputs will be saved
        log_callback: Optional callback for streaming logs
    """

    pass_out_dir = pass_dir
    dec_name = decoder.get("name")
    if dec_name:
        pass_out_dir = os.path.join(pass_out_dir, dec_name)
        os.makedirs(pass_out_dir, exist_ok=True)

    envs: List[str] = [
        "INPUT_FILE=/data/recording.bin",
        "OUTPUT_DIR=/output",
        f"FREQUENCY={sat_conf['frequency']}",
        f"BANDWIDTH={sat_conf['bandwidth']}",
        f"SAMP_RATE={sat_conf['sample_rate']}",
        f"LO_OFFSET={sat_conf.get('lo_offset', 0)}",
    ]
    for k, v in decoder.get("env", {}).items():
        envs.append(f"{k}={v}")

    cmd: List[str] = [
        "podman",
        "run",
        "--rm",
        "--read-only",
        *COMMON_FLAGS,
        *(["--userns=keep-id"] if not is_root() else []),
        *sum([["-e", e] for e in envs], []),
        *shlex.split(decoder.get("podman_args", "")),
        "-v",
        f"{pass_dir}:/data:z",
        "-v",
        f"{pass_out_dir}:/output:z",
        decoder["container"],
        *shlex.split(decoder.get("args", "")),
    ]

    logging.info(f"Running decoder: {' '.join(cmd)}")

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    try:
        await asyncio.gather(
            _read_stream(process.stdout, log_callback),
            _read_stream(process.stderr, log_callback, "ERR: "),
            process.wait(),
        )
    except asyncio.CancelledError:
        logging.warning("Decoder process cancelled. Terminating...")
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5)
        except Exception:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        raise  # Re-raise to ensure cancellation propagates

    # Count how many files were created, recursively
    files = []
    for root, _, filenames in os.walk(pass_out_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            if os.path.isfile(file_path):
                files.append(file_path)

    if dec_name and (len(files) < decoder.get("min_files", 1)):
        shutil.rmtree(pass_out_dir)
