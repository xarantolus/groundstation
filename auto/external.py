import logging
import os
import subprocess
import shlex
import threading
import io
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
        return False # podman on windows usually isn't "root" in the same way
    return os.getuid() == 0


def run_recorder(sat_conf: Satellite, stop_after: float, out_dir: str, log_callback: Optional[Callable[[str], None]] = None) -> str:
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

    # Start the process with pipes for stdout/stderr
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)

    def reader_thread(pipe, prefix=""):
        try:
            for line in io.TextIOWrapper(pipe, encoding="utf-8", errors="replace"):
                if log_callback:
                    log_callback(f"{prefix}{line.strip()}")
        except Exception as e:
            logging.debug(f"Reader thread error: {e}")

    t1 = threading.Thread(target=reader_thread, args=(process.stdout,), daemon=True)
    t2 = threading.Thread(target=reader_thread, args=(process.stderr, "ERR: "), daemon=True)
    t1.start()
    t2.start()

    try:
        process.wait(timeout=stop_after * 60)
    except subprocess.TimeoutExpired:
        logging.info(
            f"Recorder process exceeded timeout of {stop_after} minutes. Terminating..."
        )
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            logging.error("Recorder process didn't terminate properly. Killing...")
            process.kill()

    out_file: str = os.path.join(out_dir, "recording.bin")
    if not os.path.isfile(out_file):
        raise FileNotFoundError("Recorder did not produce recording.bin")
    return out_file


def run_decoder(sat_conf: Satellite, decoder: Decoder, pass_dir: str, log_callback: Optional[Callable[[str], None]] = None):
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

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)

    def reader_thread(pipe, prefix=""):
        try:
            for line in io.TextIOWrapper(pipe, encoding="utf-8", errors="replace"):
                if log_callback:
                    log_callback(f"{prefix}{line.strip()}")
        except Exception as e:
            logging.debug(f"Reader thread error: {e}")

    t1 = threading.Thread(target=reader_thread, args=(process.stdout,), daemon=True)
    t2 = threading.Thread(target=reader_thread, args=(process.stderr, "ERR: "), daemon=True)
    t1.start()
    t2.start()

    process.wait()

    # Count how many files were created, recursively
    files = []
    for root, _, filenames in os.walk(pass_out_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            if os.path.isfile(file_path):
                files.append(file_path)

    if dec_name and (len(files) < decoder.get("min_files", 1)):
        shutil.rmtree(pass_out_dir)
