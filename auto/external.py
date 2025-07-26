
import logging
import os
import subprocess
import shlex
from typing import List
import shutil
from common import Decoder, Satellite

RECORDER_IMAGE = "ghcr.io/xarantolus/groundstation/satellite-recorder:latest"

def is_root() -> bool:
    return os.getuid() == 0

def run_recorder(sat_conf: Satellite, stop_after: float, out_dir: str) -> str:
    """
    Runs a podman container to record the pass. Returns path to output file.

    sat_conf: Dictionary with satellite configuration
    stop_after: Time in minutes to stop recording after the pass starts
    out_dir: Directory where the recording will be saved
    """
    envs: List[str] = [
        f"FREQUENCY={sat_conf['frequency']}",
        f"BANDWIDTH={sat_conf['bandwidth']}",
        f"SAMP_RATE={sat_conf['sample_rate']}",
        f"LO_OFFSET={sat_conf.get('lo_offset', 0)}",
        f"OUTPUT_FILE=/data/recording.bin",
    ]
    cmd: List[str] = [
        "podman", "run", "--rm",
        *(['--userns=keep-id'] if not is_root() else []),
        *sum([["-e", e] for e in envs], []),
        "--device", "/dev/bus/usb:/dev/bus/usb",
        "-v", f"{out_dir}:/data:z",
        RECORDER_IMAGE
    ]
    logging.info(f"Running recorder: {' '.join(cmd)}")

    # Start the process and wait for it to complete or for timeout
    process = subprocess.Popen(cmd)

    try:
        process.wait(timeout=stop_after * 60)
    except subprocess.TimeoutExpired:
        logging.info(f"Recorder process exceeded timeout of {stop_after} minutes. Terminating...")
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

def run_decoder(sat_conf: Satellite, decoder: Decoder, pass_dir: str):
    """
    Runs a podman container to decode the recording.
    Returns a list of absolute paths to new files created by the decoder.

    Args:
        sat_conf: Dictionary with satellite configuration
        pass_dir: Directory where the recording is stored and where outputs will be saved
    """

    pass_out_dir = pass_dir
    dec_name = decoder.get("name")
    if dec_name:
        pass_out_dir = os.path.join(pass_out_dir, dec_name)
        os.makedirs(pass_out_dir, exist_ok=True)

    envs: List[str] = [
        "INPUT_FILE=/data/recording.bin",
        f"OUTPUT_DIR=/output",
        f"FREQUENCY={sat_conf['frequency']}",
        f"BANDWIDTH={sat_conf['bandwidth']}",
        f"SAMP_RATE={sat_conf['sample_rate']}",
        f"LO_OFFSET={sat_conf.get('lo_offset', 0)}",
    ]
    for k, v in decoder.get("env", {}).items():
        envs.append(f"{k}={v}")

    cmd: List[str] = [
        "podman", "run", "--rm",
        *(['--userns=keep-id'] if not is_root() else []),
        *sum([["-e", e] for e in envs], []),
        *shlex.split(decoder.get("podman_args", "")),
        "-v", f"{pass_dir}:/data:z",
        "-v", f"{pass_out_dir}:/output:z",
        decoder["container"],
        *shlex.split(decoder.get("args", ""))
    ]

    logging.info(f"Running decoder: {' '.join(cmd)}")

    # We intentionally do not check the return code of the decoder process
    # E.g. satdump may crash, but we still want to upload files it created
    subprocess.run(cmd)

    # Count how many files were created, recursively
    files = []
    for root, _, filenames in os.walk(pass_out_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            if os.path.isfile(file_path):
                files.append(file_path)

    if dec_name and (len(files) < decoder.get("min_files", 1)):
        shutil.rmtree(pass_out_dir)
