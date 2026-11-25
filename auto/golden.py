"""Golden passes: recordings worth keeping for decoder development.

After all decoders of a pass have run, passes of selected satellites are
sorted into three categories. A golden pass's IQ recording goes to
<nas>/golden/<category>/<pass folder>/, together with golden.json and a copy
of everything in the pass's normal NAS folder (decoder outputs, waterfall,
doppler.txt, info.json, ...):

  many_packets      a decoder produced lots of output (e.g. >= 50 packets)
  few_packets       clear signal bursts, but few packets per burst
  signal_no_packets clear signal bursts, nothing decoded

Bursts are counted with a short-time FFT energy detector around the
(Doppler-corrected) centre of the recording.
"""
from __future__ import annotations

import logging
import os
import shutil
from typing import Optional

import numpy as np

from .models import GoldenConfig, GroundstationConfig, Pass

logger = logging.getLogger("groundstation.golden")

GOLDEN_SUBDIR = "golden"
CATEGORIES = ("many_packets", "few_packets", "signal_no_packets")

WINDOW_S = 0.021  # ~21 ms analysis windows
HALF_BAND_HZ = 6_000.0  # look for the signal within +-6 kHz of centre
NOISE_BAND_HZ = (15_000.0, 35_000.0)  # reference noise, away from the signal
HOT_SNR_DB = 3.0
MIN_BURST_S = 0.08
# Only clear bursts count: weaker ones are mostly noise or out of reach of
# any demodulator, and would fill signal_no_packets with empty recordings.
CLEAR_SNR_DB = 6.0


def count_bursts(path: str, samp_rate: float, max_bytes: Optional[int] = None) -> int:
    """Count clear signal bursts in a complex64 recording.

    A window is "hot" when the excess power within +-6 kHz (over the per-bin
    median, which removes spurs and DC) is 3 dB above the noise measured
    15-35 kHz off centre. Hot windows with at most one-window gaps form a
    burst; bursts shorter than ~80 ms or with a mean SNR below 6 dB are
    ignored.
    """
    if samp_rate < 2 * NOISE_BAND_HZ[1]:
        raise ValueError(f"sample rate {samp_rate} too low for burst detection")
    win = 1 << int(np.ceil(np.log2(samp_rate * WINDOW_S)))
    freqs = np.fft.fftshift(np.fft.fftfreq(win, 1 / samp_rate))
    band = np.abs(freqs) <= HALF_BAND_HZ
    outer = (np.abs(freqs) > NOISE_BAND_HZ[0]) & (np.abs(freqs) < NOISE_BAND_HZ[1])
    taper = np.hanning(win).astype(np.float32)

    band_rows = []
    noise_rows = []
    chunk = win * 512
    read = 0
    with open(path, "rb") as f:
        while True:
            want = chunk * 8
            if max_bytes is not None:
                want = min(want, max_bytes - read)
                if want <= 0:
                    break
            buf = f.read(want)
            read += len(buf)
            n = len(buf) // (8 * win)
            if n == 0:
                break
            iq = np.frombuffer(buf[: n * win * 8], dtype=np.complex64).reshape(n, win)
            spec = np.abs(np.fft.fftshift(np.fft.fft(iq * taper, axis=1), axes=1)) ** 2
            band_rows.append(spec[:, band].astype(np.float32))
            noise_rows.append(np.median(spec[:, outer], axis=1).astype(np.float32))
            if len(buf) < want:
                break
    if not band_rows:
        return 0
    spec_band = np.concatenate(band_rows)
    noise = np.concatenate(noise_rows)
    excess = (spec_band - np.median(spec_band, axis=0)).clip(min=0).sum(axis=1)
    snr = 10 * np.log10(np.maximum(excess, 1e-30) / np.maximum(noise * band.sum(), 1e-30))
    hot = snr > HOT_SNR_DB

    min_windows = max(1, int(np.ceil(MIN_BURST_S * samp_rate / win)))
    bursts = 0
    n = len(hot)
    i = 0
    while i < n:
        if not hot[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and (hot[j + 1] or (j + 2 < n and hot[j + 2])):
            j += 1
        if j - i + 1 >= min_windows and snr[i : j + 1].mean() >= CLEAR_SNR_DB:
            bursts += 1
        i = j + 1
    return bursts


def applies_to(cfg: Optional[GoldenConfig], p: Pass) -> bool:
    if cfg is None or not p.satellite.decoder:
        return False
    if not cfg.tags:
        return True
    return any(t in cfg.tags for t in p.satellite.tags)


def classify(cfg: GoldenConfig, packets: int, bursts: Optional[int]) -> Optional[str]:
    """Category for a pass with `packets` outputs of its best data decoder
    and `bursts` detected signal bursts (None: not measured)."""
    if packets >= cfg.min_packets:
        return "many_packets"
    if bursts is None or bursts < cfg.min_bursts:
        return None
    if packets == 0:
        return "signal_no_packets"
    if packets < cfg.few_packets_ratio * bursts:
        return "few_packets"
    return None


def pass_folder(p: Pass) -> str:
    return f"{p.satellite.name}_{p.pass_info.start_time.strftime('%Y-%m-%d_%H-%M-%S')}"


def golden_dir(nas_directory: str, p: Pass, category: str) -> str:
    return os.path.join(nas_directory, GOLDEN_SUBDIR, category, pass_folder(p))


def count_existing(nas_directory: str, category: str, satellite_name: str) -> int:
    """Golden passes of this satellite already kept in a category."""
    try:
        names = os.listdir(os.path.join(nas_directory, GOLDEN_SUBDIR, category))
    except FileNotFoundError:
        return 0
    prefix = satellite_name + "_"
    return sum(1 for n in names if n.startswith(prefix))


def pass_nas_dir(nas_directory: str, p: Pass) -> str:
    start = p.pass_info.start_time
    return os.path.join(
        nas_directory, start.strftime("%Y"), start.strftime("%Y-%m-%d"), pass_folder(p)
    )


def copy_pass_folder(nas_directory: str, p: Pass) -> int:
    """Put everything in the pass's NAS folder into its golden folder as
    hard links: no data goes over the network, no extra space is used, and
    the golden copy survives deleting the pass folder. Falls back to a
    normal copy where linking fails. Files already there with the same size
    are skipped, as are unfinished uploads (.part). Returns the number of
    files added."""
    assert p.golden is not None
    src_root = pass_nas_dir(nas_directory, p)
    dest_root = golden_dir(nas_directory, p, p.golden)
    copied = 0
    for root, _dirs, files in os.walk(src_root):
        for name in files:
            if name.endswith(".part"):
                continue
            src = os.path.join(root, name)
            dest = os.path.join(dest_root, os.path.relpath(src, src_root))
            try:
                if os.path.getsize(dest) == os.path.getsize(src):
                    continue
            except OSError:
                pass
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            if os.path.lexists(dest):
                os.remove(dest)  # different size: replace
            try:
                os.link(src, dest)
            except OSError as e:
                logger.warning("golden: hard link %s failed (%s), copying", src, e)
                shutil.copyfile(src, dest)
            copied += 1
    return copied


def iq_destination(cfg: GroundstationConfig, p: Pass) -> str:
    """Where a pass's compressed IQ goes: its golden dir, else its pass dir."""
    if p.golden is not None:
        return os.path.join(golden_dir(cfg.nas_directory, p, p.golden), "recording.bin.zst")
    return os.path.join(pass_nas_dir(cfg.nas_directory, p), "recording.bin.zst")
