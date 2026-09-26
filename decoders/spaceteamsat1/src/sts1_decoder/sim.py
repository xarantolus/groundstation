"""Synthetic STS1 signal generator (for self-tests of the demod/decode chain)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import numpy as np
from scipy import signal

from . import coding


def gfsk_modulate(bits: np.ndarray, baud: float, fs: float, h: float = 0.5, bt: float = 0.5) -> np.ndarray:
    sps = fs / baud
    n = int(np.ceil(len(bits) * sps))
    t_idx = (np.arange(n) / sps).astype(int)
    nrz = (2.0 * bits.astype(float) - 1.0)[np.minimum(t_idx, len(bits) - 1)]
    # gaussian pulse shaping
    span = 4
    tt = np.arange(-span * sps / 2, span * sps / 2 + 1) / sps
    sigma = np.sqrt(np.log(2)) / (2 * np.pi * bt)
    g = np.exp(-(tt**2) / (2 * sigma**2))
    g /= g.sum()
    f = signal.fftconvolve(nrz, g, mode="same") * (h * baud / 2)
    phase = 2 * np.pi * np.cumsum(f) / fs
    return np.exp(1j * phase).astype(np.complex64)


FW_ENCODER = Path(__file__).resolve().parents[2] / "tools" / "fw_encoder" / "fw_encoder"


def encode_with_firmware(frames: list[bytes]) -> list[bytes]:
    """Encode with the STS1 flight-software channel coding (tools/fw_encoder, see build.sh)."""
    inp = "".join(f.hex() + "\n" for f in frames)
    out = subprocess.run([str(FW_ENCODER)], input=inp, capture_output=True, text=True, check=True).stdout
    return [bytes.fromhex(line) for line in out.split()]


def make_burst(tm: bytes, baud: float, fs: float, preamble_bytes: int = 1, air: bytes | None = None) -> np.ndarray:
    air = air if air is not None else coding.encode_frame(tm)
    bits = np.unpackbits(np.frombuffer(b"\x55" * preamble_bytes + air, dtype=np.uint8))
    return gfsk_modulate(bits, baud, fs)


def synth_recording(
    frames: list[bytes],
    fs: float,
    baud: float = 9600,
    f_off: float = 0.0,
    snr_db: float = 10.0,
    gap_s: float = 0.3,
    seed: int = 0,
    firmware: bool = False,
) -> np.ndarray:
    """SNR measured in a bandwidth equal to the baud rate."""
    rng = np.random.default_rng(seed)
    parts = [np.zeros(int(gap_s * fs), np.complex64)]
    airs = encode_with_firmware(frames) if firmware else [None] * len(frames)
    for tm, air in zip(frames, airs):
        parts += [make_burst(tm, baud, fs, air=air), np.zeros(int(gap_s * fs), np.complex64)]
    x = np.concatenate(parts)
    x *= np.exp(2j * np.pi * f_off / fs * np.arange(len(x))).astype(np.complex64)
    noise_p = 10 ** (-snr_db / 10) * fs / baud
    x += (rng.normal(0, np.sqrt(noise_p / 2), len(x)) + 1j * rng.normal(0, np.sqrt(noise_p / 2), len(x))).astype(
        np.complex64
    )
    return x


def main(argv=None):
    """Write a synthetic pass directory (recording.bin + info.json) for testing."""
    import argparse
    import json

    ap = argparse.ArgumentParser(description=main.__doc__)
    ap.add_argument("out_dir")
    ap.add_argument("--samp-rate", type=float, default=1024000)
    ap.add_argument("--baud", type=float, default=9600)
    ap.add_argument("--f-off", type=float, default=0.0)
    ap.add_argument("--snr-db", type=float, default=10.0)
    ap.add_argument("--n-frames", type=int, default=5)
    ap.add_argument("--encoder", choices=["firmware", "python"], default="firmware" if FW_ENCODER.exists() else "python")
    a = ap.parse_args(argv)
    rng = np.random.default_rng(42)
    frames = []
    for i in range(a.n_frames):
        hdr = bytes([0x12, 0x30, i & 0xFF, i & 0xFF, 0x18, 0x00])
        frames.append(hdr + rng.integers(0, 256, coding.RS_K - 6, dtype=np.uint8).tobytes())
    x = synth_recording(frames, a.samp_rate, a.baud, a.f_off, a.snr_db, firmware=a.encoder == "firmware")
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    x.astype(np.complex64).tofile(out / "recording.bin")
    info = {"satellite": {"name": "SpaceTeamSat-1 (synthetic)", "frequency": 437395000.0, "sample_rate": a.samp_rate}}
    (out / "info.json").write_text(json.dumps(info, indent=2))
    (out / "frames.hex").write_text("\n".join(f.hex() for f in frames) + "\n")
    print(f"[{a.encoder} encoder] wrote {len(frames)} frames, {len(x) / a.samp_rate:.2f}s to {out}")


if __name__ == "__main__":
    main()
