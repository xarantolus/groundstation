#!/usr/bin/env python3
"""Streaming IQ waterfall plotter."""
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


BYTES_PER_COMPLEX = 8


def _plan(total_complex: int, nfft: int, target_rows: int) -> Tuple[int, int]:
    """Pick (n_rows, nfft_per_row) so the spectrogram is ~target_rows tall."""
    nfft_per_row = max(1, total_complex // (nfft * target_rows))
    n_rows = total_complex // (nfft * nfft_per_row)
    return n_rows, nfft_per_row


def compute_spectrogram(
    input_file: str,
    samp_rate: float,
    nfft: int = 4096,
    target_rows: int = 1500,
) -> Tuple[np.ndarray, int, float]:
    """Stream the IQ file and return (spec_db, nfft_per_row, pass_seconds)."""
    file_bytes = os.path.getsize(input_file)
    total_complex = file_bytes // BYTES_PER_COMPLEX
    if total_complex < nfft:
        raise SystemExit(
            f"input too small: {total_complex} complex samples, need >= {nfft}"
        )

    n_rows, nfft_per_row = _plan(total_complex, nfft, target_rows)
    samples_per_row_f32 = 2 * nfft * nfft_per_row

    window = np.hanning(nfft).astype(np.float32)
    win_norm = float(np.sum(window * window))

    spec = np.empty((n_rows, nfft), dtype=np.float32)

    with open(input_file, "rb") as f:
        for row in range(n_rows):
            raw = np.fromfile(f, dtype=np.float32, count=samples_per_row_f32)
            if raw.size < samples_per_row_f32:
                spec = spec[:row]
                n_rows = row
                break
            iq = raw.view(np.complex64).reshape(nfft_per_row, nfft)
            spectrum = np.fft.fftshift(np.fft.fft(iq * window, axis=1), axes=1)
            power = spectrum.real * spectrum.real + spectrum.imag * spectrum.imag
            avg = power.mean(axis=0) / win_norm
            spec[row] = 10.0 * np.log10(np.maximum(avg, 1e-20))

    pass_seconds = n_rows * nfft_per_row * nfft / samp_rate
    return spec, nfft_per_row, pass_seconds


def render(
    spec: np.ndarray,
    output_path: str,
    samp_rate: float,
    center_freq: float,
    bandwidth: float,
    pass_seconds: float,
    cmap: str = "viridis",
    baseline: bool = True,
    pct_lo: float = 5.0,
    pct_hi: float = 99.5,
) -> Tuple[float, float]:
    """Save the waterfall PNG. Returns the (vmin, vmax) that were used."""
    nfft = spec.shape[1]
    half_hz = samp_rate / 2

    if center_freq:
        xmin = (center_freq - half_hz) / 1e6
        xmax = (center_freq + half_hz) / 1e6
        xlabel = "Frequency (MHz)"
    else:
        xmin = -half_hz / 1e3
        xmax = half_hz / 1e3
        xlabel = "Frequency (kHz)"

    if baseline:
        display = spec - np.median(spec, axis=0, keepdims=True)
        cbar_label = "Power above noise floor (dB)"
    else:
        display = spec
        cbar_label = "Power (dB)"

    vmin = float(np.percentile(display, pct_lo))
    vmax = float(np.percentile(display, pct_hi))
    if vmax <= vmin:
        vmax = vmin + 1.0

    plt.figure(figsize=(10, 20))
    plt.imshow(
        display,
        origin="lower",
        aspect="auto",
        interpolation="None",
        extent=[xmin, xmax, 0.0, pass_seconds],
        vmin=vmin,
        vmax=vmax,
        cmap=cmap,
    )
    plt.xlabel(xlabel)
    plt.ylabel("Time (seconds)")

    title_parts = []
    if center_freq:
        title_parts.append(f"Center {center_freq / 1e6:.4f} MHz")
    bw_for_title = bandwidth if bandwidth else samp_rate
    title_parts.append(f"BW {bw_for_title / 1e3:.1f} kHz")
    title_parts.append(f"nfft={nfft}")
    plt.title(" | ".join(title_parts))

    cbar = plt.colorbar(aspect=50)
    cbar.set_label(cbar_label)

    plt.savefig(output_path, bbox_inches="tight", dpi=100)
    plt.close()
    return vmin, vmax


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input_file", help="interleaved float32 IQ file (gr_complex)")
    ap.add_argument("output_file", help="output PNG path")
    ap.add_argument("--samp-rate", type=float, required=True,
                    help="complex sample rate of input, in samples/sec")
    ap.add_argument("--center-freq", type=float, default=0.0,
                    help="center frequency in Hz (title only; axis is relative)")
    ap.add_argument("--bandwidth", type=float, default=0.0,
                    help="recorded bandwidth in Hz (title only)")
    ap.add_argument("--nfft", type=int, default=4096,
                    help="FFT size per spectrum frame (default 4096)")
    ap.add_argument("--target-rows", type=int, default=1500,
                    help="approximate number of time rows in the output")
    ap.add_argument("--cmap", default="viridis",
                    help="matplotlib colormap (default viridis; try inferno, magma, turbo)")
    ap.add_argument("--no-baseline", action="store_true",
                    help="disable per-bin median subtraction (show raw dB)")
    ap.add_argument("--pct-lo", type=float, default=5.0,
                    help="lower percentile for color floor (default 5)")
    ap.add_argument("--pct-hi", type=float, default=99.5,
                    help="upper percentile for color ceiling (default 99.5)")
    args = ap.parse_args(argv)

    spec, nfft_per_row, pass_seconds = compute_spectrogram(
        args.input_file,
        args.samp_rate,
        nfft=args.nfft,
        target_rows=args.target_rows,
    )
    vmin, vmax = render(
        spec,
        args.output_file,
        samp_rate=args.samp_rate,
        center_freq=args.center_freq,
        bandwidth=args.bandwidth,
        pass_seconds=pass_seconds,
        cmap=args.cmap,
        baseline=not args.no_baseline,
        pct_lo=args.pct_lo,
        pct_hi=args.pct_hi,
    )
    print(
        f"waterfall: {args.output_file} "
        f"shape={spec.shape} rows_avg={nfft_per_row} "
        f"duration={pass_seconds:.1f}s "
        f"vmin={vmin:.1f}dB vmax={vmax:.1f}dB",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
