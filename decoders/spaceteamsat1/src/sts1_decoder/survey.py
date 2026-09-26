"""Find where (time/frequency) bursts are in a recording.

Produces a high time-resolution waterfall (the pipeline waterfall averages
over seconds, which hides ~0.4 s long 9k6 bursts) and a list of burst
candidates: time spans where the band power around some frequency rises
clearly above that bin's median noise level.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from .recording import Recording


def stft_power(rec: Recording, res_hz: float = 1000.0, row_s: float = 0.05, t0: float = 0, t1: float | None = None):
    fs = rec.samp_rate
    nfft = int(2 ** np.round(np.log2(fs / res_hz)))
    per_row = max(1, int(row_s * fs / nfft))
    iq = rec.iq
    s0 = int(t0 * fs)
    s1 = min(len(iq), int((t1 if t1 else rec.duration) * fs))
    win = np.hanning(nfft).astype(np.float32)
    rows = []
    block = nfft * per_row * 200
    for a in range(s0, s1 - nfft * per_row, block):
        x = np.asarray(iq[a : min(a + block, s1)])
        n = len(x) // (nfft * per_row)
        if n == 0:
            break
        x = x[: n * nfft * per_row].reshape(n * per_row, nfft) * win
        p = np.abs(np.fft.fftshift(np.fft.fft(x, axis=1), axes=1)) ** 2
        rows.append(p.reshape(n, per_row, nfft).mean(axis=1))
    P = np.concatenate(rows).astype(np.float32)
    freqs = np.fft.fftshift(np.fft.fftfreq(nfft, 1 / fs))
    times = t0 + np.arange(len(P)) * per_row * nfft / fs
    return times, freqs, P


def find_bursts(times, freqs, P, bw_hz=15000.0, thresh_db=3.0, min_len_s=0.1):
    """Band-power (sliding bw) above per-band median; returns list of dicts."""
    df = freqs[1] - freqs[0]
    k = max(1, int(bw_hz / df))
    kern = np.ones(k) / k
    B = np.apply_along_axis(lambda r: np.convolve(r, kern, mode="same"), 1, P)
    Bdb = 10 * np.log10(B / np.median(B, axis=0, keepdims=True))
    best_f = np.argmax(Bdb, axis=1)
    peak = Bdb[np.arange(len(Bdb)), best_f]
    on = peak > thresh_db
    bursts = []
    i = 0
    dt = times[1] - times[0]
    while i < len(on):
        if on[i]:
            j = i
            while j < len(on) and on[j]:
                j += 1
            if (j - i) * dt >= min_len_s:
                seg = Bdb[i:j].mean(axis=0)
                fi = int(np.argmax(seg))
                bursts.append(
                    {"t0": float(times[i]), "t1": float(times[j - 1] + dt), "f": float(freqs[fi]), "snr_db": float(seg[fi])}
                )
            i = j
        else:
            i += 1
    return bursts


def plot(times, freqs, P, out: Path, title: str, fmin=None, fmax=None):
    sel = np.ones(len(freqs), bool)
    if fmin is not None:
        sel &= (freqs >= fmin) & (freqs <= fmax)
    D = 10 * np.log10(P[:, sel] / np.median(P[:, sel]))
    h = min(60, max(8, len(times) / 150))
    fig, ax = plt.subplots(figsize=(14, h))
    ax.imshow(
        D, aspect="auto", origin="lower", vmin=-3, vmax=15, cmap="viridis",
        extent=[freqs[sel][0] / 1e3, freqs[sel][-1] / 1e3, times[0], times[-1]],
    )
    ax.set_xlabel("offset from center (kHz)")
    ax.set_ylabel("time (s)")
    ax.set_title(title)
    fig.savefig(out, dpi=80, bbox_inches="tight")
    plt.close(fig)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("recording", help="recording.bin or pass directory")
    ap.add_argument("--info", help="info.json (default: next to recording)")
    ap.add_argument("--out", default="survey_out")
    ap.add_argument("--t0", type=float, default=0)
    ap.add_argument("--t1", type=float, default=None)
    ap.add_argument("--zoom-khz", type=float, default=60)
    ap.add_argument("--row-s", type=float, default=0.05)
    ap.add_argument("--thresh-db", type=float, default=3.0)
    a = ap.parse_args(argv)
    rec = Recording.open(a.recording, a.info)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    print(f"{rec.path}: fs={rec.samp_rate} center={rec.center_freq} duration={rec.duration:.1f}s")
    times, freqs, P = stft_power(rec, row_s=a.row_s, t0=a.t0, t1=a.t1)
    np.savez_compressed(out / "stft.npz", times=times, freqs=freqs, P=P)
    plot(times, freqs, P, out / "waterfall_full.png", f"{rec.path.parent.name} full band")
    z = a.zoom_khz * 1e3
    plot(times, freqs, P, out / "waterfall_zoom.png", f"{rec.path.parent.name} +-{a.zoom_khz} kHz", -z, z)
    # mean spectrum + max-hold
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.plot(freqs / 1e3, 10 * np.log10(np.mean(P, 0)), label="mean")
    ax.plot(freqs / 1e3, 10 * np.log10(np.percentile(P, 99, axis=0)), label="99th pct")
    ax.legend()
    ax.set_xlabel("offset (kHz)")
    fig.savefig(out / "spectrum.png", dpi=80, bbox_inches="tight")
    plt.close(fig)
    bursts = find_bursts(times, freqs, P, thresh_db=a.thresh_db)
    with open(out / "bursts.txt", "w") as f:
        for b in bursts:
            line = f"{b['t0']:9.3f} {b['t1']:9.3f} dur={b['t1'] - b['t0']:6.3f}s f={b['f'] / 1e3:+8.2f}kHz snr={b['snr_db']:5.1f}dB"
            print(line)
            f.write(line + "\n")
    print(f"{len(bursts)} bursts; outputs in {out}")


if __name__ == "__main__":
    main()
