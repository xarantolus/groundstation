"""(G)FSK demodulation and CCSDS sync search for STS1.

The Si4463 transmits 2GFSK with modulation index ~0.5 (9600 Bd -> 2.4 kHz
deviation). We use a non-coherent FM discriminator, integrate-and-dump style
matched filter, and find frames by correlating against the *convolutionally
encoded* ASM (the encoder always starts from state 0, so that 64-bit pattern
is fixed). Timing is taken from the correlation peak; a small Gardner-free
fine-timing search over sub-sample offsets picks the best eye opening.
"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction

import numpy as np
from scipy import signal

from . import coding

BAUD_RATES = (1200, 2400, 4800, 9600, 19200, 38400, 57600, 76800, 115200)


@dataclass
class Channel:
    """Output of `fm_demod`: frequency (Hz, DC removed) at `sps` samples/symbol."""

    freq: np.ndarray
    fs: float
    sps: int
    baud: float
    start_time: float  # seconds into the recording of freq[0]
    power: np.ndarray  # instantaneous power (same rate) for SNR bookkeeping


def resample(x: np.ndarray, fs_in: float, fs_out: float) -> np.ndarray:
    fr = Fraction(fs_out / fs_in).limit_denominator(1000)
    if fr == 1:
        return x
    return signal.resample_poly(x, fr.numerator, fr.denominator)


def fm_demod(
    iq: np.ndarray,
    fs: float,
    f_off: float,
    baud: float,
    sps: int = 8,
    bw: float | None = None,
    start_time: float = 0.0,
    dc_symbols: int = 256,
) -> Channel:
    n = np.arange(len(iq))
    x = iq * np.exp(-2j * np.pi * f_off / fs * n).astype(np.complex64)
    fs2 = baud * sps
    x = resample(x, fs, fs2)
    # channel filter: GFSK h=0.5 occupies ~ +-0.75*baud, allow some slack for residual offset
    bw = bw if bw is not None else 1.0 * baud
    taps = signal.firwin(8 * sps + 1, bw, fs=fs2)
    x = signal.lfilter(taps, 1.0, x)
    d = np.angle(x[1:] * np.conj(x[:-1])) * fs2 / (2 * np.pi)
    d = np.concatenate([[0.0], d])
    # matched filter (1 symbol boxcar)
    d = signal.lfilter(np.ones(sps) / sps, 1.0, d)
    # remove residual carrier offset
    w = dc_symbols * sps
    dc = signal.lfilter(np.ones(w) / w, 1.0, d)
    d = d - np.roll(dc, -w // 2)
    return Channel(d.astype(np.float32), fs2, sps, baud, start_time, np.abs(x).astype(np.float32) ** 2)


@dataclass
class SyncHit:
    index: int  # sample index in Channel.freq of the first coded symbol
    corr: float  # normalised correlation, sign = polarity
    conv: bool


def find_sync(ch: Channel, threshold: float = 0.55, conv: bool = True) -> list[SyncHit]:
    pattern = coding.ASM_CODED if conv else coding.ASM_PLAIN
    t = 2.0 * pattern.astype(np.float32) - 1.0
    L = len(t) * ch.sps
    tmpl = np.zeros(L, dtype=np.float32)
    tmpl[:: ch.sps] = t
    y = ch.freq
    c = signal.fftconvolve(y, tmpl[::-1], mode="valid")
    e = signal.fftconvolve(y * y, (tmpl != 0).astype(np.float32), mode="valid")
    cn = c / np.sqrt(np.maximum(e, 1e-12) * len(t))
    a = np.abs(cn)
    hits: list[SyncHit] = []
    cand = np.flatnonzero(a > threshold)
    if len(cand) == 0:
        return hits
    guard = L
    # greedy peak picking
    order = cand[np.argsort(-a[cand])]
    taken = np.zeros(len(a), dtype=bool)
    for i in order:
        if taken[i]:
            continue
        lo, hi = max(0, i - guard), min(len(a), i + guard)
        taken[lo:hi] = True
        hits.append(SyncHit(int(i), float(cn[i]), conv))
    return sorted(hits, key=lambda h: h.index)


def extract_soft(ch: Channel, hit: SyncHit, n_symbols: int) -> np.ndarray | None:
    """Sample symbols with sub-sample timing refinement and linear clock-drift fit."""
    sps = ch.sps
    if hit.index + n_symbols * sps + sps >= len(ch.freq):
        return None
    y = ch.freq * np.sign(hit.corr)
    k = np.arange(n_symbols)
    xs = np.arange(len(y))
    best, best_s = -1.0, None
    # search small timing offsets and clock-rate errors (+-200 ppm)
    for dt in np.linspace(-0.5, 0.5, 5):
        for ppm in (-200, -100, 0, 100, 200):
            pos = hit.index + dt + k * sps * (1 + ppm * 1e-6)
            s = np.interp(pos, xs, y)
            q = np.mean(np.abs(s)) / (np.std(np.abs(s)) + 1e-9)  # eye quality
            if q > best:
                best, best_s = q, s
    s = best_s
    return s / (np.mean(np.abs(s)) + 1e-12)


@dataclass
class FrameResult:
    time: float  # seconds into recording
    f_off: float
    baud: float
    corr: float
    frame: coding.DecodedFrame | None


def decode_channel(ch: Channel, f_off: float, threshold: float = 0.55, conv: bool = True) -> list[FrameResult]:
    out = []
    n_sym = coding.CODED_BITS if conv else coding.CADU_LEN * 8
    for hit in find_sync(ch, threshold, conv):
        soft = extract_soft(ch, hit, n_sym)
        if soft is None:
            continue
        fr = coding.decode_coded_soft(soft, conv)
        out.append(FrameResult(ch.start_time + hit.index / ch.fs, f_off, ch.baud, hit.corr, fr))
    return out
