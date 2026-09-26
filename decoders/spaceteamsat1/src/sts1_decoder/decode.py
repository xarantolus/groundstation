"""Brute-force STS1 frame search over a recording (Python demod path).

For each chunk of the recording, the signal is brought to a moderate
intermediate rate, then for every (baud rate, frequency offset) hypothesis
it is FM-demodulated and searched for the convolutionally encoded ASM.
Every sync hit is Viterbi/RS decoded. Only RS-valid frames are reported
as decoded; sync-only hits are listed too (they show *something* STS1-like
is there even if it didn't decode).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from scipy import signal

from . import demod
from .recording import Recording
from .tm import describe_tm


def to_intermediate(x: np.ndarray, fs: float, span_hz: float) -> tuple[np.ndarray, float]:
    dec = max(1, int(fs // (2.5 * span_hz)))
    if dec == 1:
        return x, fs
    return signal.decimate(x, dec, ftype="fir", zero_phase=False).astype(np.complex64), fs / dec


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("recording")
    ap.add_argument("--info")
    ap.add_argument("--out", default="decode_out")
    ap.add_argument("--baud", type=int, nargs="+", default=[9600])
    ap.add_argument("--f-span", type=float, default=30e3, help="search +- this offset (Hz)")
    ap.add_argument("--f-step", type=float, default=None, help="default: baud/3")
    ap.add_argument("--chunk-s", type=float, default=20.0)
    ap.add_argument("--t0", type=float, default=0.0)
    ap.add_argument("--t1", type=float, default=None)
    ap.add_argument("--threshold", type=float, default=0.55)
    ap.add_argument("--plain", action="store_true", help="also search un-convolved ASM (conv coding disabled)")
    a = ap.parse_args(argv)

    rec = Recording.open(a.recording, a.info)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    fs = rec.samp_rate
    iq = rec.iq
    t1 = a.t1 if a.t1 is not None else rec.duration
    frame_s = 530 * 8 / min(a.baud)
    step = int(a.chunk_s * fs)
    ov = int((frame_s + 0.2) * fs)
    results, seen = [], set()
    for s0 in range(int(a.t0 * fs), int(t1 * fs), step):
        x = np.asarray(iq[s0 : s0 + step + ov])
        tstart = s0 / fs
        max_bw = max(a.baud) * 1.5
        xi, fsi = to_intermediate(x, fs, a.f_span + max_bw)
        n_hits = n_ok = 0
        for baud in a.baud:
            fstep = a.f_step or baud / 3
            for f_off in np.arange(-a.f_span, a.f_span + 1, fstep):
                ch = demod.fm_demod(xi, fsi, f_off, baud, bw=0.8 * baud, start_time=tstart)
                for conv in (True, False) if a.plain else (True,):
                    for r in demod.decode_channel(ch, f_off, a.threshold, conv):
                        n_hits += 1
                        if r.frame is not None:
                            n_ok += 1
                            k2 = (r.frame.tm, round(r.time, 1))
                            if k2 in seen:
                                continue
                            seen.add(k2)
                        results.append(
                            {
                                "time": r.time,
                                "baud": baud,
                                "f_off": float(f_off),
                                "corr": r.corr,
                                "conv": conv,
                                "decoded": r.frame is not None,
                                "tm_hex": r.frame.tm.hex() if r.frame else None,
                                "rs_errors": r.frame.rs_errors if r.frame else None,
                                "channel_bit_errors": r.frame.conv_bit_errors if r.frame else None,
                            }
                        )
                        if r.frame is not None:
                            print(f"  t={r.time:8.3f}s baud={baud} f_off={f_off:+7.0f} corr={r.corr:+.2f} "
                                  f"rs={r.frame.rs_errors} {describe_tm(r.frame.tm)}")
        print(f"[{tstart:7.1f}s] sync hits={n_hits} decoded={n_ok}", flush=True)
    (out / "results.json").write_text(json.dumps(results, indent=1))
    ok = [r for r in results if r["decoded"]]
    print(f"{len(ok)} unique decoded frames, {len(results) - len(ok)} sync-only hits -> {out / 'results.json'}")


if __name__ == "__main__":
    main()
