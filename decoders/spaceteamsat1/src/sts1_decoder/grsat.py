"""Run gr-satellites (in podman) on a recording with the STS1 satellite description.

gr-satellites does the real work (FSK demod, clock recovery, Viterbi, CCSDS
deframing, RS). This wrapper adds what it does not do by itself:

* optional frequency offsets (residual doppler / wrong TLE / offset radio),
* other baud rates the Si4463 can be commanded to (deviation = baud/4),
* parsing the KISS output into CCSDS TM frames.
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

import numpy as np

from .recording import Recording
from .tm import parse_kiss, describe_tm

HERE = Path(__file__).resolve().parents[2]
SATYAML = HERE / "satyaml" / "SPACETEAMSAT1.yml"
DEFAULT_IMAGE = "localhost/grsat:latest"  # built from ../gr-satellites


def make_yaml(baud: int, dst: Path) -> Path:
    txt = SATYAML.read_text()
    txt = txt.replace("baudrate: 9600", f"baudrate: {baud}").replace("deviation: 2400", f"deviation: {baud // 4}")
    txt = txt.replace("9k6 FSK CCSDS downlink", f"{baud} FSK CCSDS downlink")
    dst.write_text(txt)
    return dst


def shift_decimate(rec: Recording, f_off: float, min_rate: float, dst: Path, chunk_s: float = 10.0) -> float:
    """Write a frequency-shifted, integer-decimated complex64 copy (saves gr-satellites CPU).

    Stateful across chunks (continuous phase and filter state). Returns the output rate.
    """
    from scipy import signal

    dec = max(1, int(rec.samp_rate // min_rate))
    out_rate = rec.samp_rate / dec
    taps = signal.firwin(16 * dec + 1, 0.4 * out_rate, fs=rec.samp_rate) if dec > 1 else np.array([1.0])
    zi = np.zeros(len(taps) - 1, dtype=np.complex128)
    n_chunk = int(chunk_s * rec.samp_rate) // dec * dec
    phase = 0.0
    w = -2 * np.pi * f_off / rec.samp_rate
    iq = rec.iq
    with open(dst, "wb") as f:
        for a in range(0, len(iq), n_chunk):
            x = np.asarray(iq[a : a + n_chunk])
            x = x * np.exp(1j * (phase + w * np.arange(len(x))))
            phase = (phase + w * len(x)) % (2 * np.pi)
            y, zi = signal.lfilter(taps, 1.0, x, zi=zi)
            f.write(y[::dec].astype(np.complex64).tobytes())
    return out_rate


def run(
    rec: Recording,
    out_dir: Path,
    baud: int = 9600,
    f_off: float = 0.0,
    image: str = DEFAULT_IMAGE,
    extra: list[str] | None = None,
    decimate: bool = True,
) -> list[bytes]:
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = f"b{baud}_f{int(f_off):+d}"
    with tempfile.TemporaryDirectory(dir=out_dir) as tmp:
        tmp = Path(tmp)
        yml = make_yaml(baud, tmp / "sts1.yml")
        if decimate or f_off:
            data = tmp / "iq.cf32"
            samp_rate = shift_decimate(rec, f_off, max(48000, 5 * baud), data)
        else:
            data, samp_rate = rec.path, rec.samp_rate
        kiss = out_dir / f"frames_{tag}.kiss"
        cmd = [
            "podman", "run", "--rm",
            "-v", f"{data.parent}:/in:ro,z",
            "-v", f"{tmp}:/cfg:ro,z",
            "-v", f"{out_dir}:/out:z",
            "--entrypoint", "gr_satellites",
            image,
            "/cfg/sts1.yml",
            "--rawfile", f"/in/{data.name}",
            "--samp_rate", str(samp_rate),
            "--iq",
            "--kiss_out", f"/out/{kiss.name}",
            "--hexdump",
            *(extra or []),
        ]
        log = out_dir / f"gr_satellites_{tag}.log"
        with open(log, "w") as lf:
            subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, check=False)
    return parse_kiss(kiss.read_bytes()) if kiss.exists() else []


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("recording", help="recording.bin or pass directory")
    ap.add_argument("--info")
    ap.add_argument("--out", default="grsat_out")
    ap.add_argument("--baud", type=int, nargs="+", default=[9600])
    ap.add_argument("--f-off", type=float, nargs="+", default=[0.0], help="Hz; signal assumed at center+f_off")
    ap.add_argument("--image", default=DEFAULT_IMAGE)
    ap.add_argument("--no-decimate", action="store_true")
    a, extra = ap.parse_known_args(argv)
    rec = Recording.open(a.recording, a.info)
    for baud in a.baud:
        for f in a.f_off:
            frames = run(rec, Path(a.out), baud, f, a.image, extra, not a.no_decimate)
            print(f"baud={baud} f_off={f:+.0f}: {len(frames)} frames")
            for fr in frames:
                print("  " + describe_tm(fr))


if __name__ == "__main__":
    main()
