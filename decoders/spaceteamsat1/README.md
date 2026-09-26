# sts1-decoder — SpaceTeamSat-1 (STS1) downlink tools

Tools to find and decode SpaceTeamSat-1 frames in groundstation IQ recordings
(`recording.bin` = complex64 + `info.json`).

## Downlink format (from the flight software, STS1_COBC_SW)

| layer | value |
|---|---|
| frequency | 437.395 MHz (IARU coordinated) |
| modulation | 2GFSK, Si4463, h≈0.5 (9600 Bd → 2.4 kHz deviation); rates 1200…115200 possible, beacon 9600 |
| preamble | `ceil(500 µs · baud / 8)` bytes of 0x55 (1 byte at 9600), no radio sync word/CRC/whitening |
| coding | TM frame 223 B → RS(255,223) CCSDS **dual basis** (libfec `encode_rs_ccsds`) → CCSDS scrambler → prepend ASM `1ACFFC1D` → conv. r=1/2 k=7 (0o171, 0o133 inverted), flushed → 520 B on air |
| framing | CCSDS TM transfer frame, spacecraft ID 0x123 |
| beacon | telemetry every 30 s |

gr-satellites calls this "CCSDS Concatenated" (see `satyaml/SPACETEAMSAT1.yml`,
from [gr-satellites#783](https://github.com/daniestevez/gr-satellites/discussions/783)).

Orbit: SatNOGS/Space-Track track STS1 as analyst object 99416, which is
CelesTrak **100609 (2026-203A, "OBJECT A")**, not 2026-203D.

## Setup

```sh
uv sync
podman build -t localhost/grsat:latest ../gr-satellites   # gr-satellites image (or pull the ghcr one)
tools/fw_encoder/build.sh                                 # optional: flight-software encoder
```

## Tools

| command | what it does |
|---|---|
| `sts1-survey REC` | high time-resolution waterfall + burst list (the pipeline waterfall averages away 0.4 s bursts) |
| `sts1-grsat REC [--baud ..] [--f-off ..]` | run **gr-satellites** (reference decoder) in podman, optional shift/decimate, parse KISS → TM frames |
| `sts1-decode REC [--baud ..] [--f-span ..]` | Python path: FM demod + coded-ASM correlation over a grid of offsets/baud rates, soft Viterbi, RS. ~3 dB more sensitive than gr-satellites on STS1's 1-byte preamble |
| `sts1-retrack PASS_DIR --norad N --out DIR` | redo doppler correction for a different object (residual d_new − d_old), verified against `doppler.txt` |
| `sts1-synth DIR` | synthetic STS1 recording, encoded with the flight software's own channel coding (`tools/fw_encoder`) |

`REC` is a pass directory or a `recording.bin` (+ `--info info.json`).
Decompress first: `zstd -d recording.bin.zst -o /some/scratch/recording.bin`.
`sts1-retrack` needs `LOCATION_LAT/LON/ALT` (e.g. `set -a; . ../../.env`).

## Validation

* `coding.encode_frame` is bit-identical to the flight software encoder (`tools/fw_encoder`).
* gr-satellites with `satyaml/SPACETEAMSAT1.yml` decodes synthetic firmware-encoded
  recordings (20/20 at 20 dB, 19/20 at 12 dB, 7/20 at 9 dB SNR in 9.6 kHz);
  `sts1-decode` gets 20/20 at 9 dB.
