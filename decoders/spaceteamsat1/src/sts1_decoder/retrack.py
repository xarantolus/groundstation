"""Re-do the doppler correction of a recording for a different orbit.

The groundstation corrected the recording with the doppler of the object in
info.json (pass.omm). If that was the wrong object (e.g. 2026-203D instead of
203A for STS1), the satellite's signal carries a residual doppler
d_new(t) - d_old(t). This tool computes both tracks exactly like
auto/doppler.py (skyfield range rate, times relative to the recorder anchor
= pass start - 30 s), verifies d_old against the doppler.txt that was
actually used, and writes a re-corrected recording.

Station location comes from LOCATION_LAT/LON/ALT (e.g. the groundstation .env).
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
from pathlib import Path

import numpy as np
import requests
from skyfield.api import EarthSatellite, load, wgs84

from .recording import Recording

C = 299_792_458.0
RECORDING_LEAD_SECONDS = 30  # auto/scheduler.py


def fetch_omm(norad: int) -> dict:
    r = requests.get(f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad}&FORMAT=json", timeout=15)
    r.raise_for_status()
    return r.json()[0]


def doppler_track(omm: dict, topos, t_utc0: datetime.datetime, rel_s: np.ndarray, f0: float) -> np.ndarray:
    ts = load.timescale()
    sat = EarthSatellite.from_omm(ts, omm)
    t = ts.tt_jd(ts.from_datetime(t_utc0).tt + rel_s / 86400.0)
    rel = (sat - topos).at(t)
    pos, vel = rel.position.km, rel.velocity.km_per_s
    rr = np.sum(pos * vel, axis=0) / np.linalg.norm(pos, axis=0) * 1000.0
    return -rr / C * f0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pass_dir", help="directory with info.json and doppler.txt")
    ap.add_argument("--recording", help="decompressed recording.bin (default: pass_dir/recording.bin)")
    ap.add_argument("--norad", type=int, help="object to re-track to (fetched from CelesTrak)")
    ap.add_argument("--omm-json", help="or: OMM json file for the new object")
    ap.add_argument("--out", required=True, help="output directory (recording.bin + info.json)")
    ap.add_argument("--time-offset", type=float, default=0.0, help="extra s added to recording time base")
    ap.add_argument("--dry-run", action="store_true", help="only print the residual doppler")
    a = ap.parse_args(argv)

    pass_dir = Path(a.pass_dir)
    info = json.loads((pass_dir / "info.json").read_text())
    f0 = float(info["satellite"]["frequency"])
    old_omm = info["pass"]["omm"]
    new_omm = json.loads(Path(a.omm_json).read_text()) if a.omm_json else fetch_omm(a.norad)
    if isinstance(new_omm, list):
        new_omm = new_omm[0]
    topos = wgs84.latlon(float(os.environ["LOCATION_LAT"]), float(os.environ["LOCATION_LON"]),
                         elevation_m=float(os.environ["LOCATION_ALT"]))
    # naive datetimes in info.json are station-local time (PassInfo convention)
    start_local = datetime.datetime.fromisoformat(info["pass"]["start_time"])
    anchor = (start_local - datetime.timedelta(seconds=RECORDING_LEAD_SECONDS)).astimezone(datetime.timezone.utc)

    dop = np.loadtxt(pass_dir / "doppler.txt")
    rel = dop[:, 0]
    d_old = doppler_track(old_omm, topos, anchor, rel, f0)
    err = d_old - dop[:, 1]
    print(f"old object {old_omm['OBJECT_ID']} ({old_omm['NORAD_CAT_ID']}): recomputed vs doppler.txt "
          f"max |err| = {np.max(np.abs(err)):.2f} Hz")
    d_new = doppler_track(new_omm, topos, anchor, rel, f0)
    resid = d_new - d_old
    print(f"new object {new_omm['OBJECT_ID']} ({new_omm['NORAD_CAT_ID']}): residual doppler "
          f"min {resid.min():+.0f} Hz, max {resid.max():+.0f} Hz")
    for s in range(0, len(rel), max(1, len(rel) // 16)):
        print(f"   t={rel[s]:7.1f}s  old={d_old[s]:+8.0f}  new={d_new[s]:+8.0f}  residual={resid[s]:+7.0f} Hz")
    if a.dry_run:
        return

    rec = Recording.open(a.recording or pass_dir, pass_dir / "info.json")
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    fs = rec.samp_rate
    iq = rec.iq
    phase = 0.0
    chunk = int(10 * fs)
    with open(out / "recording.bin", "wb") as f:
        for s0 in range(0, len(iq), chunk):
            x = np.asarray(iq[s0 : s0 + chunk])
            t = (s0 + np.arange(len(x))) / fs + a.time_offset
            fr = np.interp(t, rel, resid)
            ph = phase - 2 * np.pi * np.cumsum(fr) / fs
            phase = float(ph[-1])
            f.write((x * np.exp(1j * ph)).astype(np.complex64).tobytes())
    info["pass"]["omm_original"] = old_omm
    info["pass"]["omm"] = new_omm
    info["retracked"] = {"from": old_omm["NORAD_CAT_ID"], "to": new_omm["NORAD_CAT_ID"], "time_offset": a.time_offset}
    (out / "info.json").write_text(json.dumps(info, indent=2))
    shutil.copy(pass_dir / "doppler.txt", out / "doppler_original.txt")
    print(f"wrote {out / 'recording.bin'}")


if __name__ == "__main__":
    main()
