"""Check SatNOGS for signs of life from STS1.

* SatNOGS DB: satellite entry (NORAD id, follow id, reception status) and the
  TLE, matched against CelesTrak's 2026-203 objects to see which one it is.
* SatNOGS Network: observations of the last N hours, their status /
  waterfall vetting, and their demodulated data. Demod files are checked for
  real STS1 content (CCSDS TM frame with SCID 0x123, or the ASM) instead of the
  short noise fragments the generic deframer emits.

Observations already reported are remembered in a state file, so each run
lists what is new. Last stdout line:
  SATNOGS new_obs=<n> good=<n> with_signal=<n> sts1_frames=<n> norad=<id> matches=<celestrak id|ambiguous(a|b)> reception=<status>
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import numpy as np
import requests

from .tm import tm_header

SAT_ID = "GCRL-7329-1908-5510-3384"
NET = "https://network.satnogs.org/api/observations/"
DB = "https://db.satnogs.org/api"
ASM = bytes.fromhex("1ACFFC1D")
MATCH_MARGIN = 3.0  # nearest object must be this many times closer than the 2nd


def get_json(url: str, **params):
    r = requests.get(url, params={"format": "json", **params}, timeout=30)
    r.raise_for_status()
    return r.json(), r.links.get("next", {}).get("url")


def recent_observations(hours: float) -> list[dict]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)
    obs, url, params = [], NET, {"sat_id": SAT_ID}
    for _ in range(20):  # 25 per page
        page, nxt = get_json(url, **params)
        for o in page:
            start = dt.datetime.fromisoformat(o["start"].replace("Z", "+00:00"))
            if start < cutoff:
                return obs
            if o.get("status") != "future" and start <= dt.datetime.now(dt.timezone.utc):
                obs.append(o)
        if not nxt:
            break
        url, params = nxt, {}
    return obs


def check_demod(data: bytes) -> list[str]:
    """Return descriptions of STS1-looking content in a demod file."""
    hits = []
    if len(data) >= 6 and tm_header(data)["scid"] == 0x123 and tm_header(data)["version"] == 0:
        hits.append(f"TM frame SCID 0x123 len={len(data)}")
    if ASM in data:
        hits.append(f"ASM at offset {data.index(ASM)}")
    return hits


def match_celestrak(tle1: str, tle2: str) -> list[tuple[str, float]] | None:
    """CelesTrak 2026-203 objects sorted by distance at the SatNOGS TLE epoch (closest first).

    STS1 and FramSat-1 fly within a few km of each other, so look at the margin.
    """
    try:
        from skyfield.api import EarthSatellite, load

        ts = load.timescale()
        ref = EarthSatellite(tle1, tle2, "satnogs", ts)
        objs, _ = get_json("https://celestrak.org/NORAD/elements/gp.php", INTDES="2026-203", FORMAT="json")
        t = ref.epoch
        p = ref.at(t).position.km
        return sorted(
            ((f"{o['NORAD_CAT_ID']} {o['OBJECT_ID']} {o['OBJECT_NAME']}",
              float(np.linalg.norm(EarthSatellite.from_omm(ts, o).at(t).position.km - p))) for o in objs),
            key=lambda x: x[1],
        )
    except Exception as e:  # noqa: BLE001 - informational only
        print(f"celestrak match failed: {e}")
        return None


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=48)
    ap.add_argument("--state", default=str(Path.home() / ".cache/sts1-nightly/satnogs_seen.txt"))
    ap.add_argument("--out", default=str(Path.home() / ".cache/sts1-nightly/satnogs"))
    a = ap.parse_args(argv)
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    state = Path(a.state)
    seen = set(state.read_text().split()) if state.exists() else set()

    sat, _ = get_json(f"{DB}/satellites/", sat_id=SAT_ID)
    sat = sat[0]
    tle, _ = get_json(f"{DB}/tle/", sat_id=SAT_ID)
    tle = tle[0] if tle else None
    print(f"DB: norad={sat['norad_cat_id']} follow={sat['norad_follow_id']} status={sat['status']} "
          f"reception={sat.get('reception_status')} evidence={sat.get('reception_evidence', {}).get('urls')}")
    match = None
    if tle:
        print(f"TLE ({tle['tle_source']}, updated {tle['updated']}): {tle['tle1']}")
        match = match_celestrak(tle["tle1"], tle["tle2"])
        if match:
            print("  closest CelesTrak 2026-203 objects at TLE epoch: "
                  + ", ".join(f"{m[0]} ({m[1]:.1f} km)" for m in match[:3]))

    obs = recent_observations(a.hours)
    new = [o for o in obs if str(o["id"]) not in seen]
    good = [o for o in obs if o.get("status") == "good"]
    with_signal = [o for o in obs if o.get("waterfall_status") == "with-signal"]
    frames = []
    for o in new:
        for d in o.get("demoddata") or []:
            url = d["payload_demod"]
            try:
                data = requests.get(url, timeout=30).content
            except requests.RequestException:
                continue
            hits = check_demod(data)
            if hits:
                dst = out / str(o["id"])
                dst.mkdir(exist_ok=True)
                (dst / Path(url).name).write_bytes(data)
                frames.append({"obs": o["id"], "url": url, "len": len(data), "hits": hits, "hex": data[:64].hex()})

    print(f"Network: {len(obs)} observations in last {a.hours:.0f} h, {len(new)} new")
    for o in new:
        n_demod = len(o.get("demoddata") or [])
        print(f"  {o['id']} {o['start'][:16]} {o['station_name'][:28]:28s} el={o.get('max_altitude')} "
              f"status={o.get('status')} waterfall={o.get('waterfall_status')} demod={n_demod} "
              f"https://network.satnogs.org/observations/{o['id']}/")
    for f in frames:
        print(f"  STS1 content in obs {f['obs']}: {f['hits']} {f['hex']}…")

    (out / f"{dt.date.today()}.json").write_text(json.dumps(
        {"satellite": sat, "tle": tle, "celestrak_match": match, "new_observations": [o["id"] for o in new],
         "good": [o["id"] for o in good], "with_signal": [o["id"] for o in with_signal], "sts1_frames": frames},
        indent=1))
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text("\n".join(sorted(seen | {str(o["id"]) for o in obs})) + "\n")
    # A different object only counts when the match is clear: STS1 flies close to
    # FramSat-1, and an old SatNOGS TLE can end up between the two (2026-10-04).
    if match and len(match) > 1 and match[0][1] * MATCH_MARGIN <= match[1][1]:
        matches = match[0][0].split()[0]
    elif match:
        matches = "ambiguous(" + "|".join(m[0].split()[0] for m in match[:2]) + ")"
    else:
        matches = "?"
    print(f"SATNOGS new_obs={len(new)} good={len([o for o in new if o.get('status') == 'good'])} "
          f"with_signal={len([o for o in new if o.get('waterfall_status') == 'with-signal'])} "
          f"sts1_frames={len(frames)} norad={sat['norad_cat_id']} "
          f"matches={matches} reception={sat.get('reception_status')}")


if __name__ == "__main__":
    main()
