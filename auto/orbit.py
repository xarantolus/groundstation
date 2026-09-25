"""Orbit propagation on top of OMM (CCSDS Orbit Mean-Elements Message) data.

OMM is what CelesTrak serves as ``FORMAT=json``. Unlike TLE it has no
5-digit catalog-number limit, so it works for NORAD IDs >= 100000.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from skyfield.api import EarthSatellite, wgs84
from skyfield.timelib import Time, Timescale
from skyfield.api import load

Omm = Dict[str, Any]

C_M_S = 299_792_458.0
_ts: Optional[Timescale] = None


def timescale() -> Timescale:
    global _ts
    if _ts is None:
        _ts = load.timescale()
    return _ts


def _implied_decimal(s: str) -> float:
    """Parse TLE '-11606-4' style fields (mantissa with implied leading '0.')."""
    s = s.strip()
    if not s:
        return 0.0
    sign = -1.0 if s[0] == "-" else 1.0
    s = s.lstrip("+-")
    mant, exp = s[:-2], s[-2:]
    return sign * float("0." + mant.strip()) * 10.0 ** int(exp)


def tle_to_omm(tle1: str, tle2: str, name: Optional[str] = None) -> Omm:
    """Convert a legacy TLE pair to an OMM field dict (5-digit catnr only)."""
    yy = int(tle1[18:20])
    year = 2000 + yy if yy < 57 else 1900 + yy
    epoch = datetime.datetime(year, 1, 1) + datetime.timedelta(
        days=float(tle1[20:32]) - 1
    )
    return {
        "OBJECT_NAME": name or tle1[2:7].strip(),
        "OBJECT_ID": tle1[9:17].strip(),
        "EPOCH": epoch.isoformat(timespec="microseconds"),
        "MEAN_MOTION": float(tle2[52:63]),
        "ECCENTRICITY": float("0." + tle2[26:33].strip()),
        "INCLINATION": float(tle2[8:16]),
        "RA_OF_ASC_NODE": float(tle2[17:25]),
        "ARG_OF_PERICENTER": float(tle2[34:42]),
        "MEAN_ANOMALY": float(tle2[43:51]),
        "EPHEMERIS_TYPE": int(tle1[62].strip() or 0),
        "CLASSIFICATION_TYPE": tle1[7],
        "NORAD_CAT_ID": int(tle1[2:7]),
        "ELEMENT_SET_NO": int(tle1[64:68]),
        "REV_AT_EPOCH": int(tle2[63:68]),
        "BSTAR": _implied_decimal(tle1[53:61]),
        "MEAN_MOTION_DOT": float(tle1[33:43]),
        "MEAN_MOTION_DDOT": _implied_decimal(tle1[44:52]),
    }


def parse_omm_json(text: Optional[str]) -> Optional[Omm]:
    """Extract the first OMM record from a CelesTrak JSON response, or None."""
    import json

    try:
        data = json.loads(text or "")
    except ValueError:
        return None
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return None
    omm = data[0]
    if "EPOCH" not in omm or "NORAD_CAT_ID" not in omm:
        return None
    return omm


def make_satellite(omm: Omm) -> EarthSatellite:
    return EarthSatellite.from_omm(timescale(), omm)


def observer(lat: float, lon: float, alt_m: float):
    return wgs84.latlon(lat, lon, elevation_m=alt_m)


def to_utc(dt: datetime.datetime) -> datetime.datetime:
    """Naive datetimes are local time (the PassInfo convention)."""
    return dt.astimezone(datetime.timezone.utc)


def to_local_naive(t: Time) -> datetime.datetime:
    return t.utc_datetime().astimezone().replace(tzinfo=None)


def altaz(sat: EarthSatellite, topos, t: Time) -> Tuple[np.ndarray, np.ndarray]:
    """(elevation_deg, azimuth_deg) arrays of the satellite at ``t``."""
    alt, az, _ = (sat - topos).at(t).altaz()
    return np.atleast_1d(alt.degrees), np.atleast_1d(az.degrees)


def compute_azel(
    omm: Omm,
    lat: float,
    lon: float,
    alt_m: float,
    when: datetime.datetime,
) -> Tuple[float, float]:
    """(azimuth, elevation) in degrees at ``when`` (naive = local time)."""
    sat = make_satellite(omm)
    t = timescale().from_datetime(to_utc(when))
    el, az = altaz(sat, observer(lat, lon, alt_m), t)
    return float(az[0]), float(el[0])


def range_rate_m_s(
    sat: EarthSatellite, topos, t: Time
) -> np.ndarray:
    """Range rate in m/s (positive when receding) for each time in ``t``."""
    rel = (sat - topos).at(t)
    pos = rel.position.km
    vel = rel.velocity.km_per_s
    return np.sum(pos * vel, axis=0) / np.linalg.norm(pos, axis=0) * 1000.0
