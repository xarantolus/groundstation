from __future__ import annotations

import datetime
import logging
import os

import ephem

logger = logging.getLogger("groundstation.doppler")

# Speed of light in m/s. ephem.range_velocity is in m/s.
_C_M_S = 299_792_458.0

# gr-satellites' satellites_doppler_correction reads <unix_ts>\t<doppler_hz>
# pairs and interpolates between them. 0.1s matches the upstream
# tle_to_doppler_file.py default and is fine-grained enough for LEO Doppler
# without bloating the file (a 15-minute pass = ~9000 lines, ~200 KB).
DEFAULT_TIME_STEP_S = 0.1


def _to_utc(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.astimezone().astimezone(datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def write_doppler_file(
    *,
    tle1: str,
    tle2: str,
    sat_name: str,
    lat: float,
    lon: float,
    alt_m: float,
    f_carrier: float,
    anchor: datetime.datetime,
    start: datetime.datetime,
    end: datetime.datetime,
    output_path: str,
    time_step_s: float = DEFAULT_TIME_STEP_S,
) -> int:
    """Generate a gr-satellites Doppler correction file.

    Format matches gr-satellites' ``satellites_doppler_correction`` block:
    one ``<seconds_since_anchor>\\t<doppler_hz>`` line per sample. Storing
    relative seconds (rather than absolute unix timestamps) lets the same
    file correct a replay of recording.bin without any external clock —
    the doppler block's default sample-count timeline lines up directly.
    """
    if end <= start:
        raise ValueError("doppler end must be after start")
    if time_step_s <= 0:
        raise ValueError("time_step_s must be > 0")

    sat_body = ephem.readtle(sat_name, tle1, tle2)
    observer = ephem.Observer()
    observer.lat = str(lat)
    observer.lon = str(lon)
    observer.elev = alt_m
    observer.pressure = 0  # disable refraction — irrelevant for range rate

    start_utc = _to_utc(start)
    end_utc = _to_utc(end)
    anchor_ts = _to_utc(anchor).timestamp()

    duration_s = (end_utc - start_utc).total_seconds()
    n_samples = int(duration_s / time_step_s) + 1

    tmp_path = output_path + ".tmp"
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    written = 0
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for i in range(n_samples):
                t = start_utc + datetime.timedelta(seconds=i * time_step_s)
                # ephem expects a naive UTC datetime.
                observer.date = t.replace(tzinfo=None)
                sat_body.compute(observer)
                # range_velocity > 0 when the satellite is receding.
                range_rate = float(sat_body.range_velocity)
                doppler_hz = -range_rate / _C_M_S * f_carrier
                f.write(f"{t.timestamp() - anchor_ts}\t{doppler_hz}\n")
                written += 1
        os.replace(tmp_path, output_path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

    logger.info(
        "wrote doppler file %s (%d samples over %.1fs, f=%.3f MHz)",
        output_path,
        written,
        duration_s,
        f_carrier / 1e6,
    )
    return written


def write_zero_doppler_file(
    *,
    output_path: str,
    anchor: datetime.datetime,
    start: datetime.datetime,
    end: datetime.datetime,
) -> None:
    """Write a stub doppler file that applies zero correction.

    The gr-satellites doppler_correction block hard-requires the file to
    exist and be parseable, but linearly interpolates between entries — so
    two zero-frequency bookends spanning the recording window keep the
    correction at exactly 0 Hz throughout. Timestamps are seconds since
    `anchor`, matching the relative-time scheme used by write_doppler_file.
    """
    if end <= start:
        raise ValueError("doppler end must be after start")

    start_utc = _to_utc(start)
    end_utc = _to_utc(end)
    anchor_ts = _to_utc(anchor).timestamp()

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(f"{start_utc.timestamp() - anchor_ts}\t0\n")
            f.write(f"{end_utc.timestamp() - anchor_ts}\t0\n")
        os.replace(tmp_path, output_path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

    logger.info("wrote zero-doppler stub %s", output_path)
