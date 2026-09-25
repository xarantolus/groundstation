from __future__ import annotations

import datetime
import logging
import os
import time

import numpy as np

from . import orbit

logger = logging.getLogger("groundstation.doppler")


DEFAULT_TIME_STEP_S = 0.1


def _to_utc(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.astimezone().astimezone(datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def write_doppler_file(
    *,
    omm: dict,
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

    sat_body = orbit.make_satellite(omm)
    topos = orbit.observer(lat, lon, alt_m)

    start_utc = _to_utc(start)
    end_utc = _to_utc(end)
    anchor_ts = _to_utc(anchor).timestamp()

    duration_s = (end_utc - start_utc).total_seconds()
    n_samples = int(duration_s / time_step_s) + 1

    logger.info(
        "computing doppler for %s: %d samples over %.1fs at %.3f MHz",
        os.path.basename(output_path),
        n_samples,
        duration_s,
        f_carrier / 1e6,
    )
    t0 = time.monotonic()

    # Build the whole file in memory and write once: the Pi's /tmp has
    # high I/O latency and per-line writes through the default 8KB buffer
    # still triggered ~20 flushes per file.
    start_ts = start_utc.timestamp()
    offsets = np.arange(n_samples) * time_step_s
    ts = orbit.timescale()
    t = ts.tt_jd(ts.from_datetime(start_utc).tt + offsets / 86400.0)
    # range rate > 0 when the satellite is receding.
    doppler = -orbit.range_rate_m_s(sat_body, topos, t) / orbit.C_M_S * f_carrier
    parts = [
        f"{start_ts + off - anchor_ts}\t{d}\n"
        for off, d in zip(offsets.tolist(), doppler.tolist())
    ]

    compute_s = time.monotonic() - t0

    tmp_path = output_path + ".tmp"
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write("".join(parts))
        os.replace(tmp_path, output_path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

    io_s = time.monotonic() - t0 - compute_s
    logger.info(
        "wrote doppler file %s in %.2fs (compute %.2fs, io %.2fs)",
        output_path,
        compute_s + io_s,
        compute_s,
        io_s,
    )
    return n_samples


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
            f.write(
                f"{start_utc.timestamp() - anchor_ts}\t0\n"
                f"{end_utc.timestamp() - anchor_ts}\t0\n"
            )
        os.replace(tmp_path, output_path)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise

    logger.info("wrote zero-doppler stub %s", output_path)
