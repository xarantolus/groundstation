from __future__ import annotations

import datetime
import math
import os

import pytest

from auto.doppler import write_doppler_file


# Classic ISS TLE used in the Vallado/SGP4 reference papers — checksums are
# valid, so ephem.readtle accepts it. The assertions only check format and
# order-of-magnitude bounds, not the exact Doppler value.
TLE1 = "1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927"
TLE2 = "2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537"


def test_write_doppler_file_format(tmp_path):
    out = tmp_path / "doppler.txt"
    # Stay near the TLE epoch (day 264 of 2008) — ephem refuses to propagate
    # SGP4 too far from epoch.
    start = datetime.datetime(2008, 9, 20, 12, 0, 0, tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(seconds=10)

    n = write_doppler_file(
        tle1=TLE1,
        tle2=TLE2,
        sat_name="ISS",
        lat=52.0,
        lon=13.0,
        alt_m=50.0,
        f_carrier=437e6,
        start=start,
        end=end,
        output_path=str(out),
        time_step_s=0.1,
    )

    assert n == 101  # inclusive of both endpoints

    lines = out.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == n

    prev_ts = None
    for line in lines:
        parts = line.split("\t")
        assert len(parts) == 2, f"expected '<ts>\\t<doppler>' got {line!r}"
        ts = float(parts[0])
        doppler = float(parts[1])
        if prev_ts is not None:
            assert ts > prev_ts, "timestamps must be strictly increasing"
        prev_ts = ts
        # 437 MHz LEO doppler is bounded by roughly ±10 kHz; allow generous
        # margin so this doesn't trip on different propagators.
        assert abs(doppler) < 50_000, f"unreasonable doppler {doppler} Hz"
        assert math.isfinite(doppler)

    first_ts = float(lines[0].split("\t")[0])
    last_ts = float(lines[-1].split("\t")[0])
    assert first_ts == pytest.approx(start.timestamp(), abs=1e-3)
    assert last_ts == pytest.approx(end.timestamp(), abs=1e-3)


def test_write_doppler_file_atomic_replace(tmp_path):
    """A failed write must not leave a partial doppler.txt in place."""
    out = tmp_path / "doppler.txt"
    out.write_text("STALE\n", encoding="utf-8")
    # Stay near the TLE epoch (day 264 of 2008) — ephem refuses to propagate
    # SGP4 too far from epoch.
    start = datetime.datetime(2008, 9, 20, 12, 0, 0, tzinfo=datetime.timezone.utc)
    end = start + datetime.timedelta(seconds=1)

    write_doppler_file(
        tle1=TLE1,
        tle2=TLE2,
        sat_name="ISS",
        lat=0,
        lon=0,
        alt_m=0,
        f_carrier=145e6,
        start=start,
        end=end,
        output_path=str(out),
        time_step_s=0.1,
    )
    text = out.read_text(encoding="utf-8")
    assert "STALE" not in text
    assert "\t" in text
    assert not (tmp_path / "doppler.txt.tmp").exists()


def test_write_doppler_file_rejects_bad_window(tmp_path):
    out = tmp_path / "doppler.txt"
    t = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    with pytest.raises(ValueError):
        write_doppler_file(
            tle1=TLE1,
            tle2=TLE2,
            sat_name="ISS",
            lat=0,
            lon=0,
            alt_m=0,
            f_carrier=145e6,
            start=t,
            end=t,
            output_path=str(out),
        )
    assert not out.exists()
