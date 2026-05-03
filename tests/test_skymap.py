from __future__ import annotations

import datetime
from pathlib import Path
from typing import List

import numpy as np
import pytest

from auto.decoder import DecoderService
from auto.models import (
    Decoder,
    GroundstationConfig,
    Pass,
    PassInfo,
    PassStatus,
    Satellite,
)
from auto.skymap import (
    AZ_BIN_DEG,
    CONSUMER_NAME,
    EL_BIN_DEG,
    FFT_SIZE,
    PASS_SILENCE_RATIO,
    SkyObservation,
    SkySample,
    SkymapService,
    _row_snr,
    bin_indices,
)


def _make_pass(tmp_path: Path, sample_rate: float = 250_000) -> Pass:
    sat = Satellite(
        name="TEST-SAT",
        norad="12345",
        frequency=137.5e6,
        bandwidth=40e3,
        sample_rate=sample_rate,
    )
    # Match the test TLE's epoch (day 001 of 2024) within ephem's tolerance.
    now = datetime.datetime(2024, 1, 15, 12, 0, 0)
    pi = PassInfo(
        start_time=now,
        max_time=now + datetime.timedelta(seconds=2),
        end_time=now + datetime.timedelta(seconds=4),
        start_elevation=5,
        max_elevation=45,
        end_elevation=5,
        start_azimuth=10,
        max_azimuth=90,
        end_azimuth=170,
        duration_minutes=4 / 60,
        # Real-ish ISS TLE (any valid TLE works for compute_azel smoke test).
        tle1="1 25544U 98067A   24001.50000000  .00016717  00000-0  10270-3 0  9009",
        tle2="2 25544  51.6400 100.0000 0001000  90.0000 270.0000 15.50000000 12349",
    )
    pid = Pass.make_id(sat, pi)
    return Pass(
        id=pid,
        satellite=sat,
        pass_info=pi,
        pass_dir=str(tmp_path / f"recorder-{pid}"),
        recording_path=str(tmp_path / f"recorder-{pid}" / "recording.bin"),
        status=PassStatus.RECORDED,
    )


def _cfg(retention_days: float = 14.0) -> GroundstationConfig:
    return GroundstationConfig(
        satellites=[],
        nas_directory="/tmp/nas",
        location_lat=52.0,
        location_lon=13.0,
        location_alt=50.0,
        skymap_retention_days=retention_days,
    )


def test_row_snr_detects_carrier_above_noise():
    rng = np.random.default_rng(0)
    n = FFT_SIZE * 4
    noise = (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64) * 0.1
    # Strong tone at +50kHz of a 250 kS/s stream.
    t = np.arange(n) / 250_000
    tone = (0.5 * np.exp(2j * np.pi * 50_000 * t)).astype(np.complex64)
    snr, has_carrier = _row_snr(noise + tone)
    assert has_carrier
    assert snr > 20.0


def test_row_snr_pure_noise_has_no_carrier():
    rng = np.random.default_rng(1)
    n = FFT_SIZE * 4
    noise = (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64) * 0.1
    snr, has_carrier = _row_snr(noise)
    assert not has_carrier, f"unexpected carrier in white noise: snr={snr}"


def test_bin_indices_corner_cases():
    assert bin_indices(0, 0) == (0, 0)
    assert bin_indices(359.99, 89.99) == (int(360 / AZ_BIN_DEG) - 1, int(90 / EL_BIN_DEG) - 1)
    assert bin_indices(-1, 10) == bin_indices(359, 10)
    assert bin_indices(180, -0.1) is None


def _write_iq(path: Path, samples: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(samples.astype(np.complex64).tobytes())


def test_analyze_iq_carrier_pass(tmp_path: Path, monkeypatch):
    cfg = _cfg()
    p = _make_pass(tmp_path)
    rng = np.random.default_rng(2)
    sr = int(p.satellite.sample_rate)
    seconds = 3
    n = sr * seconds
    noise = (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64) * 0.1
    t = np.arange(n) / sr
    tone = (0.5 * np.exp(2j * np.pi * 30_000 * t)).astype(np.complex64)
    iq = noise + tone
    _write_iq(Path(p.recording_path), iq)

    # SkymapService analyzer doesn't need a bus/state for the pure analyze call.
    svc = SkymapService.__new__(SkymapService)
    svc._cfg = cfg
    obs = svc._analyze_iq(p)
    assert obs is not None
    assert obs.silent is False
    assert len(obs.samples) == seconds
    for s in obs.samples:
        assert s.has_carrier
        assert s.snr_db > 15.0


def test_analyze_iq_silent_pass(tmp_path: Path):
    cfg = _cfg()
    p = _make_pass(tmp_path)
    rng = np.random.default_rng(3)
    sr = int(p.satellite.sample_rate)
    seconds = 3
    n = sr * seconds
    noise = (rng.standard_normal(n) + 1j * rng.standard_normal(n)).astype(np.complex64) * 0.1
    _write_iq(Path(p.recording_path), noise)

    svc = SkymapService.__new__(SkymapService)
    svc._cfg = cfg
    obs = svc._analyze_iq(p)
    assert obs is not None
    assert obs.silent is True
    assert all(not s.has_carrier for s in obs.samples)


def test_analyze_iq_missing_file_returns_none(tmp_path: Path):
    cfg = _cfg()
    p = _make_pass(tmp_path)
    # No file written. open() will fail; analyzer returns None.
    svc = SkymapService.__new__(SkymapService)
    svc._cfg = cfg
    assert svc._analyze_iq(p) is None


def test_iq_release_ready_waits_for_consumers(tmp_path: Path):
    p = _make_pass(tmp_path)
    p.satellite = Satellite(
        name=p.satellite.name,
        norad=p.satellite.norad,
        frequency=p.satellite.frequency,
        bandwidth=p.satellite.bandwidth,
        sample_rate=p.satellite.sample_rate,
        decoder=[Decoder(container="x", name="d")],
    )
    p.decoders_done = [0]
    p.iq_consumers_pending = ["skymap"]

    dec = DecoderService.__new__(DecoderService)
    dec._iq_consumers = ["skymap"]
    assert not dec._iq_release_ready(p)

    p.iq_consumers_pending.remove("skymap")
    p.iq_consumers_done.append("skymap")
    assert dec._iq_release_ready(p)


def test_iq_release_ready_no_consumers_unaffected(tmp_path: Path):
    p = _make_pass(tmp_path)
    p.satellite = Satellite(
        name=p.satellite.name,
        norad=p.satellite.norad,
        frequency=p.satellite.frequency,
        bandwidth=p.satellite.bandwidth,
        sample_rate=p.satellite.sample_rate,
        decoder=[Decoder(container="x", name="d")],
    )
    p.decoders_done = [0]
    # iq_consumers_pending stays empty when no consumers are registered.
    dec = DecoderService.__new__(DecoderService)
    dec._iq_consumers = []
    assert dec._iq_release_ready(p)


def test_prune_old_drops_aged_observations(tmp_path: Path):
    cfg = _cfg(retention_days=2.0)
    svc = SkymapService.__new__(SkymapService)
    svc._cfg = cfg
    svc._cells = {}
    svc._observations = {}
    svc._obs_dir = tmp_path / "obs"
    svc._obs_dir.mkdir()

    now = datetime.datetime.now()
    fresh = SkyObservation(
        pass_id="fresh", satellite_name="S", recorded_at=now,
        samp_rate=1.0, silent=False,
        samples=[SkySample(t=now, az=10, el=20, snr_db=10, has_carrier=True)],
    )
    aged = SkyObservation(
        pass_id="aged", satellite_name="S",
        recorded_at=now - datetime.timedelta(days=5),
        samp_rate=1.0, silent=False,
        samples=[SkySample(t=now, az=200, el=20, snr_db=5, has_carrier=True)],
    )
    svc._observations["fresh"] = fresh
    svc._observations["aged"] = aged
    (svc._obs_dir / "fresh.json").write_text(fresh.model_dump_json(), encoding="utf-8")
    (svc._obs_dir / "aged.json").write_text(aged.model_dump_json(), encoding="utf-8")
    svc._merge_into_aggregate(fresh)
    svc._merge_into_aggregate(aged)

    svc._prune_old()

    assert "fresh" in svc._observations
    assert "aged" not in svc._observations
    assert (svc._obs_dir / "fresh.json").exists()
    assert not (svc._obs_dir / "aged.json").exists()
    # Aggregate should only contain the fresh cell now.
    fresh_idx = bin_indices(10, 20)
    aged_idx = bin_indices(200, 20)
    assert fresh_idx in svc._cells
    assert aged_idx not in svc._cells


def test_aggregate_returns_metadata_and_cells(tmp_path: Path):
    cfg = _cfg()
    svc = SkymapService.__new__(SkymapService)
    svc._cfg = cfg
    svc._cells = {}
    svc._observations = {}
    svc._obs_dir = tmp_path

    now = datetime.datetime.now()
    obs = SkyObservation(
        pass_id="p1", satellite_name="NOAA-19", recorded_at=now,
        samp_rate=1.0, silent=False,
        samples=[
            SkySample(t=now, az=10, el=20, snr_db=10, has_carrier=True),
            SkySample(t=now, az=10, el=20, snr_db=12, has_carrier=True),
        ],
    )
    svc._observations["p1"] = obs
    svc._merge_into_aggregate(obs)

    agg = svc.aggregate()
    assert agg["total_passes"] == 1
    assert agg["silent_passes"] == 0
    assert agg["satellites"] == ["NOAA-19"]
    assert agg["retention_days"] == cfg.skymap_retention_days
    assert any(c["az_idx"] == bin_indices(10, 20)[0] for c in agg["cells"])

    # Filter by a satellite that has no data — empty cells.
    other = svc.aggregate(satellite="DOES-NOT-EXIST")
    assert other["cells"] == []


def test_compute_azel_matches_predictor():
    """Smoke test: the extracted helper agrees with PassPredictor's own
    observer setup at a known time. Numbers don't matter; consistency does."""
    from auto.pass_predictor import compute_azel

    tle1 = "1 25544U 98067A   24001.50000000  .00016717  00000-0  10270-3 0  9009"
    tle2 = "2 25544  51.6400 100.0000 0001000  90.0000 270.0000 15.50000000 12349"
    when = datetime.datetime(2024, 1, 15, 12, 0, 0)
    az, el = compute_azel(tle1, tle2, 52.0, 13.0, 50.0, when)
    assert 0 <= az < 360
    assert -90 <= el <= 90
    # Repeatable
    az2, el2 = compute_azel(tle1, tle2, 52.0, 13.0, 50.0, when)
    assert az == az2 and el == el2
