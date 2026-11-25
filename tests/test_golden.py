from __future__ import annotations

import asyncio
import datetime
import os
import shutil
from pathlib import Path

import numpy as np
import pytest

from auto import events as E
from auto import golden
from auto.artifact_store import ArtifactStore
from auto.bus import EventBus
from auto.decode_gate import DecodeGate
from auto.decoder import DecoderService
from auto.models import (
    Decoder,
    GoldenConfig,
    GroundstationConfig,
    Pass,
    PassInfo,
    Satellite,
    TransferRequest,
    should_upload_iq,
)
from auto.state import StateStore
from auto.transfer import TransferService

FS = 96_000.0


def _write_iq(path: Path, n_bursts: int, seconds: float = 60.0, snr_db: float = 10.0) -> None:
    rng = np.random.default_rng(1)
    n = int(seconds * FS)
    iq = (rng.standard_normal(n) + 1j * rng.standard_normal(n)) / np.sqrt(2)
    t = np.arange(int(FS)) / FS
    amp = np.sqrt(10 ** (snr_db / 10))
    for k in range(n_bursts):
        start = int((5 + 8 * k) * FS)
        # 1 s of 2-FSK at +-1.2 kHz around a 500 Hz carrier offset
        bits = rng.integers(0, 2, size=len(t) // 20).repeat(20)
        f = 500 + np.where(bits[: len(t)] > 0, 1200, -1200)
        phase = 2 * np.pi * np.cumsum(f) / FS
        iq[start : start + len(t)] += amp * np.exp(1j * phase)
    iq.astype(np.complex64).tofile(path)


def test_count_bursts_finds_each_burst(tmp_path: Path):
    _write_iq(tmp_path / "r.bin", n_bursts=6)
    assert golden.count_bursts(str(tmp_path / "r.bin"), FS) == 6


def test_count_bursts_ignores_weak_bursts(tmp_path: Path):
    # ~3-4 dB in the analysis band: visible, but not a clear burst
    _write_iq(tmp_path / "r.bin", n_bursts=6, snr_db=-5.5)
    assert golden.count_bursts(str(tmp_path / "r.bin"), FS) == 0


def test_count_bursts_noise_only(tmp_path: Path):
    _write_iq(tmp_path / "r.bin", n_bursts=0)
    assert golden.count_bursts(str(tmp_path / "r.bin"), FS) == 0


@pytest.mark.parametrize(
    "packets,bursts,category",
    [
        (268, None, "many_packets"),
        (50, 3, "many_packets"),
        (0, 40, "signal_no_packets"),
        (5, 40, "few_packets"),
        (20, 40, None),  # decoded a fair share of the bursts
        (0, 5, None),  # too little signal to be interesting
        (0, None, None),
    ],
)
def test_classify(packets, bursts, category):
    assert golden.classify(GoldenConfig(), packets, bursts) == category


def _sat(tags) -> Satellite:
    return Satellite(
        name="SAT", norad="1", frequency=437e6, bandwidth=50e3, sample_rate=FS,
        tags=tags, iq_upload="never",
        decoder=[Decoder(name="ax100_4800", role="decoder", container="ax100:latest")],
    )


def _pass(tmp_path: Path, tags=("AX100",)) -> Pass:
    t = datetime.datetime(2026, 1, 1, 12, 0, 0)
    pi = PassInfo(
        start_time=t, max_time=t, end_time=t + datetime.timedelta(minutes=5),
        start_elevation=5, max_elevation=40, end_elevation=5,
        start_azimuth=0, max_azimuth=90, end_azimuth=180,
        duration_minutes=5, omm={},
    )
    d = tmp_path / "pass"
    d.mkdir(exist_ok=True)
    return Pass(id="SAT_1", satellite=_sat(list(tags)), pass_info=pi, pass_dir=str(d),
                recording_path=str(d / "recording.bin"), decoders_done=[0])


def _service(tmp_path: Path, p: Pass, max_per_satellite: int = 20) -> DecoderService:
    cfg = GroundstationConfig(
        satellites=[p.satellite], nas_directory=str(tmp_path / "nas"),
        location_lat=0, location_lon=0, location_alt=0,
        golden=GoldenConfig(tags=["AX100"], max_per_satellite=max_per_satellite),
    )
    gate = DecodeGate()
    gate.mark_ready()  # idle station, no upcoming pass
    return DecoderService(
        cfg, EventBus(), StateStore(str(tmp_path / "state")), gate,
        {p.id: p}, ArtifactStore(tmp_path / "store"),
    )


@pytest.mark.asyncio
async def test_signal_without_packets_becomes_golden(tmp_path: Path):
    p = _pass(tmp_path)
    _write_iq(Path(p.recording_path), n_bursts=12, seconds=110)
    svc = _service(tmp_path, p)
    sub = svc._bus.subscribe(E.PassGoldenChanged)

    await svc._check_golden(p, p.recording_path)

    assert p.signal_bursts == 12
    assert p.golden == "signal_no_packets"
    ev = await sub.get()
    assert ev.golden == "signal_no_packets" and ev.signal_bursts == 12
    # iq_upload=never, but a golden pass keeps its IQ - in the golden dir.
    assert should_upload_iq(p)
    assert golden.iq_destination(svc._cfg, p) == str(
        tmp_path / "nas" / "golden" / "signal_no_packets" / "SAT_2026-01-01_12-00-00"
        / "recording.bin.zst"
    )


@pytest.mark.asyncio
async def test_many_packets_skips_the_burst_count(tmp_path: Path):
    p = _pass(tmp_path)
    p.max_decoder_outputs = 268
    svc = _service(tmp_path, p)
    await svc._check_golden(p, "/does/not/exist")
    assert p.golden == "many_packets" and p.signal_bursts is None


@pytest.mark.asyncio
async def test_golden_folder_copied_after_the_last_upload(tmp_path: Path):
    p = _pass(tmp_path)
    p.golden = "many_packets"
    nas = tmp_path / "nas"
    nas_pass = Path(golden.pass_nas_dir(str(nas), p))
    (nas_pass / "ax100_4800" / "messages").mkdir(parents=True)
    (nas_pass / "waterfall.png").write_bytes(b"png")
    (nas_pass / "ax100_4800" / "messages" / "csp_packet_0.bin").write_bytes(b"pkt")
    (nas_pass / "info.json.part").write_bytes(b"half")  # upload in flight
    cfg = _service(tmp_path, p)._cfg
    svc = TransferService(cfg, EventBus(), StateStore(str(tmp_path / "state")),
                          num_workers=1, passes={p.id: p})
    dest = nas / "golden" / "many_packets" / "SAT_2026-01-01_12-00-00"

    def upload(name: str) -> TransferRequest:
        src = Path(p.pass_dir) / name
        src.write_text(name)
        return TransferRequest(id=name, source_path=str(src),
                               destination_path=str(dest / name), keep_source=False,
                               compress=False, pass_id=p.id)

    # Another upload of the pass still queued: no copy yet.
    await svc._queue.put(upload("later.txt"))
    await svc._process(upload("golden.json"), asyncio.get_running_loop())
    assert not (dest / "waterfall.png").exists()

    # The last one: the whole NAS folder is copied.
    await svc._process(await svc._queue.get(), asyncio.get_running_loop())
    assert (dest / "waterfall.png").read_bytes() == b"png"
    assert (dest / "ax100_4800" / "messages" / "csp_packet_0.bin").read_bytes() == b"pkt"
    assert not (dest / "info.json.part").exists()
    assert golden.copy_pass_folder(str(nas), p) == 0  # nothing changed
    # Hard links: deleting the pass folder keeps the golden files.
    assert (dest / "waterfall.png").stat().st_nlink == 2
    shutil.rmtree(nas_pass)
    assert (dest / "waterfall.png").read_bytes() == b"png"


@pytest.mark.asyncio
async def test_cap_per_satellite(tmp_path: Path):
    p = _pass(tmp_path)
    p.max_decoder_outputs = 100
    for i in range(2):
        (tmp_path / "nas" / "golden" / "many_packets" / f"SAT_2025-01-0{i + 1}_00-00-00").mkdir(
            parents=True
        )
    svc = _service(tmp_path, p, max_per_satellite=2)
    await svc._check_golden(p, p.recording_path)
    assert p.golden is None
    assert golden.iq_destination(svc._cfg, p).startswith(str(tmp_path / "nas" / "2026"))


@pytest.mark.asyncio
async def test_other_satellites_are_not_checked(tmp_path: Path):
    p = _pass(tmp_path, tags=())
    p.max_decoder_outputs = 100
    svc = _service(tmp_path, p)
    await svc._check_golden(p, p.recording_path)
    assert p.golden is None and p.signal_bursts is None
