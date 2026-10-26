from __future__ import annotations

import datetime
import os
import time
from pathlib import Path

import pytest

from auto import events as E
from auto.artifact_store import ArtifactStore
from auto.bus import EventBus
from auto.decode_gate import DecodeGate
from auto.decoder import DecoderService
from auto.models import (
    Decoder,
    GroundstationConfig,
    Pass,
    PassInfo,
    PassStatus,
    Satellite,
)
from auto.state import StateStore
from auto.view import ViewModel


def _sat() -> Satellite:
    return Satellite(
        name="SAT",
        norad="1",
        frequency=437e6,
        bandwidth=50e3,
        sample_rate=96e3,
        decoder=[
            Decoder(name="ax100_9600", container="ax100:latest"),
            Decoder(name="waterfall", role="waterfall", container="waterfall:latest"),
            Decoder(name="ax100_2400", container="ax100:latest"),
        ],
    )


def _pass(pid: str = "p1") -> Pass:
    t = datetime.datetime(2026, 1, 1)
    pi = PassInfo(
        start_time=t, max_time=t, end_time=t + datetime.timedelta(minutes=5),
        start_elevation=5, max_elevation=40, end_elevation=5,
        start_azimuth=0, max_azimuth=90, end_azimuth=180,
        duration_minutes=5, omm={},
    )
    return Pass(id=pid, satellite=_sat(), pass_info=pi, pass_dir="/tmp/x")


def _decoder_service(tmp_path: Path, passes) -> DecoderService:
    cfg = GroundstationConfig(
        satellites=[_sat()], nas_directory=str(tmp_path / "nas"),
        location_lat=0, location_lon=0, location_alt=0,
    )
    return DecoderService(
        cfg, EventBus(), StateStore(str(tmp_path / "state")), DecodeGate(),
        passes, ArtifactStore(tmp_path / "store"),
    )


# ---- roles ----

def test_waterfall_does_not_count_toward_backlog(tmp_path: Path):
    p = _pass()
    svc = _decoder_service(tmp_path, {p.id: p})
    for idx in range(3):
        svc._enqueue((p.id, idx, 0))
    assert svc.waiting() == 2  # the two ax100 runs, not the waterfall

    svc._queue.clear()
    svc._in_queue.clear()
    svc._current = (p.id, 1, 0)  # waterfall in flight
    assert svc.waiting() == 0


@pytest.mark.asyncio
async def test_waterfall_is_queued_first(tmp_path: Path):
    p = _pass()
    svc = _decoder_service(tmp_path, {p.id: p})
    await svc._on_event(E.RecordingCompleted(pass_id=p.id, path="/tmp/x/recording.bin"))
    assert [item[1] for item in svc._queue] == [1, 0, 2]


def test_outcome_counts_only_data_decoders():
    p = _pass()
    p.decoders_done = [1]  # waterfall only
    assert not p.data_decoders_done()
    p.decoders_done = [1, 2]
    assert p.data_decoders_done()


# ---- status ----

def test_view_takes_status_and_reason_from_status_event():
    view = ViewModel()
    p = _pass()
    view.apply(E.PassesTableChanged(passes=[p]))
    view.apply(E.RecordingStarted(pass_id=p.id))  # no longer sets status
    assert view.snapshot().passes[0].status == PassStatus.PREDICTED
    view.apply(E.PassStatusChanged(pass_id=p.id, status=PassStatus.SKIPPED, reason="busy"))
    snap = view.snapshot().passes[0]
    assert snap.status == PassStatus.SKIPPED and snap.status_reason == "busy"


# ---- artifact store ----

def _png(path: Path, data: bytes = b"\x89PNG") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def test_store_keeps_images_only_waterfall_first(tmp_path: Path):
    pass_dir = tmp_path / "pass"
    outs = [
        _png(pass_dir / "x_lrpt" / "MSU-MR" / "MSU-MR-4.png"),
        _png(pass_dir / "waterfall.png"),
        _png(pass_dir / "x_lrpt" / "meteor.cadu", b"data"),
    ]
    store = ArtifactStore(tmp_path / "store")
    images = store.add("p1", pass_dir, outs)
    assert images == ["waterfall.png", "x_lrpt/MSU-MR/MSU-MR-4.png"]
    assert store.path_for("p1", "x_lrpt/MSU-MR/MSU-MR-4.png").read_bytes() == b"\x89PNG"


def test_store_refuses_paths_outside_the_pass(tmp_path: Path):
    store = ArtifactStore(tmp_path / "store")
    store.add("p1", tmp_path / "pass", [_png(tmp_path / "pass" / "waterfall.png")])
    _png(tmp_path / "store" / "artifacts" / "p2" / "secret.png")
    assert store.path_for("p1", "../p2/secret.png") is None
    assert store.path_for("p1", "/etc/passwd") is None
    assert store.path_for("..", "artifacts/p1/waterfall.png") is None
    assert store.images("../store") == []


def test_store_keeps_only_most_recent_passes(tmp_path: Path):
    store = ArtifactStore(tmp_path / "store", keep_passes=3)
    for i in range(5):
        d = tmp_path / f"pass{i}"
        store.add(f"p{i}", d, [_png(d / "waterfall.png")])
        t = time.time() - 100 + i
        os.utime(store.root / f"p{i}", (t, t))
    store.cleanup()
    kept = sorted(p.name for p in store.root.iterdir())
    assert kept == ["p2", "p3", "p4"]


def test_store_never_evicts_the_pass_just_added(tmp_path: Path):
    store = ArtifactStore(tmp_path / "store", keep_passes=1)
    store.add("old", tmp_path / "a", [_png(tmp_path / "a" / "waterfall.png")])
    future = time.time() + 1000
    os.utime(store.root / "old", (future, future))  # clock skew
    store.add("new", tmp_path / "b", [_png(tmp_path / "b" / "waterfall.png")])
    assert [p.name for p in store.root.iterdir()] == ["new"]


# ---- web ----

@pytest.mark.asyncio
async def test_web_serves_artifacts_and_attaches_images(tmp_path: Path):
    from aiohttp.test_utils import TestClient, TestServer

    from auto.web import WebService

    store = ArtifactStore(tmp_path / "store")
    store.add("SAT_1", tmp_path / "pass", [_png(tmp_path / "pass" / "waterfall.png"),
                                           _png(tmp_path / "pass" / "x" / "img 1.png")])
    cfg = GroundstationConfig(
        satellites=[_sat()], nas_directory=str(tmp_path / "nas"),
        location_lat=0, location_lon=0, location_alt=0,
    )
    web = WebService(cfg, EventBus(), ViewModel(), store, skymap=None)
    async with TestClient(TestServer(web._app)) as client:
        r = await client.get("/artifacts/SAT_1/x/img%201.png")
        assert r.status == 200 and await r.read() == b"\x89PNG"
        assert (await client.get("/artifacts/SAT_1/..%2F..%2Fsecret.png")).status == 404
        assert (await client.get("/artifacts/SAT_1/missing.png")).status == 404

    payload = {"passes": [{"id": "SAT_1"}, {"id": "OTHER"}]}
    web._apply_images_map(payload, await web._images_map(["SAT_1", "OTHER"]))
    assert payload["passes"][0]["images"] == ["waterfall.png", "x/img 1.png"]
    assert payload["passes"][1]["images"] == []


# ---- decoder containers ----

class _Captured(Exception):
    pass


@pytest.mark.parametrize("network,offline", [(False, True), (True, False)])
@pytest.mark.asyncio
async def test_decoders_run_offline_unless_network_is_enabled(tmp_path: Path, monkeypatch, network, offline):
    from auto import podman

    seen = {}

    async def fake_exec(*cmd, **kwargs):
        seen["cmd"] = cmd
        raise _Captured

    monkeypatch.setattr(podman.asyncio, "create_subprocess_exec", fake_exec)
    dec = Decoder(name="lrpt", container="satdump:latest", network=network)
    with pytest.raises(_Captured):
        await podman.run_decoder(_sat(), dec, str(tmp_path), str(tmp_path / "out"))
    assert ("--network=none" in seen["cmd"]) is offline
