from __future__ import annotations

import datetime
import json
import os
from pathlib import Path

import pytest

from auto.models import Decoder, Pass, PassInfo, PassStatus, Satellite, TransferRequest
from auto.state import StateStore


def _make_pass(tmp_path: Path, name: str = "SAT") -> Pass:
    sat = Satellite(
        name=name,
        norad="12345",
        frequency=137e6,
        bandwidth=50e3,
        sample_rate=192e3,
        decoder=[Decoder(container="ghcr.io/x/y:latest")],
    )
    now = datetime.datetime.now()
    pi = PassInfo(
        start_time=now,
        max_time=now,
        end_time=now + datetime.timedelta(minutes=5),
        start_elevation=5,
        max_elevation=45,
        end_elevation=5,
        start_azimuth=0,
        max_azimuth=90,
        end_azimuth=180,
        duration_minutes=5.0,
        tle1="t1",
        tle2="t2",
    )
    return Pass(
        id=Pass.make_id(sat, pi),
        satellite=sat,
        pass_info=pi,
        pass_dir=str(tmp_path / f"recorder-{name}"),
    )


def test_pass_roundtrip(tmp_path: Path):
    s = StateStore(str(tmp_path))
    p = _make_pass(tmp_path)
    s.save_pass(p)
    loaded = s.load_passes()
    assert len(loaded) == 1
    assert loaded[0].id == p.id
    assert loaded[0].satellite.name == "SAT"


def test_corrupt_pass_quarantined(tmp_path: Path):
    s = StateStore(str(tmp_path))
    good = _make_pass(tmp_path, "GOOD")
    s.save_pass(good)
    # Write a bogus file
    (s.passes_dir / "bogus.json").write_text("{not valid json", encoding="utf-8")
    loaded = s.load_passes()
    assert len(loaded) == 1
    assert loaded[0].id == good.id
    assert s.corrupt_dir.exists()
    assert any(s.corrupt_dir.iterdir()), "corrupt file should be quarantined"


def test_transfer_queue_put_and_tombstone(tmp_path: Path):
    s = StateStore(str(tmp_path))
    req = TransferRequest(id="r1", source_path="/a", destination_path="/b")
    s.transfer_put(req)
    assert [r.id for r in s.load_transfer_queue()] == ["r1"]
    s.transfer_tombstone("r1")
    assert s.load_transfer_queue() == []


def test_transfer_queue_survives_truncated_tail(tmp_path: Path):
    s = StateStore(str(tmp_path))
    req = TransferRequest(id="good", source_path="/a", destination_path="/b")
    s.transfer_put(req)
    with open(s.transfer_queue_log, "a", encoding="utf-8") as f:
        f.write('{"op": "put", "req": {')  # truncated
    # Loader should stop at the truncated line and keep what came before
    loaded = s.load_transfer_queue()
    assert [r.id for r in loaded] == ["good"]


def test_decode_queue_put_and_tombstone(tmp_path: Path):
    s = StateStore(str(tmp_path))
    s.decode_put("p1", 0)
    s.decode_put("p1", 1)
    s.decode_put("p2", 0)
    s.decode_tombstone("p1", 0)
    assert set(s.load_decode_queue()) == {("p1", 1), ("p2", 0)}


def test_atomic_write_no_partial_file(tmp_path: Path, monkeypatch):
    s = StateStore(str(tmp_path))
    p = _make_pass(tmp_path)
    # Force os.fsync to raise so we know save_pass doesn't leave a partial file
    real_replace = os.replace

    def bad_replace(src, dst):
        raise OSError("simulated failure")

    monkeypatch.setattr("os.replace", bad_replace)
    s.save_pass(p)
    # Original file must not be visible (partial-file-safe)
    target = s.passes_dir / f"{p.id}.json"
    assert not target.exists()
    monkeypatch.setattr("os.replace", real_replace)
