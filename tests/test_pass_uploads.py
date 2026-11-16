from __future__ import annotations

import asyncio
import datetime as _dt
import json

from auto import events as E
from auto.bus import EventBus
from auto.main import _backfill_uploads
from auto.models import Pass, PassInfo, PassStatus, Satellite, TransferRequest, UploadedFile
from auto.state import StateStore
from auto.transfer import TransferService, uploaded_file_for
from auto.view import ViewModel


def _pass(pass_dir: str, pid: str = "SAT_2026-01-01_00-00-00") -> Pass:
    info = PassInfo(
        start_time=_dt.datetime(2026, 1, 1, 0, 0, 0),
        max_time=_dt.datetime(2026, 1, 1, 0, 5, 0),
        end_time=_dt.datetime(2026, 1, 1, 0, 10, 0),
        start_elevation=0.0, max_elevation=45.0, end_elevation=0.0,
        start_azimuth=0.0, max_azimuth=180.0, end_azimuth=359.0,
        duration_minutes=10.0, omm={},
    )
    sat = Satellite(name="SAT", norad="1", frequency=1.0, bandwidth=1.0, sample_rate=1.0)
    return Pass(id=pid, satellite=sat, pass_info=info, pass_dir=pass_dir, status=PassStatus.DONE)


def test_uploaded_file_path_is_relative_to_pass_dir():
    p = _pass("/tmp/recorder-x")
    assert uploaded_file_for(p, "/tmp/recorder-x/info.json", "/nas/a/info.json").path == "info.json"
    assert uploaded_file_for(
        p, "/tmp/recorder-x/x_lrpt/MSU-MR/1.png", "/nas/a/x_lrpt/MSU-MR/1.png"
    ).path == "x_lrpt/MSU-MR/1.png"
    # Compressed on the way: the NAS file carries .zst.
    assert uploaded_file_for(
        p, "/tmp/recorder-x/recording.bin", "/nas/a/recording.bin.zst"
    ).path == "recording.bin.zst"
    # Source outside the pass dir: fall back to the destination's name.
    assert uploaded_file_for(p, "/elsewhere/f.bin", "/nas/a/f.bin").path == "f.bin"


async def test_finished_upload_is_recorded_on_the_pass(tmp_path):
    pass_dir = tmp_path / "recorder-x"
    pass_dir.mkdir()
    (pass_dir / "info.json").write_text("{}")
    p = _pass(str(pass_dir))
    passes = {p.id: p}
    state = StateStore(str(tmp_path / "state"))
    bus = EventBus()
    sub = bus.subscribe(E.PassUploadsChanged, name="t", queue_size=16)
    svc = TransferService(None, bus, state, num_workers=1, passes=passes)  # type: ignore[arg-type]
    req = TransferRequest(
        id="r1",
        source_path=str(pass_dir / "info.json"),
        destination_path=str(tmp_path / "nas" / "info.json"),
        keep_source=True,
        compress=False,
        pass_id=p.id,
        label="SAT info",
    )
    await svc._process(req, asyncio.get_running_loop())
    # Uploading the same file again replaces the entry instead of duplicating it.
    await svc._process(req.model_copy(update={"id": "r2"}), asyncio.get_running_loop())

    assert [u.path for u in p.uploads] == ["info.json"]
    assert p.uploads[0].size == 2
    assert p.uploads[0].label == "SAT info"
    saved = json.loads((tmp_path / "state" / "passes" / f"{p.id}.json").read_text())
    assert saved["uploads"][0]["path"] == "info.json"
    ev = await asyncio.wait_for(sub.get(), 1)
    assert isinstance(ev, E.PassUploadsChanged) and ev.pass_id == p.id
    sub.close()


def test_backfill_from_completed_history(tmp_path):
    state = StateStore(str(tmp_path / "state"))
    a = _pass("/tmp/recorder-a", "A")
    b = _pass("/tmp/recorder-b", "B")
    b.uploads = [UploadedFile(path="already.txt")]
    for source, dest in [
        ("/tmp/recorder-a/info.json", "/nas/A/info.json"),
        ("/tmp/recorder-a/dec/img.png", "/nas/A/dec/img.png"),
        ("/tmp/recorder-a/recording.bin", "/nas/A/recording.bin.zst"),
        ("/tmp/recorder-ab/info.json", "/nas/AB/info.json"),  # other pass, same prefix
        ("/tmp/recorder-b/info.json", "/nas/B/info.json"),
    ]:
        state.record_completed({"id": "x", "source": source, "dest": dest, "label": None,
                                "ts": "2026-01-01T00:20:00"})
    assert _backfill_uploads(state, {"A": a, "B": b}) == 1
    assert [u.path for u in a.uploads] == ["info.json", "dec/img.png", "recording.bin.zst"]
    assert [u.path for u in b.uploads] == ["already.txt"]  # untouched


def test_view_applies_uploads_event():
    view = ViewModel()
    p = _pass("/tmp/recorder-a", "A")
    view.apply(E.PassesTableChanged(passes=[p]))
    view.apply(E.PassUploadsChanged(pass_id="A", uploads=[UploadedFile(path="info.json", size=3)]))
    snap = view.snapshot()
    assert [u.path for u in snap.passes[0].uploads] == ["info.json"]
    assert p.uploads == []  # the service-side pass is not shared with the view
