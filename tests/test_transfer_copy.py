from __future__ import annotations

import os
import threading

import pytest

from auto import transfer
from auto.transfer import TransferAborted, _copy_with_progress_sync


def _src(tmp_path, size: int = 300_000) -> tuple[str, bytes]:
    data = os.urandom(size)
    path = tmp_path / "src.bin.zst"
    path.write_bytes(data)
    return str(path), data


def _copy(src: str, dst: str, **kw) -> list[tuple[int, int]]:
    calls: list[tuple[int, int]] = []
    _copy_with_progress_sync(src, dst, lambda c, t: calls.append((c, t)), **kw)
    return calls


def test_fresh_copy_goes_through_part_file(tmp_path, monkeypatch):
    monkeypatch.setattr(transfer, "FSYNC_EVERY_BYTES", 100_000)
    src, data = _src(tmp_path)
    dst = str(tmp_path / "nas" / "recording.bin.zst")
    calls = _copy(src, dst)
    assert open(dst, "rb").read() == data
    assert not os.path.exists(dst + ".part")
    assert calls[-1] == (len(data), len(data))


def test_partial_destination_is_replaced_not_resumed(tmp_path):
    # A partial upload from an interrupted attempt may contain holes; it must
    # be rewritten from the start instead of appended to.
    src, data = _src(tmp_path)
    dst = str(tmp_path / "recording.bin.zst")
    with open(dst, "wb") as f:
        f.write(b"\0" * (len(data) // 2))
    calls = _copy(src, dst)
    assert open(dst, "rb").read() == data
    assert calls[0][0] < len(data) // 2  # started from the beginning


def test_leftover_part_file_is_overwritten(tmp_path):
    src, data = _src(tmp_path)
    dst = str(tmp_path / "recording.bin.zst")
    with open(dst + ".part", "wb") as f:
        f.write(b"garbage" * 100_000)
    _copy(src, dst)
    assert open(dst, "rb").read() == data
    assert not os.path.exists(dst + ".part")


def test_abort_leaves_no_final_file(tmp_path):
    src, _ = _src(tmp_path)
    dst = str(tmp_path / "recording.bin.zst")
    stop = threading.Event()
    stop.set()
    with pytest.raises(TransferAborted):
        _copy(src, dst, stop_event=stop)
    assert not os.path.exists(dst)


def test_complete_destination_is_not_copied_again(tmp_path):
    src, data = _src(tmp_path)
    dst = str(tmp_path / "recording.bin.zst")
    with open(dst, "wb") as f:
        f.write(data)
    mtime = os.path.getmtime(dst)
    calls = _copy(src, dst)
    assert calls == [(len(data), len(data))]
    assert os.path.getmtime(dst) == mtime


def test_size_mismatch_is_an_error_and_keeps_destination_absent(tmp_path, monkeypatch):
    src, _ = _src(tmp_path)
    dst = str(tmp_path / "recording.bin.zst")
    real_getsize = os.path.getsize

    def short_part(path):
        size = real_getsize(path)
        return size - 1 if str(path).endswith(".part") else size

    monkeypatch.setattr(transfer.os.path, "getsize", short_part)
    with pytest.raises(OSError, match="incomplete"):
        _copy(src, dst)
    assert not os.path.exists(dst)
