from __future__ import annotations

import asyncio
import os
import stat
from pathlib import Path

from auto import podman
from auto.models import Satellite

FAKE_PODMAN = """#!/bin/sh
echo "$*" >> "$PODMAN_LOG"
case "$1" in
  ps) echo gs-recorder-leftover1 ;;
  run) exec sleep 30 ;;
esac
"""


def _sat() -> Satellite:
    return Satellite(name="SAT", norad=1, frequency=437e6, bandwidth=1e5,
                     sample_rate=384e3, lo_offset=1e5)


async def test_stopped_recorder_is_removed_and_leftovers_cleared(tmp_path: Path, monkeypatch):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    fake = bindir / "podman"
    fake.write_text(FAKE_PODMAN)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "podman.log"
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("PODMAN_LOG", str(log))
    monkeypatch.setattr(podman, "RECORDER_STRAGGLER_DELAY_S", 0.0)

    out = tmp_path / "out"
    out.mkdir()
    (out / "recording.bin").write_bytes(b"\0" * 16)
    stop = asyncio.Event()
    task = asyncio.create_task(podman.run_recorder(_sat(), 10, str(out), stop_event=stop))
    for _ in range(100):
        if log.exists() and "run " in log.read_text():
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 30)

    calls = log.read_text().splitlines()
    name = next(c.split("--name ")[1].split()[0] for c in calls if c.startswith("run "))
    # The leftover from an earlier recording is removed before the new one starts.
    first_run = next(i for i, c in enumerate(calls) if c.startswith("run "))
    assert any("rm" in c and "gs-recorder-leftover1" in c for c in calls[:first_run])
    # The stopped container is force-removed twice (catches one still starting up).
    assert sum(1 for c in calls if c.startswith("rm ") and c.endswith(name)) == 2
