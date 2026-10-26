#!/usr/bin/env python3
"""Add a "221" false-colour image next to every Meteor MSU-MR channel set.

SatDump 2.0-alpha no longer renders composites ("NO MORE CPP COMPOSITES"),
so only the single channels MSU-MR-<n>.png come out. 221 is the classic
daytime Meteor colour image: R = G = channel 2 (near-IR, vegetation
bright), B = channel 1 (visible). Each channel is contrast-stretched on its
own. Folders without both channels (e.g. night passes) are left alone.

usage: meteor_composites.py <output dir>
"""
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageOps

STRETCH_CUTOFF_PERCENT = 0.5


def compose_221(folder: Path) -> Path | None:
    ch1, ch2 = folder / "MSU-MR-1.png", folder / "MSU-MR-2.png"
    if not (ch1.is_file() and ch2.is_file()):
        return None
    blue = Image.open(ch1).convert("L")
    red = Image.open(ch2).convert("L")
    if blue.size != red.size:
        print(f"composite: {folder}: channel sizes differ, skipped", file=sys.stderr)
        return None
    red = ImageOps.autocontrast(red, cutoff=STRETCH_CUTOFF_PERCENT)
    blue = ImageOps.autocontrast(blue, cutoff=STRETCH_CUTOFF_PERCENT)
    out = folder / "MSU-MR-221.png"
    Image.merge("RGB", (red, red, blue)).save(out)
    return out


def main(root: str) -> None:
    for ch1 in sorted(Path(root).rglob("MSU-MR-1.png")):
        out = compose_221(ch1.parent)
        if out is not None:
            print(f"composite: wrote {out}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/output")
