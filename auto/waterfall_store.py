from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger("groundstation.waterfall_store")

DEFAULT_KEEP = 3
WATERFALL_FILENAME = "waterfall.png"


class WaterfallStore:
    """Persists waterfall PNGs out of the per-pass /tmp directory before the
    transfer service deletes them. Keeps the last N (default 3) by mtime so
    the web UI can show the recent passes' spectrograms on demand."""

    def __init__(self, root: str | Path, keep_n: int = DEFAULT_KEEP) -> None:
        self.root = Path(root) / "waterfalls"
        self.keep_n = keep_n
        self.root.mkdir(parents=True, exist_ok=True)
        # Trim leftovers from previous runs in case keep_n shrank or the
        # service crashed before the last persist() could evict.
        evicted = self.cleanup()
        if evicted:
            logger.info("waterfall_store: evicted %d stale file(s) at startup", evicted)

    def path_for(self, pass_id: str) -> Optional[Path]:
        path = self.root / f"{pass_id}.png"
        return path if path.is_file() else None

    def has(self, pass_id: str) -> bool:
        return (self.root / f"{pass_id}.png").is_file()

    def has_many(self, pass_ids: list[str]) -> dict[str, bool]:
        try:
            present = {
                p.stem
                for p in self.root.iterdir()
                if p.is_file() and p.suffix == ".png"
            }
        except OSError:
            return {pid: False for pid in pass_ids}
        return {pid: pid in present for pid in pass_ids}

    def persist(self, pass_id: str, src_path: str | Path) -> Optional[Path]:
        src = Path(src_path)
        if not src.is_file():
            return None
        dest = self.root / f"{pass_id}.png"
        try:
            tmp = dest.with_suffix(".png.tmp")
            shutil.copyfile(src, tmp)
            os.replace(tmp, dest)
        except OSError:
            logger.exception("could not persist waterfall %s -> %s", src, dest)
            return None
        self.cleanup()
        return dest

    def cleanup(self) -> int:
        try:
            entries = [p for p in self.root.iterdir() if p.is_file() and p.suffix == ".png"]
        except OSError:
            return 0
        if len(entries) <= self.keep_n:
            return 0
        entries.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        removed = 0
        for p in entries[self.keep_n:]:
            try:
                p.unlink()
                removed += 1
            except OSError:
                logger.exception("could not evict old waterfall %s", p)
        return removed


def find_waterfall_in_pass_dir(pass_dir: str | Path) -> Optional[Path]:
    """Locate the waterfall PNG produced by the waterfall decoder. The
    waterfall container always writes to ``$OUTPUT_DIR/waterfall.png`` and
    its decoder is configured without a ``name`` so OUTPUT_DIR == pass_dir.
    Falls back to a recursive scan in case a future config nests it."""
    pd = Path(pass_dir)
    direct = pd / WATERFALL_FILENAME
    if direct.is_file():
        return direct
    try:
        for p in pd.rglob(WATERFALL_FILENAME):
            if p.is_file():
                return p
    except OSError:
        pass
    return None
