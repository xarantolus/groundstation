from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional

logger = logging.getLogger("groundstation.artifact_store")

DEFAULT_KEEP_PASSES = 10
IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp"})


def is_image(path: str | Path) -> bool:
    return Path(path).suffix.lower() in IMAGE_SUFFIXES


class ArtifactStore:
    """Keeps the images a pass produced (waterfall, decoder products) after
    the per-pass /tmp directory is uploaded and deleted, so the web UI can
    show them. Layout: <root>/artifacts/<pass_id>/<path relative to the pass
    dir>. Only the most recent ``keep_passes`` passes are kept."""

    def __init__(self, root: str | Path, keep_passes: int = DEFAULT_KEEP_PASSES) -> None:
        self.root = Path(root) / "artifacts"
        self.keep_passes = keep_passes
        self.root.mkdir(parents=True, exist_ok=True)
        evicted = self.cleanup()
        if evicted:
            logger.info("artifact_store: evicted %d old pass(es) at startup", evicted)

    def _pass_dir(self, pass_id: str) -> Path:
        d = (self.root / pass_id).resolve()
        if d.parent != self.root.resolve():
            raise ValueError(f"invalid pass id {pass_id!r}")
        return d

    def add(self, pass_id: str, pass_dir: str | Path, paths: Iterable[str | Path]) -> List[str]:
        """Copy the image files among ``paths`` (inside ``pass_dir``) into the
        store. Returns all images now kept for the pass."""
        dest_root = self._pass_dir(pass_id)
        base = Path(pass_dir).resolve()
        copied = False
        for src in map(Path, paths):
            if not is_image(src) or not src.is_file():
                continue
            try:
                rel = src.resolve().relative_to(base)
            except ValueError:
                logger.warning("artifact %s is outside pass dir %s, skipped", src, base)
                continue
            dest = dest_root / rel
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                tmp = dest.with_name(dest.name + ".tmp")
                shutil.copyfile(src, tmp)
                os.replace(tmp, dest)
                copied = True
            except OSError:
                logger.exception("could not keep artifact %s -> %s", src, dest)
        if copied:
            os.utime(dest_root)  # mark as most recent for eviction
            self.cleanup(keep=pass_id)
        return self.images(pass_id)

    def images(self, pass_id: str) -> List[str]:
        """Relative image paths of a pass, waterfall first, then by path."""
        try:
            d = self._pass_dir(pass_id)
        except ValueError:
            return []
        if not d.is_dir():
            return []
        found = [
            p.relative_to(d).as_posix()
            for p in d.rglob("*")
            if p.is_file() and is_image(p)
        ]
        return sorted(found, key=lambda r: (r != "waterfall.png", r))

    def images_many(self, pass_ids: Iterable[str]) -> Dict[str, List[str]]:
        return {pid: self.images(pid) for pid in set(pass_ids)}

    def path_for(self, pass_id: str, rel: str) -> Optional[Path]:
        """Resolve a stored image, refusing anything outside the pass dir."""
        try:
            d = self._pass_dir(pass_id)
        except ValueError:
            return None
        p = (d / rel).resolve()
        if d not in p.parents or not p.is_file() or not is_image(p):
            return None
        return p

    def cleanup(self, keep: Optional[str] = None) -> int:
        """Evict all but the ``keep_passes`` most recently updated passes;
        ``keep`` (the pass just added) is never evicted."""
        try:
            dirs = [p for p in self.root.iterdir() if p.is_dir()]
        except OSError:
            return 0
        if len(dirs) <= self.keep_passes:
            return 0
        dirs.sort(key=lambda p: (p.name == keep, p.stat().st_mtime), reverse=True)
        removed = 0
        for d in dirs[self.keep_passes:]:
            try:
                shutil.rmtree(d)
                removed += 1
            except OSError:
                logger.exception("could not evict artifacts of %s", d.name)
        return removed
