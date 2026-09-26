"""Access to groundstation recordings (recording.bin = complex64 IQ + info.json)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass
class Recording:
    path: Path
    samp_rate: float
    center_freq: float
    info: dict

    @classmethod
    def open(cls, path: str | Path, info_json: str | Path | None = None) -> "Recording":
        """`path` is a recording.bin (decompressed) or a pass directory containing one."""
        path = Path(path)
        if path.is_dir():
            path = path / "recording.bin"
        info_path = Path(info_json) if info_json else path.parent / "info.json"
        info = json.loads(info_path.read_text())
        sat = info["satellite"]
        return cls(path, float(sat["sample_rate"]), float(sat["frequency"]), info)

    @property
    def iq(self) -> np.memmap:
        return np.memmap(self.path, dtype=np.complex64, mode="r")

    @property
    def n_samples(self) -> int:
        return self.path.stat().st_size // 8

    @property
    def duration(self) -> float:
        return self.n_samples / self.samp_rate

    def chunks(self, chunk_s: float = 5.0, overlap_s: float = 0.0):
        """Yield (start_sample, array) chunks, overlapping so frames on boundaries survive."""
        n = int(chunk_s * self.samp_rate)
        ov = int(overlap_s * self.samp_rate)
        iq = self.iq
        start = 0
        while start < len(iq):
            yield start, np.asarray(iq[start : start + n + ov])
            start += n
