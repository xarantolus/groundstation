"""Per-pass skymap analyzer.

Streams the raw IQ recording in 1-second chunks (no copy), computes
per-second peak-over-floor SNR and a carrier-presence flag, tags each
sample with the satellite's az/el at that moment, and persists a per-pass
observation JSON to ``state/skymap/observations/{pass_id}.json``.
Maintains an in-memory aggregate (az/el grid) for the web UI.

Hooks into the existing IQ-retention gate via ``Pass.iq_consumers_pending``
/ ``iq_consumers_done`` — :class:`DecoderService` waits for both decoders
and consumers before compressing/deleting ``recording.bin``.
"""
from __future__ import annotations

import asyncio
import collections
import datetime
import logging
import os
from pathlib import Path
from typing import Deque, Dict, List, Optional, Set, Tuple

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from . import events as E
from .bus import EventBus, run_subscriber
from .models import GroundstationConfig, Pass
from .pass_predictor import compute_azel
from .state import StateStore, _atomic_write_text

logger = logging.getLogger("groundstation.skymap")

CONSUMER_NAME = "skymap"

FFT_SIZE = 4096
FFTS_PER_SECOND = 16
CARRIER_K_MAD = 4.0
CARRIER_MIN_SNR_DB = 6.0
PASS_SILENCE_RATIO = 0.05

AZ_BIN_DEG = 5.0
EL_BIN_DEG = 5.0
N_AZ_BINS = int(360 / AZ_BIN_DEG)
N_EL_BINS = int(90 / EL_BIN_DEG)


class SkySample(BaseModel):
    model_config = ConfigDict(extra="forbid")
    t: datetime.datetime
    az: float
    el: float
    snr_db: float
    has_carrier: bool


class SkyObservation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pass_id: str
    satellite_name: str
    recorded_at: datetime.datetime
    samp_rate: float
    silent: bool
    samples: List[SkySample] = Field(default_factory=list)


def bin_indices(az: float, el: float) -> Optional[Tuple[int, int]]:
    if el < 0 or el > 90:
        return None
    az = az % 360
    az_idx = min(int(az / AZ_BIN_DEG), N_AZ_BINS - 1)
    el_idx = min(int(el / EL_BIN_DEG), N_EL_BINS - 1)
    return az_idx, el_idx


class _CellAccumulator:
    """Streaming aggregator for one (az, el) cell. Per-satellite samples
    keep the satellite filter cheap, and let the all-satellites view skip
    cross-satellite normalisation entirely."""

    __slots__ = ("snrs_by_sat", "silent_by_sat", "last_seen_by_sat")

    def __init__(self) -> None:
        self.snrs_by_sat: Dict[str, List[float]] = {}
        self.silent_by_sat: Dict[str, List[bool]] = {}
        self.last_seen_by_sat: Dict[str, datetime.datetime] = {}

    def add(
        self,
        sat: str,
        snr_db: float,
        has_carrier: bool,
        t: datetime.datetime,
    ) -> None:
        if has_carrier:
            self.snrs_by_sat.setdefault(sat, []).append(snr_db)
        self.silent_by_sat.setdefault(sat, []).append(not has_carrier)
        prev = self.last_seen_by_sat.get(sat)
        if prev is None or t > prev:
            self.last_seen_by_sat[sat] = t

    def view(self, sat_filter: Optional[str] = None) -> Optional[Dict]:
        if sat_filter:
            snrs = self.snrs_by_sat.get(sat_filter, [])
            silent = self.silent_by_sat.get(sat_filter, [])
            last_seen = self.last_seen_by_sat.get(sat_filter)
        else:
            snrs = [v for arr in self.snrs_by_sat.values() for v in arr]
            silent = [v for arr in self.silent_by_sat.values() for v in arr]
            last_seens = list(self.last_seen_by_sat.values())
            last_seen = max(last_seens) if last_seens else None

        if not silent:
            return None

        if snrs:
            arr = np.asarray(snrs)
            snr_p50 = float(np.median(arr))
            snr_mad = float(np.median(np.abs(arr - snr_p50)))
        else:
            snr_p50 = 0.0
            snr_mad = 0.0
        return {
            "samples": len(silent),
            "snr_p50": snr_p50,
            "snr_mad": snr_mad,
            "silence_rate": sum(silent) / len(silent),
            "last_seen": last_seen.isoformat() if last_seen else None,
        }


class SkymapService:
    def __init__(
        self,
        cfg: GroundstationConfig,
        bus: EventBus,
        state: StateStore,
        passes: Dict[str, Pass],
        root: str = "state",
    ) -> None:
        self._cfg = cfg
        self._bus = bus
        self._state = state
        self._passes = passes
        self._root = Path(root)
        self._obs_dir = self._root / "skymap" / "observations"
        self._obs_dir.mkdir(parents=True, exist_ok=True)

        self._queue: Deque[str] = collections.deque()
        self._in_queue: Set[str] = set()
        self._wakeup = asyncio.Event()
        self._stop = asyncio.Event()

        self._cells: Dict[Tuple[int, int], _CellAccumulator] = {}
        self._observations: Dict[str, SkyObservation] = {}
        self._load_observations()

        for p in passes.values():
            if (
                CONSUMER_NAME in p.iq_consumers_pending
                and CONSUMER_NAME not in p.iq_consumers_done
                and p.recording_path
                and os.path.isfile(p.recording_path)
            ):
                self._enqueue(p.id)
        if self._in_queue:
            logger.info(
                "skymap: restored %d analyze job(s) from state", len(self._in_queue)
            )

    def _enqueue(self, pass_id: str) -> None:
        if pass_id in self._in_queue:
            return
        self._in_queue.add(pass_id)
        self._queue.append(pass_id)
        self._wakeup.set()

    async def run(self) -> None:
        sub = self._bus.subscribe(
            E.RecordingCompleted,
            name="skymap.events",
            queue_size=128,
        )
        sub_task = asyncio.create_task(
            run_subscriber(sub, self._on_event, "skymap.events")
        )
        try:
            while not self._stop.is_set():
                if not self._queue:
                    self._wakeup.clear()
                    if self._queue:
                        continue
                    wakeup_task = asyncio.create_task(self._wakeup.wait())
                    stop_task = asyncio.create_task(self._stop.wait())
                    _, pending = await asyncio.wait(
                        {wakeup_task, stop_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()
                    if self._stop.is_set():
                        break
                    continue

                pass_id = self._queue.popleft()
                self._in_queue.discard(pass_id)
                try:
                    await self._analyze_pass(pass_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("skymap: analyze failed for %s", pass_id)
                    await self._mark_done(pass_id)
                self._prune_old()
        finally:
            sub.close()
            sub_task.cancel()
            try:
                await sub_task
            except asyncio.CancelledError:
                pass

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()

    async def _on_event(self, event: E.Event) -> None:
        if isinstance(event, E.RecordingCompleted):
            self._enqueue(event.pass_id)

    async def _analyze_pass(self, pass_id: str) -> None:
        p = self._passes.get(pass_id)
        if not p:
            logger.warning("skymap: pass %s unknown — releasing", pass_id)
            return
        if not p.recording_path or not os.path.isfile(p.recording_path):
            logger.warning(
                "skymap: IQ missing for %s at %s — releasing",
                pass_id,
                p.recording_path,
            )
            await self._mark_done(pass_id)
            return

        loop = asyncio.get_running_loop()
        obs = await loop.run_in_executor(None, self._analyze_iq, p)
        if obs is None:
            logger.warning("skymap: no analyzable data for %s — releasing", pass_id)
            await self._mark_done(pass_id)
            return

        self._persist_observation(obs)
        if not obs.silent:
            self._merge_into_aggregate(obs)
        else:
            logger.info(
                "skymap: %s flagged silent (carrier rate < %.0f%%)",
                pass_id,
                PASS_SILENCE_RATIO * 100,
            )

        await self._mark_done(pass_id)
        await self._bus.publish(E.SkymapUpdated(pass_id=pass_id))

    def _analyze_iq(self, p: Pass) -> Optional[SkyObservation]:
        samp_rate = float(p.satellite.sample_rate)
        if samp_rate <= 0:
            logger.warning("skymap: bad sample rate %s for %s", samp_rate, p.id)
            return None

        bytes_per_sample = 8  # complex64
        samples_per_second = int(samp_rate)
        chunk_bytes = samples_per_second * bytes_per_sample
        if chunk_bytes <= 0:
            return None

        start_t = (
            p.pass_info.recording_start_override
            if p.pass_info.recording_start_override is not None
            else p.pass_info.start_time
        )

        samples_out: List[SkySample] = []
        rows_with_carrier = 0
        total_rows = 0

        try:
            with open(p.recording_path, "rb") as f:
                row_idx = 0
                while True:
                    buf = f.read(chunk_bytes)
                    if len(buf) < chunk_bytes:
                        break
                    arr = np.frombuffer(buf, dtype=np.complex64)
                    snr_db, has_carrier = _row_snr(arr)
                    t = start_t + datetime.timedelta(seconds=row_idx)
                    az, el = compute_azel(
                        p.pass_info.tle1,
                        p.pass_info.tle2,
                        self._cfg.location_lat,
                        self._cfg.location_lon,
                        self._cfg.location_alt,
                        t,
                    )
                    samples_out.append(
                        SkySample(
                            t=t,
                            az=az,
                            el=el,
                            snr_db=snr_db,
                            has_carrier=has_carrier,
                        )
                    )
                    total_rows += 1
                    if has_carrier:
                        rows_with_carrier += 1
                    row_idx += 1
        except OSError:
            logger.exception("skymap: read failed for %s", p.id)
            return None

        if total_rows == 0:
            return None

        carrier_rate = rows_with_carrier / total_rows
        return SkyObservation(
            pass_id=p.id,
            satellite_name=p.satellite.name,
            recorded_at=start_t,
            samp_rate=samp_rate,
            silent=carrier_rate < PASS_SILENCE_RATIO,
            samples=samples_out,
        )

    def _persist_observation(self, obs: SkyObservation) -> None:
        path = self._obs_dir / f"{obs.pass_id}.json"
        try:
            _atomic_write_text(path, obs.model_dump_json(indent=2))
            self._observations[obs.pass_id] = obs
        except OSError:
            logger.exception("skymap: persist failed for %s", obs.pass_id)

    def _merge_into_aggregate(self, obs: SkyObservation) -> None:
        for s in obs.samples:
            idx = bin_indices(s.az, s.el)
            if idx is None:
                continue
            cell = self._cells.get(idx)
            if cell is None:
                cell = _CellAccumulator()
                self._cells[idx] = cell
            cell.add(obs.satellite_name, s.snr_db, s.has_carrier, s.t)

    def _load_observations(self) -> None:
        cutoff = datetime.datetime.now() - datetime.timedelta(
            days=self._cfg.skymap_retention_days
        )
        loaded = 0
        pruned = 0
        for f in sorted(self._obs_dir.glob("*.json")):
            try:
                obs = SkyObservation.model_validate_json(
                    f.read_text(encoding="utf-8")
                )
            except (ValidationError, ValueError, OSError) as e:
                logger.warning("skymap: quarantining corrupt obs %s: %s", f, e)
                _quarantine(f)
                continue
            if obs.recorded_at < cutoff:
                try:
                    f.unlink()
                    pruned += 1
                except OSError:
                    logger.exception("skymap: prune failed for %s", f)
                continue
            self._observations[obs.pass_id] = obs
            if not obs.silent:
                self._merge_into_aggregate(obs)
            loaded += 1
        if loaded or pruned:
            logger.info(
                "skymap: loaded %d observation(s); pruned %d (>%g d)",
                loaded,
                pruned,
                self._cfg.skymap_retention_days,
            )

    def _prune_old(self) -> None:
        cutoff = datetime.datetime.now() - datetime.timedelta(
            days=self._cfg.skymap_retention_days
        )
        stale_ids = [
            pid for pid, obs in self._observations.items() if obs.recorded_at < cutoff
        ]
        if not stale_ids:
            return
        for pid in stale_ids:
            self._observations.pop(pid, None)
            try:
                (self._obs_dir / f"{pid}.json").unlink(missing_ok=True)
            except OSError:
                logger.exception("skymap: prune failed for %s", pid)
        # The aggregate has stale samples baked in — rebuild from survivors.
        self._cells.clear()
        for obs in self._observations.values():
            if not obs.silent:
                self._merge_into_aggregate(obs)
        logger.info("skymap: pruned %d aged observation(s)", len(stale_ids))

    async def _mark_done(self, pass_id: str) -> None:
        p = self._passes.get(pass_id)
        if p is None:
            return
        if CONSUMER_NAME in p.iq_consumers_pending:
            p.iq_consumers_pending = [
                n for n in p.iq_consumers_pending if n != CONSUMER_NAME
            ]
        if CONSUMER_NAME not in p.iq_consumers_done:
            p.iq_consumers_done.append(CONSUMER_NAME)
        await self._state.save_pass_async(p)
        await self._bus.publish(
            E.IqConsumerSettled(pass_id=pass_id, consumer_name=CONSUMER_NAME)
        )

    def aggregate(self, satellite: Optional[str] = None) -> Dict:
        cells: List[Dict] = []
        for (az_idx, el_idx), cell in self._cells.items():
            view = cell.view(satellite)
            if view is None:
                continue
            cells.append(
                {
                    "az_idx": az_idx,
                    "el_idx": el_idx,
                    "az_deg": (az_idx + 0.5) * AZ_BIN_DEG,
                    "el_deg": (el_idx + 0.5) * EL_BIN_DEG,
                    **view,
                }
            )
        sats = sorted({obs.satellite_name for obs in self._observations.values()})
        silent_passes = sum(1 for o in self._observations.values() if o.silent)
        return {
            "az_bin_deg": AZ_BIN_DEG,
            "el_bin_deg": EL_BIN_DEG,
            "n_az_bins": N_AZ_BINS,
            "n_el_bins": N_EL_BINS,
            "cells": cells,
            "satellites": sats,
            "total_passes": len(self._observations),
            "silent_passes": silent_passes,
            "retention_days": self._cfg.skymap_retention_days,
        }

    def recent_tracks(
        self, satellite: Optional[str] = None, limit: int = 30
    ) -> List[Dict]:
        obs = sorted(
            self._observations.values(),
            key=lambda o: o.recorded_at,
            reverse=True,
        )
        out: List[Dict] = []
        for o in obs:
            if satellite and o.satellite_name != satellite:
                continue
            out.append(
                {
                    "pass_id": o.pass_id,
                    "satellite_name": o.satellite_name,
                    "recorded_at": o.recorded_at.isoformat(),
                    "silent": o.silent,
                    "samples": [
                        {
                            "az": s.az,
                            "el": s.el,
                            "snr_db": s.snr_db,
                            "has_carrier": s.has_carrier,
                        }
                        for s in o.samples
                    ],
                }
            )
            if len(out) >= limit:
                break
        return out


def _row_snr(samples: np.ndarray) -> Tuple[float, bool]:
    """Average a few non-overlapping FFTs over the chunk, return
    ``(peak − floor, has_carrier)`` in dB. Auto-calibrating: floor is the
    median of the per-bin power for this row, so the absolute scale of the
    SDR drops out."""
    n = samples.size
    if n < FFT_SIZE:
        return 0.0, False
    n_avg = min(FFTS_PER_SECOND, n // FFT_SIZE)
    stride = n // n_avg
    accum: Optional[np.ndarray] = None
    window = np.hanning(FFT_SIZE).astype(np.float32)
    for i in range(n_avg):
        start = i * stride
        seg = samples[start : start + FFT_SIZE] * window
        spec = np.fft.fftshift(np.fft.fft(seg))
        power = (spec.real ** 2) + (spec.imag ** 2)
        accum = power if accum is None else accum + power
    assert accum is not None
    accum = accum / n_avg
    spec_db = 10.0 * np.log10(accum + 1e-20)
    floor = float(np.median(spec_db))
    peak = float(np.max(spec_db))
    mad = float(np.median(np.abs(spec_db - floor)))
    snr = peak - floor
    threshold = max(CARRIER_K_MAD * mad, CARRIER_MIN_SNR_DB)
    return snr, snr > threshold


def _quarantine(path: Path) -> None:
    quarantine_dir = path.parent.parent / "corrupt"
    try:
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        target = quarantine_dir / f"{stamp}-{path.name}"
        path.rename(target)
    except OSError:
        logger.exception("skymap: quarantine failed for %s", path)
