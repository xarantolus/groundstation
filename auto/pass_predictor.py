from __future__ import annotations

import datetime
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import json

import numpy as np
import requests

from . import orbit
from .models import PassInfo, Satellite
from .orbit import Omm, compute_azel  # noqa: F401  (compute_azel re-exported)

logger = logging.getLogger("groundstation.predictor")

# Coarse search starts this far in the past so a pass already in progress at
# startup is still picked up.
LOOKBACK = datetime.timedelta(minutes=90)


class PassPredictor:
    def __init__(
        self,
        n2yo_api_key: Optional[str] = None,
        cache_dir: Optional[Union[str, Path]] = None,
    ) -> None:
        self._n2yo_api_key = n2yo_api_key
        self._omm_cache: Dict[str, Omm] = {}
        # Last-known-good elements persisted to disk so a celestrak outage (or
        # a restart during one) doesn't leave us with zero elements — and
        # therefore zero predicted passes and a blank map. On a network
        # failure we fall back to the on-disk copy: the same elements last
        # used to compute this satellite's overpasses.
        self._cache_dir: Optional[Path] = Path(cache_dir) if cache_dir else None

    def _disk_path(self, norad: str) -> Optional[Path]:
        if self._cache_dir is None:
            return None
        return self._cache_dir / f"{norad}.json"

    def _save_omm_disk(self, norad: str, omm: Omm) -> None:
        path = self._disk_path(norad)
        if path is None:
            return
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[union-attr]
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(json.dumps(omm), encoding="utf-8")
            tmp.replace(path)
        except OSError:
            logger.warning("could not persist OMM for %s to disk", norad, exc_info=True)

    def _load_omm_disk(self, norad: str) -> Optional[Omm]:
        path = self._disk_path(norad)
        if path is None:
            return None
        try:
            omm = orbit.parse_omm_json(path.read_text(encoding="utf-8"))
            age_h = (time.time() - path.stat().st_mtime) / 3600.0
        except OSError:
            return None
        if omm is not None:
            logger.warning(
                "using cached on-disk OMM for %s (%.1f h old) — network unavailable",
                norad,
                age_h,
            )
        return omm

    def fetch_omm(self, norad: str) -> Omm:
        if norad in self._omm_cache:
            return self._omm_cache[norad]

        omm: Optional[Omm] = None
        try:
            r = requests.get(
                f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad}&FORMAT=json",
                timeout=10,
            )
            r.raise_for_status()
            omm = orbit.parse_omm_json(r.text)
        except requests.RequestException as e:
            logger.error("OMM fetch from celestrak failed for %s: %s", norad, e)
            if self._n2yo_api_key:
                # n2yo only serves TLEs (so only 5-digit catalog numbers);
                # convert to OMM so everything downstream sees one format.
                try:
                    r = requests.get(
                        f"https://api.n2yo.com/rest/v1/satellite/tle/{norad}/?apiKey={self._n2yo_api_key}",
                        timeout=10,
                    )
                    r.raise_for_status()
                    lines = [
                        ln.strip()
                        for ln in r.json().get("tle", "").splitlines()
                        if ln.strip()
                    ]
                    if len(lines) >= 2:
                        omm = orbit.tle_to_omm(lines[-2], lines[-1])
                except Exception as ex:
                    logger.error("n2yo TLE fetch failed for %s: %s", norad, ex)

        if omm is not None:
            # Fresh elements: cache in memory for this run and persist for later.
            self._omm_cache[norad] = omm
            self._save_omm_disk(norad, omm)
            return omm

        # Network/parse failed — fall back to the last-known-good on disk.
        # Deliberately not stored in _omm_cache so the next prediction cycle
        # retries the network and recovers once celestrak is reachable again.
        disk = self._load_omm_disk(norad)
        if disk is not None:
            return disk

        raise RuntimeError(
            f"could not fetch OMM for {norad} and no cached copy on disk"
        )

    def passes_for(
        self,
        sat: Satellite,
        lat: float,
        lon: float,
        alt_m: float,
        threshold_deg: float,
        pass_start_threshold_deg: float,
        hours: float,
    ) -> List[PassInfo]:
        omm = self.fetch_omm(sat.norad)

        ts = orbit.timescale()
        topos = orbit.observer(lat, lon, alt_m)
        body = orbit.make_satellite(omm)

        start = datetime.datetime.now(datetime.timezone.utc) - LOOKBACK
        t0 = ts.from_datetime(start)
        t1 = ts.from_datetime(start + datetime.timedelta(hours=hours))
        times, events = body.find_events(topos, t0, t1, altitude_degrees=0.0)

        def sample(tt: float):
            t = ts.tt_jd(tt)
            el, az = orbit.altaz(body, topos, t)
            return t, float(el[0]), float(az[0])

        def crossing(a: float, b: float, upward: bool) -> Optional[float]:
            grid = np.linspace(a, b, 100, endpoint=False)
            el, _ = orbit.altaz(body, topos, ts.tt_jd(grid))
            hit = np.nonzero(
                el >= pass_start_threshold_deg
                if upward
                else el <= pass_start_threshold_deg
            )[0]
            return float(grid[hit[0]]) if hit.size else None

        passes: List[PassInfo] = []
        # find_events yields rise(0)/culminate(1)/set(2); only complete
        # rise→culminate→set triples are usable passes.
        i = 0
        while i + 2 < len(events):
            if list(events[i : i + 3]) != [0, 1, 2]:
                i += 1
                continue
            rise, culm, sett = (times[i].tt, times[i + 1].tt, times[i + 2].tt)
            i += 3
            try:
                _, max_el, max_az = sample(culm)
                if max_el < threshold_deg:
                    continue

                ascending = crossing(rise, culm, upward=True)
                descending = crossing(culm, sett, upward=False)
                start_tt = ascending if ascending is not None else rise
                end_tt = descending if descending is not None else sett

                st, start_el, start_az = sample(start_tt)
                et, end_el, end_az = sample(end_tt)

                passes.append(
                    PassInfo(
                        start_time=orbit.to_local_naive(st),
                        max_time=orbit.to_local_naive(ts.tt_jd(culm)),
                        end_time=orbit.to_local_naive(et),
                        start_elevation=start_el,
                        max_elevation=max_el,
                        end_elevation=end_el,
                        start_azimuth=start_az,
                        max_azimuth=max_az,
                        end_azimuth=end_az,
                        duration_minutes=(end_tt - start_tt) * 24 * 60,
                        omm=omm,
                    )
                )
            except Exception:
                logger.exception("error calculating pass for %s", sat.name)

        return passes

    def predict_all(
        self,
        sats: List[Satellite],
        lat: float,
        lon: float,
        alt_m: float,
        threshold: float,
        pass_start_threshold: float,
        hours: float,
    ) -> List[Tuple[Satellite, PassInfo]]:
        all_passes: List[Tuple[Satellite, PassInfo]] = []
        for sat in sats:
            try:
                for p in self.passes_for(
                    sat, lat, lon, alt_m, threshold, pass_start_threshold, hours
                ):
                    all_passes.append((sat, p))
            except Exception:
                logger.exception("error predicting for %s", sat.name)

        all_passes.sort(key=lambda x: x[1].start_time)
        return prioritize(all_passes)


ELEVATION_SCORE_DIVISOR = 30.0
MIN_RECORDING_WINDOW = datetime.timedelta(minutes=2)
# Edge of a pass spent at low elevation; signal there is rarely useful, so
# don't accept a trim that lands inside it.
USELESS_EDGE = datetime.timedelta(minutes=1)
# Time-slicing granularity for score-based overlap allocation. 30s is fine
# enough that a crossover lands on a slice boundary within rounding error
# even for fast LEO passes, and coarse enough that a 15-minute cluster only
# evaluates ~30 slices per pass.
SLICE_SECONDS = 30


def _pass_score(sat: Satellite, pi: PassInfo) -> float:
    # D=30: a 1-tier priority gap is flipped only when elevation differs
    # by 30°. Keeps image sats (4-tier gap) unflippable while letting a
    # high-elevation pass beat a same-class neighbour with a poor track.
    return sat.priority + pi.max_elevation / ELEVATION_SCORE_DIVISOR


def _elevation_at(pi: PassInfo, t: datetime.datetime) -> float:
    """Linear interpolation of the pass elevation at time ``t``. Returns the
    edge value if ``t`` falls outside the pass window. Two-segment triangle
    (start→max, max→end) — close enough for scoring; we don't need the full
    sub-degree astronomy here."""
    if t <= pi.start_time:
        return pi.start_elevation
    if t >= pi.end_time:
        return pi.end_elevation
    if t <= pi.max_time:
        denom = max(1.0, (pi.max_time - pi.start_time).total_seconds())
        frac = (t - pi.start_time).total_seconds() / denom
        return pi.start_elevation + frac * (pi.max_elevation - pi.start_elevation)
    denom = max(1.0, (pi.end_time - pi.max_time).total_seconds())
    frac = (t - pi.max_time).total_seconds() / denom
    return pi.max_elevation + frac * (pi.end_elevation - pi.max_elevation)


def _score_at(sat: Satellite, pi: PassInfo, t: datetime.datetime) -> float:
    return sat.priority + _elevation_at(pi, t) / ELEVATION_SCORE_DIVISOR


def _slice_cluster(
    cluster: List[Tuple[Satellite, PassInfo]],
) -> List[Tuple[Satellite, PassInfo]]:
    """Time-slice an overlapping cluster by per-moment score: each 30s slot
    goes to whichever pass has the highest priority+interpolated-elevation
    at that moment. Each pass keeps its single longest contiguous winning
    region — the recorder model takes one window per pass, not multiple.
    Adjacent winners' windows abut at the score crossover so the recorder's
    handoff happens naturally there."""
    cluster_start = min(p.start_time for _, p in cluster)
    cluster_end = max(p.end_time for _, p in cluster)
    slice_dt = datetime.timedelta(seconds=SLICE_SECONDS)

    # Walk the cluster window slot-by-slot, recording the winner per slot.
    slots: List[Tuple[datetime.datetime, datetime.datetime, int]] = []
    t = cluster_start
    while t < cluster_end:
        slot_end = min(t + slice_dt, cluster_end)
        mid = t + (slot_end - t) / 2
        winner = -1
        best = float("-inf")
        for idx, (sat, pi) in enumerate(cluster):
            if mid < pi.start_time or mid >= pi.end_time:
                continue
            s = _score_at(sat, pi, mid)
            if s > best:
                best = s
                winner = idx
        slots.append((t, slot_end, winner))
        t = slot_end

    # For each pass, find the longest run of contiguous winning slots.
    longest: Dict[int, Tuple[datetime.datetime, datetime.datetime]] = {}
    run_start: Optional[datetime.datetime] = None
    run_idx = -2  # sentinel that doesn't match any real winner
    for s_start, s_end, idx in slots + [(cluster_end, cluster_end, -2)]:
        if idx != run_idx:
            if run_idx >= 0 and run_start is not None:
                run_end = s_start
                cur = longest.get(run_idx)
                if cur is None or (run_end - run_start) > (cur[1] - cur[0]):
                    longest[run_idx] = (run_start, run_end)
            run_idx = idx
            run_start = s_start

    picked: List[Tuple[Satellite, PassInfo]] = []
    for idx, (sat, pi) in enumerate(cluster):
        win = longest.get(idx)
        if win is None:
            logger.info("overlap: %s lost every slot to higher-scoring passes", sat.name)
            continue
        win_start, win_end = win

        is_trimmed = win_start > pi.start_time or win_end < pi.end_time
        if is_trimmed:
            useful_start = pi.start_time + USELESS_EDGE
            useful_end = pi.end_time - USELESS_EDGE
            win_start = max(win_start, useful_start)
            win_end = min(win_end, useful_end)

        if win_end - win_start < MIN_RECORDING_WINDOW:
            logger.info(
                "overlap: skipped %s — only %ds in window after edge filter",
                sat.name,
                int((win_end - win_start).total_seconds()),
            )
            continue

        trim_start = win_start if win_start > pi.start_time else None
        trim_end = win_end if win_end < pi.end_time else None
        if trim_start is not None or trim_end is not None:
            new_pi = pi.model_copy(
                update={
                    "recording_start_override": trim_start,
                    "recording_end_override": trim_end,
                }
            )
            logger.info(
                "overlap: %s trimmed to %s–%s (was %s–%s, max el %.0f°)",
                sat.name,
                win_start.strftime("%H:%M:%S"),
                win_end.strftime("%H:%M:%S"),
                pi.start_time.strftime("%H:%M:%S"),
                pi.end_time.strftime("%H:%M:%S"),
                pi.max_elevation,
            )
            picked.append((sat, new_pi))
        else:
            logger.info(
                "overlap: %s kept full window (max el %.0f°)",
                sat.name,
                pi.max_elevation,
            )
            picked.append((sat, pi))

    return picked


def prioritize(
    passes: List[Tuple[Satellite, PassInfo]],
) -> List[Tuple[Satellite, PassInfo]]:
    picked: List[Tuple[Satellite, PassInfo]] = []
    i = 0
    n = len(passes)
    while i < n:
        cluster_end = passes[i][1].end_time
        j = i + 1
        while j < n and passes[j][1].start_time < cluster_end:
            cluster_end = max(cluster_end, passes[j][1].end_time)
            j += 1
        cluster = passes[i:j]

        if len(cluster) == 1:
            picked.append(cluster[0])
            i = j
            continue

        considered = ", ".join(
            f"{s.name}@{p.max_elevation:.0f}°" for s, p in cluster
        )
        logger.info("overlap: %d passes (%s) — score-slicing", len(cluster), considered)
        picked.extend(_slice_cluster(cluster))
        i = j
    return picked


def azimuth_to_compass(azimuth: float) -> str:
    sectors = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int(((azimuth + 22.5) % 360) / 45)
    return sectors[idx]


def build_pass_id(sat: Satellite, pass_info: PassInfo) -> str:
    from .models import Pass

    return Pass.make_id(sat, pass_info)


def now() -> datetime.datetime:
    return datetime.datetime.now()
