from __future__ import annotations

import datetime
import logging
from typing import Dict, List, Optional, Tuple

import ephem
import requests

from .models import PassInfo, Satellite

logger = logging.getLogger("groundstation.predictor")


class PassPredictor:
    def __init__(self, n2yo_api_key: Optional[str] = None) -> None:
        self._n2yo_api_key = n2yo_api_key
        self._tle_cache: Dict[str, Tuple[str, str]] = {}

    def fetch_tle(self, norad: str) -> Tuple[str, str]:
        if norad in self._tle_cache:
            return self._tle_cache[norad]

        tle_text: Optional[str] = None
        try:
            r = requests.get(
                f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad}&FORMAT=TLE",
                timeout=10,
            )
            r.raise_for_status()
            tle_text = r.text
        except requests.RequestException as e:
            logger.error("TLE fetch from celestrak failed for %s: %s", norad, e)
            if self._n2yo_api_key:
                try:
                    r = requests.get(
                        f"https://api.n2yo.com/rest/v1/satellite/tle/{norad}/?apiKey={self._n2yo_api_key}",
                        timeout=10,
                    )
                    r.raise_for_status()
                    tle_text = r.json().get("tle", "")
                except Exception as ex:
                    raise RuntimeError(f"could not fetch TLE for {norad}") from ex
            else:
                raise RuntimeError(f"could not fetch TLE for {norad}") from e

        lines = [line.strip() for line in (tle_text or "").splitlines() if line.strip()]
        if len(lines) >= 3:
            tle = (lines[-2], lines[-1])
        elif len(lines) == 2:
            tle = (lines[0], lines[1])
        else:
            raise RuntimeError(f"unexpected TLE format for {norad}")

        self._tle_cache[norad] = tle
        return tle

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
        tle1, tle2 = self.fetch_tle(sat.norad)

        observer = ephem.Observer()
        observer.lat = str(lat)
        observer.lon = str(lon)
        observer.elev = alt_m / 1000
        observer.horizon = "0"

        start_date = ephem.now() - ephem.minute * 90
        observer.date = start_date
        end_date = start_date + ephem.hour * hours
        sat_body = ephem.readtle(sat.name, tle1, tle2)

        passes: List[PassInfo] = []
        current_date = start_date
        while current_date < end_date:
            try:
                observer.date = current_date
                np = observer.next_pass(sat_body)
                if np is None or np[0] > end_date:
                    break
                rise_time, rise_az, max_time, max_el, set_time, set_az = np
                current_date = set_time + ephem.minute

                if max_el * 180 / ephem.pi < threshold_deg:
                    continue

                def crossing(a: ephem.Date, b: ephem.Date, upward: bool) -> Optional[ephem.Date]:
                    step = (b - a) / 100
                    for i in range(100):
                        t = a + step * i
                        observer.date = t
                        sat_body.compute(observer)
                        el_deg = float(sat_body.alt) * 180 / ephem.pi
                        if upward and el_deg >= pass_start_threshold_deg:
                            return t
                        if not upward and el_deg <= pass_start_threshold_deg:
                            return t
                    return None

                ascending = crossing(rise_time, max_time, upward=True)
                descending = crossing(max_time, set_time, upward=False)
                start_time = ascending if ascending else rise_time
                end_time = descending if descending else set_time

                observer.date = start_time
                sat_body.compute(observer)
                start_el = float(sat_body.alt) * 180 / ephem.pi
                start_az = float(sat_body.az) * 180 / ephem.pi

                observer.date = max_time
                sat_body.compute(observer)
                max_el_deg = float(sat_body.alt) * 180 / ephem.pi
                max_az = float(sat_body.az) * 180 / ephem.pi

                observer.date = end_time
                sat_body.compute(observer)
                end_el = float(sat_body.alt) * 180 / ephem.pi
                end_az = float(sat_body.az) * 180 / ephem.pi

                passes.append(
                    PassInfo(
                        start_time=ephem.localtime(start_time),
                        max_time=ephem.localtime(max_time),
                        end_time=ephem.localtime(end_time),
                        start_elevation=start_el,
                        max_elevation=max_el_deg,
                        end_elevation=end_el,
                        start_azimuth=start_az,
                        max_azimuth=max_az,
                        end_azimuth=end_az,
                        duration_minutes=(end_time - start_time) * 24 * 60,
                        tle1=tle1,
                        tle2=tle2,
                    )
                )
            except Exception:
                logger.exception("error calculating pass for %s", sat.name)
                current_date += ephem.minute

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
