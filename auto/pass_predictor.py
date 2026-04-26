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


def _pass_score(sat: Satellite, pi: PassInfo) -> float:
    # D=30: a 1-tier priority gap is flipped only when elevation differs
    # by 30°. Keeps image sats (4-tier gap) unflippable while letting a
    # high-elevation pass beat a same-class neighbour with a poor track.
    return sat.priority + pi.max_elevation / ELEVATION_SCORE_DIVISOR


def _largest_free_subwindow(
    window_start: datetime.datetime,
    window_end: datetime.datetime,
    occupied: List[Tuple[datetime.datetime, datetime.datetime]],
) -> Tuple[datetime.datetime, datetime.datetime]:
    cuts = sorted(
        (max(s, window_start), min(e, window_end))
        for s, e in occupied
        if s < window_end and e > window_start
    )
    best = (window_start, window_start)
    cursor = window_start
    for s, e in cuts:
        if s > cursor:
            piece = (cursor, s)
            if piece[1] - piece[0] > best[1] - best[0]:
                best = piece
        cursor = max(cursor, e)
    if window_end > cursor:
        piece = (cursor, window_end)
        if piece[1] - piece[0] > best[1] - best[0]:
            best = piece
    return best


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

        ranked = sorted(cluster, key=lambda t: _pass_score(*t), reverse=True)
        best_sat, best_pi = ranked[0]
        considered = ", ".join(t[0].name for t in cluster)
        logger.info(
            "overlap: %d passes (%s), selected %s",
            len(cluster),
            considered,
            best_sat.name,
        )
        picked.append((best_sat, best_pi))
        occupied: List[Tuple[datetime.datetime, datetime.datetime]] = [
            (best_pi.start_time, best_pi.end_time)
        ]

        for cand_sat, cand_pi in ranked[1:]:
            useful_start = cand_pi.start_time + USELESS_EDGE
            useful_end = cand_pi.end_time - USELESS_EDGE
            if useful_end - useful_start < MIN_RECORDING_WINDOW:
                logger.info(
                    "overlap: skipped %s — useful window too short",
                    cand_sat.name,
                )
                continue
            sub_start, sub_end = _largest_free_subwindow(
                useful_start, useful_end, occupied
            )
            free = sub_end - sub_start
            if free < MIN_RECORDING_WINDOW:
                logger.info(
                    "overlap: skipped %s — only %ds free in window",
                    cand_sat.name,
                    int(free.total_seconds()),
                )
                continue
            trimmed_pi = cand_pi
            trim_start = sub_start if sub_start > cand_pi.start_time else None
            trim_end = sub_end if sub_end < cand_pi.end_time else None
            if trim_start is not None or trim_end is not None:
                trimmed_pi = cand_pi.model_copy(
                    update={
                        "recording_start_override": trim_start,
                        "recording_end_override": trim_end,
                    }
                )
                logger.info(
                    "overlap: trimmed %s to %s–%s (was %s–%s)",
                    cand_sat.name,
                    sub_start.strftime("%H:%M:%S"),
                    sub_end.strftime("%H:%M:%S"),
                    cand_pi.start_time.strftime("%H:%M:%S"),
                    cand_pi.end_time.strftime("%H:%M:%S"),
                )
            picked.append((cand_sat, trimmed_pi))
            occupied.append((sub_start, sub_end))

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
