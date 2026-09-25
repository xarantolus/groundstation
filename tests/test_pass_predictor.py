from __future__ import annotations

import json
from unittest import mock

import pytest
import requests

from auto import orbit
from auto.pass_predictor import PassPredictor

OMM = {
    "OBJECT_NAME": "AISTECHSAT 2",
    "OBJECT_ID": "2018-099L",
    "EPOCH": "2026-09-24T02:29:24.171072",
    "MEAN_MOTION": 15.16286078,
    "ECCENTRICITY": 0.00035556,
    "INCLINATION": 97.4199,
    "RA_OF_ASC_NODE": 312.9775,
    "ARG_OF_PERICENTER": 334.262,
    "MEAN_ANOMALY": 25.8435,
    "EPHEMERIS_TYPE": 0,
    "CLASSIFICATION_TYPE": "U",
    "NORAD_CAT_ID": 43768,
    "ELEMENT_SET_NO": 999,
    "REV_AT_EPOCH": 42726,
    "BSTAR": 0.00020347909,
    "MEAN_MOTION_DOT": 3.874e-5,
    "MEAN_MOTION_DDOT": 0,
}
OMM_TEXT = json.dumps([OMM])


def _ok_response(text: str) -> mock.Mock:
    return mock.Mock(text=text, raise_for_status=lambda: None)


def test_parse_omm_json_handles_garbage():
    assert orbit.parse_omm_json(OMM_TEXT) == OMM
    assert orbit.parse_omm_json("") is None
    assert orbit.parse_omm_json("No GP data found") is None
    assert orbit.parse_omm_json("[]") is None


def test_tle_to_omm_matches_sgp4_elements():
    from sgp4.api import Satrec

    tle1 = "1 25544U 98067A   08264.51782528 -.00002182  00000-0 -11606-4 0  2927"
    tle2 = "2 25544  51.6416 247.4627 0006703 130.5360 325.0288 15.72125391563537"
    a = Satrec.twoline2rv(tle1, tle2)
    b = orbit.make_satellite(orbit.tle_to_omm(tle1, tle2)).model
    for attr in ("ecco", "inclo", "nodeo", "argpo", "mo", "no_kozai", "bstar", "jdsatepoch"):
        assert getattr(a, attr) == pytest.approx(getattr(b, attr), rel=1e-9, abs=1e-12)


def test_six_digit_catalog_number_propagates():
    omm = dict(OMM, NORAD_CAT_ID=123456)
    az, el = orbit.compute_azel(omm, 52.0, 13.0, 50.0, __import__("datetime").datetime(2026, 9, 24, 12, 0, 0))
    assert 0 <= az < 360 and -90 <= el <= 90


def test_successful_fetch_persists_to_disk(tmp_path):
    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", return_value=_ok_response(OMM_TEXT)) as g:
        assert p.fetch_omm("43768") == OMM
    assert "FORMAT=json" in g.call_args.args[0]
    assert json.loads((tmp_path / "43768.json").read_text()) == OMM


def test_falls_back_to_disk_when_network_down(tmp_path):
    with mock.patch("requests.get", return_value=_ok_response(OMM_TEXT)):
        PassPredictor(cache_dir=tmp_path).fetch_omm("43768")

    # A fresh predictor (e.g. after a restart) with celestrak unreachable must
    # reuse the on-disk elements rather than failing.
    p2 = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        assert p2.fetch_omm("43768") == OMM


def test_disk_fallback_is_not_memo_cached_so_network_recovers(tmp_path):
    with mock.patch("requests.get", return_value=_ok_response(OMM_TEXT)):
        PassPredictor(cache_dir=tmp_path).fetch_omm("43768")

    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        assert p.fetch_omm("43768") == OMM  # served from disk

    # celestrak recovers with newer elements — the disk fallback must not have
    # poisoned the in-memory cache, so a real fetch happens and wins.
    newer = dict(OMM, ELEMENT_SET_NO=1000)
    with mock.patch("requests.get", return_value=_ok_response(json.dumps([newer]))) as g:
        assert p.fetch_omm("43768") == newer
        assert g.called


def test_raises_when_uncached_and_network_down(tmp_path):
    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        with pytest.raises(RuntimeError):
            p.fetch_omm("99999")


def test_memory_cache_avoids_refetch_within_run(tmp_path):
    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", return_value=_ok_response(OMM_TEXT)) as g:
        p.fetch_omm("43768")
        p.fetch_omm("43768")
        assert g.call_count == 1
