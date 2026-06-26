from __future__ import annotations

from unittest import mock

import pytest
import requests

from auto.pass_predictor import PassPredictor, _parse_tle

TLE1 = "1 43768U 18099L   26116.44486358  .00006162  00000+0  33416-3 0  9997"
TLE2 = "2 43768  97.4347 165.6197 0006429 113.5898 246.6010 15.14907789404441"
TLE_TEXT = f"AISTECHSAT-2\n{TLE1}\n{TLE2}\n"


def _ok_response(text: str) -> mock.Mock:
    return mock.Mock(text=text, raise_for_status=lambda: None)


def test_parse_tle_handles_2le_3le_and_garbage():
    assert _parse_tle(f"{TLE1}\n{TLE2}") == (TLE1, TLE2)
    assert _parse_tle(TLE_TEXT) == (TLE1, TLE2)
    assert _parse_tle("") is None
    assert _parse_tle("only one line") is None


def test_successful_fetch_persists_to_disk(tmp_path):
    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", return_value=_ok_response(TLE_TEXT)):
        assert p.fetch_tle("43768") == (TLE1, TLE2)
    assert (tmp_path / "43768.tle").read_text().splitlines() == [TLE1, TLE2]


def test_falls_back_to_disk_when_network_down(tmp_path):
    # First predictor fetches and persists.
    with mock.patch("requests.get", return_value=_ok_response(TLE_TEXT)):
        PassPredictor(cache_dir=tmp_path).fetch_tle("43768")

    # A fresh predictor (e.g. after a restart) with celestrak unreachable must
    # reuse the on-disk elements rather than failing.
    p2 = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        assert p2.fetch_tle("43768") == (TLE1, TLE2)


def test_disk_fallback_is_not_memo_cached_so_network_recovers(tmp_path):
    with mock.patch("requests.get", return_value=_ok_response(TLE_TEXT)):
        PassPredictor(cache_dir=tmp_path).fetch_tle("43768")

    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        assert p.fetch_tle("43768") == (TLE1, TLE2)  # served from disk

    # celestrak recovers with newer elements — the disk fallback must not have
    # poisoned the in-memory cache, so a real fetch happens and wins.
    newer1 = TLE1.replace("9997", "9998")
    with mock.patch("requests.get", return_value=_ok_response(f"{newer1}\n{TLE2}")) as g:
        assert p.fetch_tle("43768") == (newer1, TLE2)
        assert g.called


def test_raises_when_uncached_and_network_down(tmp_path):
    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", side_effect=requests.ConnectionError("down")):
        with pytest.raises(RuntimeError):
            p.fetch_tle("99999")


def test_memory_cache_avoids_refetch_within_run(tmp_path):
    p = PassPredictor(cache_dir=tmp_path)
    with mock.patch("requests.get", return_value=_ok_response(TLE_TEXT)) as g:
        p.fetch_tle("43768")
        p.fetch_tle("43768")
        assert g.call_count == 1
