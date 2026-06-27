from __future__ import annotations

import datetime

from auto.web import _localize_iso, _slim_pass_for_wire


def test_localize_iso_attaches_local_offset_to_naive():
    # A naive timestamp gains the server's local offset but keeps the same
    # wall-clock reading; round-tripping back to local-naive is lossless.
    naive = "2026-06-27T19:52:26.125230"
    out = _localize_iso(naive)
    parsed = datetime.datetime.fromisoformat(out)
    assert parsed.tzinfo is not None
    assert parsed.replace(tzinfo=None) == datetime.datetime.fromisoformat(naive)


def test_localize_iso_is_idempotent_on_aware_and_passes_non_strings():
    aware = "2026-06-27T19:52:26+02:00"
    assert _localize_iso(aware) == aware  # already unambiguous, untouched
    assert _localize_iso(None) is None
    assert _localize_iso(123) == 123


def test_localize_iso_yields_a_timezone_independent_instant():
    # The whole point: a differently-zoned client must resolve the SAME instant.
    # An aware ISO string encodes one absolute instant; parsing it is not
    # affected by the reader's local zone, unlike the naive input.
    out = _localize_iso("2026-06-27T19:52:26")
    a = datetime.datetime.fromisoformat(out).astimezone(datetime.timezone.utc)
    b = datetime.datetime.fromisoformat(out).astimezone(
        datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    )
    assert a == b  # same absolute instant regardless of how it's viewed


def test_slim_pass_localizes_pass_info_times_and_trims():
    entry = {
        "id": "SAT_2026-06-27_19-52-26",
        "satellite": {"name": "SNIPE-4", "norad": "56744", "frequency": 137.1},
        "pass_info": {
            "start_time": "2026-06-27T19:52:26.125230",
            "max_time": "2026-06-27T19:58:00",
            "end_time": "2026-06-27T20:04:00",
            "recording_start_override": None,
            "recording_end_override": "2026-06-27T20:03:00",
            "tle1": "1 ...",
            "tle2": "2 ...",
        },
        "created_at": "2026-06-26T18:09:41",
        "decoders_pending": ["x"],
    }
    _slim_pass_for_wire(entry)

    # Slimming still happens.
    assert entry["satellite"] == {"name": "SNIPE-4", "norad": "56744"}
    assert "created_at" not in entry
    assert "decoders_pending" not in entry

    pi = entry["pass_info"]
    # Every present timestamp is now timezone-aware; null overrides stay null.
    for k in ("start_time", "max_time", "end_time", "recording_end_override"):
        assert datetime.datetime.fromisoformat(pi[k]).tzinfo is not None
    assert pi["recording_start_override"] is None
    # Non-time fields untouched.
    assert pi["tle1"] == "1 ..."
