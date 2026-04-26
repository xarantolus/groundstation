from __future__ import annotations

import textwrap

import pytest

from auto.config import load_config
from auto.models import Decoder, Pass, PassInfo, Satellite, should_upload_iq
import datetime as _dt


def _waterfall() -> Decoder:
    return Decoder(container="waterfall:latest")


def _real(name: str = "ax100") -> Decoder:
    return Decoder(name=name, container="ax100:latest", min_size_bytes=1)


def _make_sat(*, iq_upload: str, decoders: list[Decoder]) -> Satellite:
    return Satellite(
        name="TEST",
        norad="12345",
        frequency=1.0,
        bandwidth=1.0,
        sample_rate=1.0,
        iq_upload=iq_upload,
        decoder=decoders,
    )


def _make_pass(sat: Satellite, *, decoders_done: list[int]) -> Pass:
    info = PassInfo(
        start_time=_dt.datetime(2026, 1, 1, 0, 0, 0),
        max_time=_dt.datetime(2026, 1, 1, 0, 5, 0),
        end_time=_dt.datetime(2026, 1, 1, 0, 10, 0),
        start_elevation=0.0, max_elevation=45.0, end_elevation=0.0,
        start_azimuth=0.0, max_azimuth=180.0, end_azimuth=359.0,
        duration_minutes=10.0, tle1="x", tle2="y",
    )
    return Pass(
        id="p1", satellite=sat, pass_info=info, pass_dir="/tmp/x",
        decoders_done=decoders_done,
    )


# ---- should_upload_iq matrix ----

def test_never_skips_even_if_decoders_succeeded():
    sat = _make_sat(iq_upload="never", decoders=[_waterfall(), _real()])
    p = _make_pass(sat, decoders_done=[0, 1])
    assert should_upload_iq(p) is False


def test_always_uploads_even_if_nothing_succeeded():
    sat = _make_sat(iq_upload="always", decoders=[_waterfall(), _real()])
    p = _make_pass(sat, decoders_done=[])
    assert should_upload_iq(p) is True


def test_on_decode_uploads_when_real_decoder_succeeded():
    sat = _make_sat(iq_upload="on_decode", decoders=[_waterfall(), _real()])
    p = _make_pass(sat, decoders_done=[1])
    assert should_upload_iq(p) is True


def test_on_decode_skips_when_only_waterfall_succeeded():
    sat = _make_sat(iq_upload="on_decode", decoders=[_waterfall(), _real()])
    p = _make_pass(sat, decoders_done=[0])
    assert should_upload_iq(p) is False


def test_on_decode_skips_when_no_decoder_succeeded():
    sat = _make_sat(iq_upload="on_decode", decoders=[_waterfall(), _real()])
    p = _make_pass(sat, decoders_done=[])
    assert should_upload_iq(p) is False


def test_always_with_no_decoders_still_skips():
    # `always` is meaningful, but we never upload IQ for a satellite with
    # no decoders at all (the file would be orphaned on the NAS).
    sat = _make_sat(iq_upload="always", decoders=[])
    p = _make_pass(sat, decoders_done=[])
    assert should_upload_iq(p) is False


# ---- model-level acceptance ----

@pytest.mark.parametrize("mode", ["never", "on_decode", "always"])
def test_model_accepts_all_three_modes(mode):
    sat = _make_sat(iq_upload=mode, decoders=[_real()])
    assert sat.iq_upload == mode


def test_model_rejects_unknown_mode():
    with pytest.raises(Exception):
        _make_sat(iq_upload="sometimes", decoders=[_real()])


# ---- config-level validation ----

_BASE_YAML = """
nas_directory: /tmp
satellites:
  - name: T
    norad: 1
    frequency: 1.0
    bandwidth: 1.0
    sample_rate: 1.0
    iq_upload: {iq_upload}
    decoder:
{decoders}
"""

_WATERFALL_ONLY = "      - container: waterfall:latest\n"
_REAL_ONLY = "      - name: ax100\n        container: ax100:latest\n"
_BOTH = _WATERFALL_ONLY + _REAL_ONLY
_NONE = "      []\n"  # represented via plain empty list trick


def _write(tmp_path, body: str):
    f = tmp_path / "config.yml"
    f.write_text(textwrap.dedent(body))
    return str(f)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("LOCATION_LAT", "0")
    monkeypatch.setenv("LOCATION_LON", "0")
    monkeypatch.setenv("LOCATION_ALT", "0")


def test_config_rejects_on_decode_with_only_waterfall(env, tmp_path):
    path = _write(tmp_path, _BASE_YAML.format(iq_upload="on_decode", decoders=_WATERFALL_ONLY))
    with pytest.raises(SystemExit):
        load_config(path)


def test_config_accepts_on_decode_with_real_decoder(env, tmp_path):
    path = _write(tmp_path, _BASE_YAML.format(iq_upload="on_decode", decoders=_BOTH))
    cfg = load_config(path)
    assert cfg.satellites[0].iq_upload == "on_decode"


def test_config_accepts_never_with_only_waterfall(env, tmp_path):
    path = _write(tmp_path, _BASE_YAML.format(iq_upload="never", decoders=_WATERFALL_ONLY))
    cfg = load_config(path)
    assert cfg.satellites[0].iq_upload == "never"


def test_config_rejects_always_with_no_decoders(env, tmp_path):
    yml = """
    nas_directory: /tmp
    satellites:
      - name: T
        norad: 1
        frequency: 1.0
        bandwidth: 1.0
        sample_rate: 1.0
        iq_upload: always
    """
    path = _write(tmp_path, yml)
    with pytest.raises(SystemExit):
        load_config(path)
