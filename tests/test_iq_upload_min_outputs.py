from __future__ import annotations

import datetime as _dt

import pytest

from auto.models import Decoder, Pass, PassInfo, Satellite, should_upload_iq


def _pass(mode: str, outputs: int, done: bool = True) -> Pass:
    info = PassInfo(
        start_time=_dt.datetime(2026, 1, 1), max_time=_dt.datetime(2026, 1, 1, 0, 5),
        end_time=_dt.datetime(2026, 1, 1, 0, 10), start_elevation=0, max_elevation=45,
        end_elevation=0, start_azimuth=0, max_azimuth=90, end_azimuth=180,
        duration_minutes=10, omm={},
    )
    sat = Satellite(
        name="SAT", norad="1", frequency=1.0, bandwidth=1.0, sample_rate=1.0,
        iq_upload=mode, decoder=[Decoder(name="dec", role="decoder", container="x")],
    )
    return Pass(
        id="P", satellite=sat, pass_info=info, pass_dir="/tmp/p",
        decoders_done=[0] if done else [], max_decoder_outputs=outputs,
    )


@pytest.mark.parametrize(
    "mode,outputs,min_outputs,upload",
    [
        ("never", 268, None, False),  # rule off: old behaviour
        ("never", 268, 50, True),  # many packets: keep the IQ
        ("never", 50, 50, True),  # inclusive
        ("never", 49, 50, False),
        ("on_decode", 3, 50, True),  # on_decode still uploads on any output
        ("always", 0, 50, True),
    ],
)
def test_should_upload_iq_min_outputs(mode, outputs, min_outputs, upload):
    assert should_upload_iq(_pass(mode, outputs), min_outputs) is upload
