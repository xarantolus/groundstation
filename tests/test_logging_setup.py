from __future__ import annotations

import logging
import logging.handlers

from auto.main import LOG_KEEP_DAYS, daily_log_handler


def _write(handler: logging.Handler, msg: str) -> None:
    handler.emit(logging.LogRecord("t", logging.INFO, __file__, 1, msg, None, None))
    handler.close()


def test_restart_appends_instead_of_replacing_the_log(tmp_path):
    _write(daily_log_handler("tracker.log", "%(message)s", log_dir=str(tmp_path / "logs")), "first run")
    _write(daily_log_handler("tracker.log", "%(message)s", log_dir=str(tmp_path / "logs")), "second run")
    assert (tmp_path / "logs" / "tracker.log").read_text().splitlines() == ["first run", "second run"]


def test_log_rotates_daily_and_keeps_a_day(tmp_path):
    h = daily_log_handler("decoders.log", "%(message)s", log_dir=str(tmp_path))
    try:
        assert isinstance(h, logging.handlers.TimedRotatingFileHandler)
        assert h.when == "MIDNIGHT"
        assert h.backupCount == LOG_KEEP_DAYS >= 1
    finally:
        h.close()
