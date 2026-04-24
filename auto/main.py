from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import os
import signal
import sys
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

from . import events as E
from .bus import EventBus, event_types, run_subscriber
from .config import load_config
from .decode_gate import DecodeGate
from .decoder import DecoderService
from .logging_bus import BusLogHandler
from .models import GroundstationConfig, Pass, PassStatus, TransferRequest
from .pass_predictor import PassPredictor
from .recorder import RecorderService
from .scheduler import SchedulerService
from .state import StateStore
from .transfer import TransferService
from .tui import TUIService
from .view import ViewModel
from .web import WebService


logger = logging.getLogger("groundstation")


def _setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler("tracker.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(fh)


def _attach_bus_logging(bus: EventBus, loop: asyncio.AbstractEventLoop) -> None:
    root = logging.getLogger()
    root.addHandler(BusLogHandler(bus, loop))


def _recover_interrupted_pass(
    p: Pass, state: StateStore
) -> Optional[TransferRequest]:
    """Handle a pass that was RECORDING when the groundstation died.
    Marks it RECORDED_PARTIAL, annotates info.json, and (optionally) returns
    a TransferRequest for the partial IQ. Does NOT enqueue decoders."""
    p.status = PassStatus.RECORDED_PARTIAL
    p.interrupted = True
    state.save_pass(p)

    info_path = Path(p.pass_dir) / "info.json"
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            info = {}
        info["interrupted"] = True
        info["interrupted_detected_at"] = datetime.datetime.now().isoformat()
        info["reason"] = "groundstation restarted during recording"
        try:
            info_path.write_text(json.dumps(info, indent=2, default=str), encoding="utf-8")
        except OSError:
            logger.exception("could not annotate info.json for %s", p.id)

    iq = Path(p.pass_dir) / "recording.bin"
    if iq.exists() and iq.stat().st_size > 0 and not p.satellite.skip_iq_upload:
        dst = os.path.join(
            "",  # filled by caller using cfg.nas_directory
        )
        return TransferRequest(
            id=str(uuid.uuid4()),
            source_path=str(iq),
            destination_path="",  # placeholder, resolved below
            keep_source=False,
            compress=True,
            pass_id=p.id,
            label=f"{p.satellite.name} IQ (partial)",
        )
    return None


def _boot_recovery(
    cfg: GroundstationConfig, state: StateStore
) -> tuple[Dict[str, Pass], List[TransferRequest]]:
    """Reload persisted passes and figure out what to do with each.
    Returns the in-memory pass cache + any extra transfers to enqueue."""
    passes_by_id: Dict[str, Pass] = {}
    extra_transfers: List[TransferRequest] = []
    for p in state.load_passes():
        if p.status == PassStatus.RECORDING:
            logger.warning("pass %s was RECORDING at shutdown — recovering as partial", p.id)
            partial = _recover_interrupted_pass(p, state)
            if partial is not None:
                partial.destination_path = _nas_path(cfg, p, "recording.bin.zst")
                state.transfer_put(partial)
                extra_transfers.append(partial)
        passes_by_id[p.id] = p
    return passes_by_id, extra_transfers


def _nas_path(cfg: GroundstationConfig, p: Pass, *parts: str) -> str:
    prefix = os.path.join(
        cfg.nas_directory,
        p.pass_info.start_time.strftime("%Y"),
        p.pass_info.start_time.strftime("%Y-%m-%d"),
        f"{p.satellite.name}_{p.pass_info.start_time.strftime('%Y-%m-%d_%H-%M-%S')}",
    )
    return os.path.join(prefix, *parts)


async def _run(cfg: GroundstationConfig, no_tui: bool) -> None:
    loop = asyncio.get_running_loop()
    bus = EventBus()
    _attach_bus_logging(bus, loop)

    state = StateStore("state")
    view = ViewModel()

    passes_by_id, extra_transfers = _boot_recovery(cfg, state)

    # Subscribe the view keeper BEFORE constructing anything that publishes
    # initial events (the gate emits its starting state at construction).
    view_sub = bus.subscribe(
        *event_types(), name="view", queue_size=4096, overflow="drop_oldest"
    )
    view_task = asyncio.create_task(
        run_subscriber(view_sub, view.apply, "view"), name="view_updater"
    )

    predictor = PassPredictor(n2yo_api_key=cfg.n2yo_api_key)

    def _on_gate_change(open_: bool, reason: str) -> None:
        loop.create_task(
            bus.publish(E.DecodeGateStateChanged(open=open_, reason=reason))
        )

    gate = DecodeGate(
        safety_minutes=cfg.decode_safety_minutes,
        on_change=_on_gate_change,
    )

    scheduler = SchedulerService(cfg, bus, state, predictor, gate, passes_by_id)
    recorder = RecorderService(cfg, bus, state)
    decoder = DecoderService(cfg, bus, state, gate, passes_by_id)
    transfer = TransferService(cfg, bus, state)
    web_service = WebService(cfg, bus, view)

    # Push recovered passes into view so UIs have something to show immediately
    if passes_by_id:
        await bus.publish(E.PassesTableChanged(passes=list(passes_by_id.values())))

    services: List = [scheduler, recorder, decoder, transfer, web_service]
    if not no_tui:
        services.append(TUIService(bus, view))

    tasks = [asyncio.create_task(s.run(), name=type(s).__name__) for s in services]

    # Extra transfers discovered during boot go on the bus now that subscribers are live
    await asyncio.sleep(0)
    for req in extra_transfers:
        await bus.publish(E.TransferQueued(request=req))

    stop_event = asyncio.Event()

    def _request_stop(*_: object) -> None:
        logger.info("shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _request_stop)
        loop.add_signal_handler(signal.SIGTERM, _request_stop)
    except NotImplementedError:
        # Windows: add_signal_handler not supported. KeyboardInterrupt still works.
        pass

    try:
        done_waiter = asyncio.create_task(stop_event.wait())
        await asyncio.wait(
            [done_waiter, *tasks], return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        logger.info("shutting down services")
        for s in services:
            s.stop()
        gate.close()
        for t in tasks:
            try:
                await asyncio.wait_for(t, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        view_sub.close()
        view_task.cancel()
        try:
            await view_task
        except (asyncio.CancelledError, Exception):
            pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="config.yml")
    parser.add_argument("--no-tui", action="store_true", help="run without the Rich TUI")
    args = parser.parse_args()

    load_dotenv()
    _setup_logging()

    try:
        cfg = load_config(args.config)
    except SystemExit:
        raise
    except Exception:
        logger.exception("config load failed")
        sys.exit(2)

    try:
        asyncio.run(_run(cfg, no_tui=args.no_tui))
    except KeyboardInterrupt:
        logger.info("interrupted")


if __name__ == "__main__":
    main()
