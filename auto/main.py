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

from typing import Optional


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
            # Atomic write — a crash mid-annotation on a previously valid
            # info.json must not leave a truncated file on disk.
            tmp = info_path.with_suffix(info_path.suffix + ".tmp")
            tmp.write_text(json.dumps(info, indent=2, default=str), encoding="utf-8")
            os.replace(tmp, info_path)
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
    Returns the in-memory pass cache + any extra transfers to enqueue.

    Handles several crash-between-writes scenarios:
      * RECORDING → mark partial, queue partial IQ upload
      * RECORDED / DECODING with empty decode queue → re-enqueue decoders
        (covers a crash between RecordingCompleted save and decoder
        pending_decodes persistence)
      * RECORDED / DECODING with every decoder already settled → advance
        to DECODED (covers a crash between the last decoder finishing and
        _after_all_decoders launching)
      * DECODED pass with recording.bin.zst or recording.bin still on disk
        → queue IQ upload (covers a crash between compression and
        transfer_put in DecoderService._after_all_decoders)
    """
    passes_by_id: Dict[str, Pass] = {}
    extra_transfers: List[TransferRequest] = []

    persisted_decode_keys = set(state.load_decode_queue())
    persisted_transfer_sources = {
        req.source_path for req in state.load_transfer_queue()
    }

    recovered_actions: Dict[str, int] = {}

    def _bump(action: str) -> None:
        recovered_actions[action] = recovered_actions.get(action, 0) + 1

    for p in state.load_passes():
        if p.status == PassStatus.RECORDING:
            logger.warning("pass %s was RECORDING at shutdown — recovering as partial", p.id)
            partial = _recover_interrupted_pass(p, state)
            _bump("recording→partial")
            if partial is not None:
                partial.destination_path = _nas_path(cfg, p, "recording.bin.zst")
                state.transfer_put(partial)
                extra_transfers.append(partial)
                _bump("partial_iq_queued")

        elif p.status in (PassStatus.RECORDED, PassStatus.DECODING):
            missing = _missing_decoders(p, persisted_decode_keys)
            if missing:
                logger.warning(
                    "pass %s in %s but %d decoder(s) missing from queue — re-enqueueing",
                    p.id,
                    p.status.value,
                    len(missing),
                )
                for idx in missing:
                    state.decode_put(p.id, idx)
                    if idx not in p.decoders_pending:
                        p.decoders_pending.append(idx)
                state.save_pass(p)
                _bump("decoder_re_enqueued")
            elif p.satellite.decoder:
                # No decoders are queued AND none are missing → every decoder
                # has already settled. We crashed after the last decoder
                # completed but before _after_all_decoders ran (or before it
                # advanced status to DECODED/DONE). Fall through to the
                # DECODED handling below so the IQ upload gets reconstructed.
                logger.warning(
                    "pass %s was %s but all %d decoder(s) settled — advancing to DECODED",
                    p.id,
                    p.status.value,
                    len(p.satellite.decoder),
                )
                p.status = PassStatus.DECODED
                state.save_pass(p)
                _bump("advanced_to_decoded")

        if p.status == PassStatus.DECODED:
            # Post-decode IQ upload may not have been queued if we died
            # between compression and transfer_put. Reconstruct the upload
            # here using whatever survived on disk. Also runs for passes
            # just advanced from DECODING above.
            if not p.satellite.skip_iq_upload and p.satellite.decoder:
                iq_zst = os.path.join(p.pass_dir, "recording.bin.zst")
                iq_bin = os.path.join(p.pass_dir, "recording.bin")
                chosen: Optional[str] = None
                compress = False
                if os.path.isfile(iq_zst) and iq_zst not in persisted_transfer_sources:
                    chosen = iq_zst
                    compress = False
                elif os.path.isfile(iq_bin) and iq_bin not in persisted_transfer_sources:
                    chosen = iq_bin
                    compress = True
                if chosen is not None:
                    logger.warning(
                        "pass %s was DECODED but IQ upload missing — queueing %s",
                        p.id,
                        chosen,
                    )
                    req = TransferRequest(
                        id=str(uuid.uuid4()),
                        source_path=chosen,
                        destination_path=_nas_path(cfg, p, "recording.bin.zst"),
                        keep_source=False,
                        compress=compress,
                        pass_id=p.id,
                        label=f"{p.satellite.name} IQ (recovered)",
                    )
                    state.transfer_put(req)
                    extra_transfers.append(req)
                    _bump("decoded_iq_queued")

        passes_by_id[p.id] = p

    if recovered_actions:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(recovered_actions.items()))
        logger.info("boot recovery: %s", summary)
    return passes_by_id, extra_transfers


def _missing_decoders(p: Pass, queued: set) -> List[int]:
    """Return the decoder indices that should be queued but aren't."""
    total = len(p.satellite.decoder)
    done = set(p.decoders_done) | set(p.decoders_failed)
    missing: List[int] = []
    for idx in range(total):
        if idx in done:
            continue
        if (p.id, idx) in queued:
            continue
        missing.append(idx)
    return missing


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

    # Recovered transfers from _boot_recovery were already persisted to state
    # in that function. TransferService.run() loads state at startup, so
    # republishing the event here would double-enqueue and race two workers
    # on the same file. Nothing to do.
    _ = extra_transfers

    stop_event = asyncio.Event()
    signal_count = [0]

    def _request_stop(*_: object) -> None:
        signal_count[0] += 1
        if signal_count[0] == 1:
            logger.info("shutdown requested — finishing in-flight work (Ctrl+C again to force)")
            stop_event.set()
        else:
            # Second signal: don't run finalisers. We've already asked nicely;
            # something is stuck. Kill the process directly.
            logger.warning("second signal — forcing exit")
            os._exit(130)

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
            try:
                s.stop()
            except Exception:
                logger.exception("service stop() failed")
        gate.close()

        # Wait for all tasks in parallel with a single deadline, then cancel
        # anything still running. Sequential waits would multiply the shutdown
        # time by the number of services.
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=10
            )
        except asyncio.TimeoutError:
            logger.warning("services didn't stop in 10s — cancelling")
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

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
