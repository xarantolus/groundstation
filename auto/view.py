from __future__ import annotations

import collections
import datetime
import logging
import os
import threading
from typing import Deque, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict, Field

from . import events as E
from .models import Pass, PassStatus

logger = logging.getLogger("groundstation.view")

LOG_TAIL_CAPACITY = 500
DECODER_LOG_TAIL_CAPACITY = 200
COMPLETED_TRANSFERS_CAPACITY = 50


class TransferView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    source_path: str
    destination_path: str
    label: str | None = None
    copied: int = 0
    total: int = 0
    progress: float = 0.0  # 0..100


class CompletedTransferView(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    source_path: str
    label: str | None = None
    status: str  # "ok" | "fail"
    ts: datetime.datetime


class LogLine(BaseModel):
    model_config = ConfigDict(extra="forbid")

    level: str
    logger: str
    message: str
    ts: datetime.datetime


class DecoderLogLine(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pass_id: str
    decoder_index: int
    line: str
    ts: datetime.datetime


class DecodingNow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pass_id: str
    decoder_index: int
    decoder_name: str | None = None


class GateState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    open: bool = True
    reason: str = ""


class ViewSnapshot(BaseModel):
    """Everything a fresh UI client needs to render the current state."""

    model_config = ConfigDict(extra="forbid")

    passes: List[Pass] = Field(default_factory=list)
    recording: str | None = None
    decoding: DecodingNow | None = None
    pending_decoders: int = 0
    active_transfers: List[TransferView] = Field(default_factory=list)
    completed_transfers: List[CompletedTransferView] = Field(default_factory=list)
    main_log: List[LogLine] = Field(default_factory=list)
    decoder_log: List[DecoderLogLine] = Field(default_factory=list)
    gate: GateState = Field(default_factory=GateState)


class ViewModel:
    """Event-driven view state. Shared by the TUI and the Web UI so both
    render from the same source of truth.

    The Rich Live renderer reads this from a background thread while the
    asyncio view subscriber mutates it. A threading lock serialises both.
    """

    def __init__(self) -> None:
        self.passes: List[Pass] = []
        self.recording_pass_id: Optional[str] = None
        self.decoding: Optional[DecodingNow] = None
        self.decoders_pending: Set[Tuple[str, int]] = set()
        self.active_transfers: Dict[str, TransferView] = {}
        self.completed_transfers: Deque[CompletedTransferView] = collections.deque(
            maxlen=COMPLETED_TRANSFERS_CAPACITY
        )
        self.main_log: Deque[LogLine] = collections.deque(maxlen=LOG_TAIL_CAPACITY)
        self.decoder_log: Deque[DecoderLogLine] = collections.deque(
            maxlen=DECODER_LOG_TAIL_CAPACITY
        )
        self.gate = GateState()
        self.lock = threading.RLock()

    def apply(self, event: E.Event) -> None:
        with self.lock:
            self._apply_locked(event)

    def _apply_locked(self, event: E.Event) -> None:
        if isinstance(event, E.PassesTableChanged):
            self.passes = [p.model_copy() for p in event.passes]
        elif isinstance(event, (E.PassPredicted, E.PassUpcoming, E.PassStarted, E.PassEnded)):
            self._upsert_pass(event.pass_)
        elif isinstance(event, E.RecordingStarted):
            self.recording_pass_id = event.pass_id
            self._update_pass_status(event.pass_id, PassStatus.RECORDING)
        elif isinstance(event, E.RecordingCompleted):
            if self.recording_pass_id == event.pass_id:
                self.recording_pass_id = None
            self._update_pass_status(event.pass_id, PassStatus.RECORDED)
        elif isinstance(event, E.RecordingFailed):
            if self.recording_pass_id == event.pass_id:
                self.recording_pass_id = None
            self._update_pass_status(event.pass_id, PassStatus.FAILED)
        elif isinstance(event, E.DecodeQueued):
            self.decoders_pending.add((event.pass_id, event.decoder_index))
        elif isinstance(event, E.DecodeStarted):
            self.decoders_pending.discard((event.pass_id, event.decoder_index))
            self.decoding = DecodingNow(
                pass_id=event.pass_id,
                decoder_index=event.decoder_index,
                decoder_name=event.decoder_name,
            )
            self._update_pass_status(event.pass_id, PassStatus.DECODING)
        elif isinstance(event, (E.DecodeCompleted, E.DecodeFailed)):
            self.decoders_pending.discard((event.pass_id, event.decoder_index))
            if self.decoding and self.decoding.pass_id == event.pass_id and self.decoding.decoder_index == event.decoder_index:
                self.decoding = None
        elif isinstance(event, E.DecodeLog):
            self.decoder_log.append(
                DecoderLogLine(
                    pass_id=event.pass_id,
                    decoder_index=event.decoder_index,
                    line=event.line,
                    ts=event.ts,
                )
            )
        elif isinstance(event, E.DecodeGateStateChanged):
            self.gate = GateState(open=event.open, reason=event.reason)
        elif isinstance(event, E.TransferStarted):
            # Stat once at start so the TUI/Web summary can show total queued
            # bytes without statting every refresh. Progress events will
            # overwrite `total` with the actual byte count once copying begins
            # (which may differ for items that compress before upload).
            total = 0
            try:
                if event.source_path:
                    total = os.path.getsize(event.source_path)
            except OSError:
                pass
            self.active_transfers[event.request_id] = TransferView(
                request_id=event.request_id,
                source_path=event.source_path,
                destination_path=event.destination_path,
                label=event.label,
                total=total,
            )
        elif isinstance(event, E.TransferProgress):
            existing = self.active_transfers.get(event.request_id)
            progress = (event.copied / event.total * 100) if event.total > 0 else 0.0
            if existing:
                existing.copied = event.copied
                existing.total = event.total
                existing.progress = progress
            else:
                self.active_transfers[event.request_id] = TransferView(
                    request_id=event.request_id,
                    source_path=event.source_path,
                    destination_path="",
                    copied=event.copied,
                    total=event.total,
                    progress=progress,
                )
        elif isinstance(event, E.TransferCompleted):
            self.active_transfers.pop(event.request_id, None)
            self.completed_transfers.append(
                CompletedTransferView(
                    request_id=event.request_id,
                    source_path=event.source_path,
                    label=event.label,
                    status="ok",
                    ts=event.ts,
                )
            )
        elif isinstance(event, E.TransferFailed):
            if not event.will_retry:
                self.active_transfers.pop(event.request_id, None)
                self.completed_transfers.append(
                    CompletedTransferView(
                        request_id=event.request_id,
                        source_path=event.source_path,
                        label=None,
                        status="fail",
                        ts=event.ts,
                    )
                )
        elif isinstance(event, E.LogMessage):
            self.main_log.append(
                LogLine(level=event.level, logger=event.logger, message=event.message, ts=event.ts)
            )

    def snapshot(self) -> ViewSnapshot:
        # Deep-copy each Pass so the renderer never races with the event-loop
        # writer. TransferView / CompletedTransferView / LogLine / DecoderLogLine
        # are append-once, not mutated in place — a reference copy is enough.
        with self.lock:
            return ViewSnapshot(
                passes=[p.model_copy(deep=True) for p in self.passes],
                recording=self.recording_pass_id,
                decoding=self.decoding.model_copy() if self.decoding else None,
                pending_decoders=len(self.decoders_pending),
                active_transfers=[t.model_copy() for t in self.active_transfers.values()],
                completed_transfers=list(self.completed_transfers),
                main_log=list(self.main_log),
                decoder_log=list(self.decoder_log),
                gate=self.gate.model_copy(),
            )

    def _upsert_pass(self, p: Pass) -> None:
        for i, existing in enumerate(self.passes):
            if existing.id == p.id:
                self.passes[i] = p.model_copy()
                return
        self.passes.append(p.model_copy())
        self.passes.sort(key=lambda x: x.pass_info.start_time)

    def _update_pass_status(self, pass_id: str, status: PassStatus) -> None:
        for p in self.passes:
            if p.id == pass_id:
                p.status = status
                return
