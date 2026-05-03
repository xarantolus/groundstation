from __future__ import annotations

import datetime
from typing import List, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from .models import Pass, TransferRequest


class _EventBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ts: datetime.datetime = Field(default_factory=datetime.datetime.now)


class PassPredicted(_EventBase):
    type: Literal["pass_predicted"] = "pass_predicted"
    pass_: Pass = Field(alias="pass")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class PassesTableChanged(_EventBase):
    type: Literal["passes_table_changed"] = "passes_table_changed"
    passes: List[Pass]


class PassUpcoming(_EventBase):
    type: Literal["pass_upcoming"] = "pass_upcoming"
    pass_: Pass = Field(alias="pass")
    seconds_until_start: float

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class PassStarted(_EventBase):
    type: Literal["pass_started"] = "pass_started"
    pass_: Pass = Field(alias="pass")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class PassEnded(_EventBase):
    type: Literal["pass_ended"] = "pass_ended"
    pass_: Pass = Field(alias="pass")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class RecordingStarted(_EventBase):
    type: Literal["recording_started"] = "recording_started"
    pass_id: str


class RecordingLog(_EventBase):
    type: Literal["recording_log"] = "recording_log"
    pass_id: str
    line: str


class RecordingCompleted(_EventBase):
    type: Literal["recording_completed"] = "recording_completed"
    pass_id: str
    path: str


class RecordingFailed(_EventBase):
    type: Literal["recording_failed"] = "recording_failed"
    pass_id: str
    error: str


class DecodeQueued(_EventBase):
    type: Literal["decode_queued"] = "decode_queued"
    pass_id: str
    decoder_index: int


class DecodeStarted(_EventBase):
    type: Literal["decode_started"] = "decode_started"
    pass_id: str
    decoder_index: int
    decoder_name: str | None = None


class DecodeLog(_EventBase):
    type: Literal["decode_log"] = "decode_log"
    pass_id: str
    decoder_index: int
    line: str


class DecodeCompleted(_EventBase):
    type: Literal["decode_completed"] = "decode_completed"
    pass_id: str
    decoder_index: int
    outputs: List[str] = Field(default_factory=list)


class DecodeFailed(_EventBase):
    type: Literal["decode_failed"] = "decode_failed"
    pass_id: str
    decoder_index: int
    error: str


class DecodeGateStateChanged(_EventBase):
    type: Literal["decode_gate_state_changed"] = "decode_gate_state_changed"
    open: bool
    reason: str


class TransferQueued(_EventBase):
    type: Literal["transfer_queued"] = "transfer_queued"
    request: TransferRequest


class TransferStarted(_EventBase):
    type: Literal["transfer_started"] = "transfer_started"
    request_id: str
    source_path: str
    destination_path: str
    label: str | None = None


class TransferProgress(_EventBase):
    type: Literal["transfer_progress"] = "transfer_progress"
    request_id: str
    source_path: str
    copied: int
    total: int


class TransferCompleted(_EventBase):
    type: Literal["transfer_completed"] = "transfer_completed"
    request_id: str
    source_path: str
    destination_path: str
    label: str | None = None


class TransferFailed(_EventBase):
    type: Literal["transfer_failed"] = "transfer_failed"
    request_id: str
    source_path: str
    error: str
    will_retry: bool


class LogMessage(_EventBase):
    type: Literal["log_message"] = "log_message"
    level: str
    logger: str
    message: str


class ShutdownRequested(_EventBase):
    type: Literal["shutdown_requested"] = "shutdown_requested"


class IqConsumerSettled(_EventBase):
    type: Literal["iq_consumer_settled"] = "iq_consumer_settled"
    pass_id: str
    consumer_name: str


class SkymapUpdated(_EventBase):
    type: Literal["skymap_updated"] = "skymap_updated"
    pass_id: str


Event = Union[
    PassPredicted,
    PassesTableChanged,
    PassUpcoming,
    PassStarted,
    PassEnded,
    RecordingStarted,
    RecordingLog,
    RecordingCompleted,
    RecordingFailed,
    DecodeQueued,
    DecodeStarted,
    DecodeLog,
    DecodeCompleted,
    DecodeFailed,
    DecodeGateStateChanged,
    TransferQueued,
    TransferStarted,
    TransferProgress,
    TransferCompleted,
    TransferFailed,
    LogMessage,
    ShutdownRequested,
    IqConsumerSettled,
    SkymapUpdated,
]
