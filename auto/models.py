from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, List

from pydantic import BaseModel, ConfigDict, Field, field_validator


IQ_DATA_FILE_EXTENSION = ".bin"


class Decoder(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    container: str
    podman_args: str | None = None
    args: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    min_files: int = 1
    min_size_bytes: int = 0
    timeout_minutes: float | None = 30.0

    @field_validator("env", mode="before")
    @classmethod
    def _coerce_env(cls, v: Any) -> Any:
        if v is None:
            return {}
        if isinstance(v, dict):
            return {str(k): str(val) for k, val in v.items()}
        return v


class Satellite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    norad: str
    frequency: float
    bandwidth: float
    sample_rate: float
    lo_offset: float = 0.0
    gain: int | None = None
    priority: int = 0
    skip_iq_upload: bool = False
    decoder: List[Decoder] = Field(default_factory=list)

    @field_validator("norad", mode="before")
    @classmethod
    def _norad_as_str(cls, v: Any) -> str:
        return str(v)

    @field_validator("decoder", mode="before")
    @classmethod
    def _normalize_decoder(cls, v: Any) -> Any:
        if v is None:
            return []
        if isinstance(v, dict):
            return [v]
        return v


class PassInfo(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_time: datetime.datetime
    max_time: datetime.datetime
    end_time: datetime.datetime
    start_elevation: float
    max_elevation: float
    end_elevation: float
    start_azimuth: float
    max_azimuth: float
    end_azimuth: float
    duration_minutes: float
    tle1: str
    tle2: str


class PassStatus(str, Enum):
    PREDICTED = "predicted"
    RECORDING = "recording"
    RECORDED = "recorded"
    RECORDED_PARTIAL = "recorded_partial"
    DECODING = "decoding"
    DECODED = "decoded"
    UPLOADING = "uploading"
    DONE = "done"
    FAILED = "failed"


class Pass(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    satellite: Satellite
    pass_info: PassInfo
    pass_dir: str
    status: PassStatus = PassStatus.PREDICTED
    recording_path: str | None = None
    decoders_pending: List[int] = Field(default_factory=list)
    decoders_done: List[int] = Field(default_factory=list)
    decoders_failed: List[int] = Field(default_factory=list)
    iq_upload_confirmed: bool = False
    interrupted: bool = False
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = Field(default_factory=datetime.datetime.now)

    @staticmethod
    def make_id(sat: Satellite, pass_info: PassInfo) -> str:
        ts = pass_info.start_time.strftime("%Y-%m-%d_%H-%M-%S")
        safe_name = sat.name.replace(" ", "_").replace("/", "_")
        return f"{safe_name}_{ts}"


class TransferRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    source_path: str
    destination_path: str
    keep_source: bool = False
    compress: bool = True
    attempt: int = 0
    disk_retry_count: int = 0
    pass_id: str | None = None
    label: str | None = None
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)


class GroundstationConfig(BaseModel):
    """Fully validated runtime config. Produced by auto.config.load_config."""

    model_config = ConfigDict(extra="forbid")

    satellites: List[Satellite]
    nas_directory: str
    location_lat: float
    location_lon: float
    location_alt: float
    pass_elevation_threshold_deg: float = 5.0
    pass_start_elevation_threshold_deg: float = 5.0
    update_interval_hours: float = 2.0
    decode_safety_minutes: float = 3.0
    web_host: str = "0.0.0.0"
    web_port: int = 80
    n2yo_api_key: str | None = None
