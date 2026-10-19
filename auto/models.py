from __future__ import annotations

import datetime
from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


IQUploadMode = Literal["never", "on_decode", "always"]


IQ_DATA_FILE_EXTENSION = ".bin"


DecoderRole = Literal["waterfall", "decoder"]


@dataclass(frozen=True)
class RolePolicy:
    """How the pipeline treats a decoder of a given role."""

    # Output counts as "decoded" (pass outcome, iq_upload=on_decode).
    produces_data: bool
    # Counts toward the decode backlog that makes low-priority passes skip.
    counts_toward_backlog: bool
    # Writes into the pass directory itself instead of a <name>/ subdir
    # (keeps waterfall.png at the top of the pass folder on the NAS).
    output_in_pass_dir: bool
    # Queued ahead of the other decoders of the same pass.
    runs_first: bool


ROLE_POLICIES: Dict[str, RolePolicy] = {
    "waterfall": RolePolicy(
        produces_data=False, counts_toward_backlog=False, output_in_pass_dir=True, runs_first=True
    ),
    "decoder": RolePolicy(
        produces_data=True, counts_toward_backlog=True, output_in_pass_dir=False, runs_first=False
    ),
}


class Decoder(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    role: DecoderRole = "decoder"
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

    @model_validator(mode="before")
    @classmethod
    def _migrate_nameless_waterfall(cls, v: Any) -> Any:
        # Passes persisted before decoder roles existed marked the waterfall
        # by leaving `name` out.
        if isinstance(v, dict) and v.get("name") is None and "role" not in v:
            v = dict(v)
            v["name"] = "waterfall"
            v["role"] = "waterfall"
        return v

    @property
    def policy(self) -> RolePolicy:
        return ROLE_POLICIES[self.role]


class Satellite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    norad: str
    # Optional SatNOGS DB satellite id. If set, orbital elements come from the
    # SatNOGS DB TLE (e.g. Space-Track analyst objects not on CelesTrak, or
    # fresh launches whose CelesTrak object labels may still swap); `norad` is
    # then only the CelesTrak fallback.
    satnogs_id: str | None = None
    frequency: float
    bandwidth: float
    sample_rate: float
    lo_offset: float = 0.0
    gain: int | None = None
    priority: int = 0
    iq_upload: IQUploadMode = "on_decode"
    doppler_correction: bool = True
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
    # CCSDS OMM field dict as served by CelesTrak (FORMAT=json).
    omm: dict[str, Any]
    recording_start_override: Optional[datetime.datetime] = None
    recording_end_override: Optional[datetime.datetime] = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_tle(cls, v: Any) -> Any:
        # Passes persisted before the TLE→OMM switch carry tle1/tle2.
        if isinstance(v, dict) and "omm" not in v and "tle1" in v and "tle2" in v:
            from .orbit import tle_to_omm

            v = dict(v)
            v["omm"] = tle_to_omm(v.pop("tle1"), v.pop("tle2"))
        return v


class PassStatus(str, Enum):
    PREDICTED = "predicted"
    RECORDING = "recording"
    RECORDED = "recorded"
    RECORDED_PARTIAL = "recorded_partial"
    DECODING = "decoding"
    DECODED = "decoded"
    UPLOADING = "uploading"
    # Terminal outcomes; Pass.status_reason says why.
    DONE = "done"  # at least one data decoder produced output
    FINISHED_WITHOUT_DECODES = "finished_without_decodes"  # recorded, nothing decoded
    SKIPPED = "skipped"  # deliberately not recorded
    FAILED = "failed"  # an error stopped the pass


TERMINAL_STATUSES = frozenset(
    {
        PassStatus.DONE,
        PassStatus.FINISHED_WITHOUT_DECODES,
        PassStatus.SKIPPED,
        PassStatus.FAILED,
        PassStatus.RECORDED_PARTIAL,
    }
)


class Pass(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    satellite: Satellite
    pass_info: PassInfo
    pass_dir: str
    status: PassStatus = PassStatus.PREDICTED
    # Human-readable why for the current status (skip rule, error, ...).
    status_reason: str | None = None
    recording_path: str | None = None
    decoders_pending: List[int] = Field(default_factory=list)
    decoders_done: List[int] = Field(default_factory=list)
    decoders_failed: List[int] = Field(default_factory=list)
    # Non-decoder consumers of the IQ recording (e.g. the skymap analyzer).
    # `_after_all_decoders` waits for both decoders and consumers before
    # compressing/deleting recording.bin.
    iq_consumers_pending: List[str] = Field(default_factory=list)
    iq_consumers_done: List[str] = Field(default_factory=list)
    iq_upload_confirmed: bool = False
    interrupted: bool = False
    created_at: datetime.datetime = Field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = Field(default_factory=datetime.datetime.now)

    def set_status(self, status: PassStatus, reason: str | None = None) -> None:
        """Only change status through here (or auto.lifecycle.set_pass_status,
        which also persists and announces it)."""
        self.status = status
        self.status_reason = reason
        self.updated_at = datetime.datetime.now()

    def data_decoders_done(self) -> bool:
        return any(
            self.satellite.decoder[i].policy.produces_data
            for i in self.decoders_done
            if i < len(self.satellite.decoder)
        )

    @staticmethod
    def make_id(sat: Satellite, pass_info: PassInfo) -> str:
        ts = pass_info.start_time.strftime("%Y-%m-%d_%H-%M-%S")
        safe_name = sat.name.replace(" ", "_").replace("/", "_")
        return f"{safe_name}_{ts}"


def should_upload_iq(p: Pass) -> bool:
    """Decide whether to upload the raw IQ recording for a pass.

    `on_decode` (the default) requires that at least one data-producing
    decoder (see RolePolicy) produced output that survived its filters.
    """
    mode = p.satellite.iq_upload
    if mode == "never" or not p.satellite.decoder:
        return False
    if mode == "always":
        return True
    return p.data_decoders_done()


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
    decode_safety_minutes: float = 1.0
    web_host: str = "0.0.0.0"
    web_port: int = 80
    n2yo_api_key: str | None = None
    skymap_retention_days: float = 14.0
    # Passes of satellites with priority >= this are always recorded. Lower
    # priority passes are skipped while decoders are still waiting, so the
    # decode backlog can drain. None: record every pass.
    always_record_priority: int | None = None
