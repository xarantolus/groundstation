
import datetime
from typing import Dict, List, TypedDict


IQ_DATA_FILE_EXTENSION=".bin"

class Decoder(TypedDict):
    # if name is given and more than one decoder is defined, a subdirectory will be created in the overpass directory
    name: str | None
    container: str
    podman_args: str | None
    args: str | None
    env: Dict[str, str] | None
    # if less than this number of output files are created, nothing will be uploaded
    min_files: int | None

class Satellite(TypedDict):
    name: str
    norad: str
    frequency: float
    bandwidth: float
    sample_rate: float
    priority: int | None
    decoder: Decoder | List[Decoder] | None
    lo_offset: float | None
    skip_iq_upload: bool | None

class PassInfo(TypedDict):
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
