from __future__ import annotations

import itertools
import os
import sys
from typing import Any, Dict, List

import yaml
from pydantic import ValidationError

from .models import Decoder, GroundstationConfig, Satellite


def _expand_decoder_matrix(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    env = raw.get("env") or {}
    matrix_keys: List[str] = []
    matrix_values: List[list] = []
    scalar_env: Dict[str, Any] = {}
    for k, v in env.items():
        if isinstance(v, list):
            matrix_keys.append(k)
            matrix_values.append(v)
        else:
            scalar_env[k] = v

    if not matrix_keys:
        return [raw]

    expanded: List[Dict[str, Any]] = []
    base_name = raw.get("name")
    for combo in itertools.product(*matrix_values):
        new_env = dict(scalar_env)
        suffix_parts: List[str] = []
        for key, val in zip(matrix_keys, combo):
            new_env[key] = val
            suffix_parts.append(str(val))
        suffix = "_".join(suffix_parts)
        new_raw = dict(raw)
        new_raw["env"] = new_env
        new_raw["name"] = f"{base_name}_{suffix}" if base_name else suffix
        expanded.append(new_raw)
    return expanded


def _normalize_decoder_field(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        raw_list = [raw]
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raise ValueError("decoder must be a dict, list, or null")
    return [e for d in raw_list for e in _expand_decoder_matrix(d)]


def _env_float(name: str) -> float:
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    try:
        return float(value)
    except ValueError as e:
        raise ValueError(f"Environment variable {name} must be a number (got {value!r})") from e


def load_config(path: str) -> GroundstationConfig:
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw: Dict[str, Any] = yaml.safe_load(f) or {}
    except OSError as e:
        raise SystemExit(f"cannot open config {path}: {e}")

    if not isinstance(raw.get("satellites"), list):
        raise SystemExit(f"{path}: 'satellites' must be a list")
    if not isinstance(raw.get("nas_directory"), str):
        raise SystemExit(f"{path}: 'nas_directory' must be a string")

    satellites: List[Satellite] = []
    for idx, sat_raw in enumerate(raw["satellites"]):
        if not isinstance(sat_raw, dict):
            raise SystemExit(f"{path}: satellites[{idx}] must be a mapping")
        sat_input = dict(sat_raw)
        if "decoder" in sat_input:
            sat_input["decoder"] = _normalize_decoder_field(sat_input["decoder"])
        try:
            sat = Satellite.model_validate(sat_input)
        except ValidationError as e:
            sys.stderr.write(
                f"{path}: invalid satellite at index {idx} ({sat_raw.get('name', '?')}):\n{e}\n"
            )
            raise SystemExit(2) from None

        if sat.iq_upload != "never" and not sat.decoder:
            raise SystemExit(
                f"{path}: satellites[{idx}] ({sat.name}): "
                f"iq_upload={sat.iq_upload!r} requires at least one decoder"
            )
        if sat.iq_upload == "on_decode" and not any(
            d.name is not None for d in sat.decoder
        ):
            raise SystemExit(
                f"{path}: satellites[{idx}] ({sat.name}): "
                "iq_upload='on_decode' but no non-waterfall decoder is configured "
                "— IQ would never be uploaded; set iq_upload='always' or "
                "add a real decoder"
            )
        satellites.append(sat)

    try:
        cfg = GroundstationConfig(
            satellites=satellites,
            nas_directory=raw["nas_directory"],
            location_lat=_env_float("LOCATION_LAT"),
            location_lon=_env_float("LOCATION_LON"),
            location_alt=_env_float("LOCATION_ALT"),
            pass_elevation_threshold_deg=float(raw.get("pass_elevation_threshold_deg", 5.0)),
            pass_start_elevation_threshold_deg=float(
                raw.get("pass_start_elevation_threshold_deg", 5.0)
            ),
            update_interval_hours=float(raw.get("update_interval_hours", 2.0)),
            decode_safety_minutes=float(raw.get("decode_safety_minutes", 1.0)),
            web_host=str(raw.get("web_host", "0.0.0.0")),
            web_port=int(raw.get("web_port", 80)),
            n2yo_api_key=os.getenv("N2YO_API_KEY"),
        )
    except (ValidationError, ValueError) as e:
        sys.stderr.write(f"{path}: {e}\n")
        raise SystemExit(2) from None

    os.makedirs(cfg.nas_directory, exist_ok=True)
    return cfg


def decoder_display_name(d: Decoder, index: int) -> str:
    return d.name or f"decoder_{index}"
