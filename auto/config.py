import itertools
import os
import yaml
from typing import Any, Dict, List


def _expand_decoder_matrix(decoder: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Expands a decoder whose `env` has list-valued entries into multiple
    decoders via cross product. Each resulting decoder gets a name suffix
    encoding the matrix values so output subdirectories don't collide.
    """
    env = decoder.get("env")
    if not env:
        return [decoder]

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
        return [decoder]

    expanded: List[Dict[str, Any]] = []
    base_name = decoder.get("name")
    for combo in itertools.product(*matrix_values):
        new_env = dict(scalar_env)
        suffix_parts: List[str] = []
        for key, val in zip(matrix_keys, combo):
            new_env[key] = val
            suffix_parts.append(str(val))
        suffix = "_".join(suffix_parts)

        new_decoder = dict(decoder)
        new_decoder["env"] = new_env
        new_decoder["name"] = f"{base_name}_{suffix}" if base_name else suffix
        expanded.append(new_decoder)

    return expanded


def load_config(path: str) -> Dict[str, Any]:
    """
    Loads and validates the configuration file.
    """
    with open(path) as f:
        cfg: Dict[str, Any] = yaml.safe_load(f)

    # Validate required keys
    required_keys: Dict[str, type] = {
        "satellites": list,
        "nas_directory": str,
    }
    for key, expected_type in required_keys.items():
        if key not in cfg:
            raise ValueError(f"Missing required key in config: '{key}'")
        if not isinstance(cfg[key], expected_type):
            raise TypeError(f"Key '{key}' must be of type {expected_type.__name__}")

    # Validate "satellites"
    for sat in cfg["satellites"]:
        if not isinstance(sat, dict):
            raise TypeError("Each satellite must be a dictionary")
        required_sat_keys = {"name", "norad", "frequency", "bandwidth", "sample_rate"}
        if not required_sat_keys.issubset(sat.keys()):
            raise ValueError(f"Each satellite must have keys: {required_sat_keys}")
        if not isinstance(sat["name"], str):
            raise TypeError("Satellite 'name' must be a string")
        if not isinstance(sat["norad"], (str, int)):
            raise TypeError("Satellite 'norad' must be a string")
        if not isinstance(sat["frequency"], (int, float, str)):
            raise TypeError("Satellite 'frequency' must be a number")
        if not isinstance(sat["bandwidth"], (int, float, str)):
            raise TypeError("Satellite 'bandwidth' must be a number")
        if not isinstance(sat["sample_rate"], (int, float, str)):
            raise TypeError("Satellite 'sample_rate' must be a number")
        if "priority" in sat and not isinstance(sat["priority"], int):
            raise TypeError("Satellite 'priority' must be an integer")
        if "lo_offset" in sat and not isinstance(
            sat["lo_offset"], (int, float, str, type(None))
        ):
            raise TypeError("Satellite 'lo_offset' must be a number or None")
        if "skip_iq_upload" in sat and not isinstance(sat["skip_iq_upload"], bool):
            raise TypeError("Satellite 'skip_iq_upload' must be a boolean")

        # Validate decoders
        if "decoder" in sat:
            decoder = sat["decoder"]
            if decoder is not None:
                if isinstance(decoder, str):
                    # Legacy format - a string
                    pass
                elif isinstance(decoder, dict):
                    # Single Decoder object
                    if "container" not in decoder:
                        raise ValueError(
                            f"Decoder for satellite '{sat['name']}' is missing required 'container' field"
                        )
                    if not isinstance(decoder["container"], str):
                        raise TypeError(
                            f"Decoder 'container' for satellite '{sat['name']}' must be a string"
                        )

                    # Optional fields validation
                    if (
                        "name" in decoder
                        and decoder["name"] is not None
                        and not isinstance(decoder["name"], str)
                    ):
                        raise TypeError(
                            f"Decoder 'name' for satellite '{sat['name']}' must be a string or None"
                        )
                    if (
                        "args" in decoder
                        and decoder["args"] is not None
                        and not isinstance(decoder["args"], str)
                    ):
                        raise TypeError(
                            f"Decoder 'args' for satellite '{sat['name']}' must be a string or None"
                        )
                    if (
                        "podman_args" in decoder
                        and decoder["podman_args"] is not None
                        and not isinstance(decoder["podman_args"], str)
                    ):
                        raise TypeError(
                            f"Decoder 'podman_args' for satellite '{sat['name']}' must be a string or None"
                        )
                    if (
                        "env" in decoder
                        and decoder["env"] is not None
                        and not isinstance(decoder["env"], dict)
                    ):
                        raise TypeError(
                            f"Decoder 'env' for satellite '{sat['name']}' must be a dictionary or None"
                        )
                    if (
                        "min_files" in decoder
                        and decoder["min_files"] is not None
                        and not isinstance(decoder["min_files"], int)
                    ):
                        raise TypeError(
                            f"Decoder 'min_files' for satellite '{sat['name']}' must be an integer or None"
                        )
                elif isinstance(decoder, list):
                    # List of Decoder objects
                    for i, d in enumerate(decoder):
                        if not isinstance(d, dict):
                            raise TypeError(
                                f"Decoder at index {i} for satellite '{sat['name']}' must be a dictionary"
                            )
                        if "container" not in d:
                            raise ValueError(
                                f"Decoder at index {i} for satellite '{sat['name']}' is missing required 'container' field"
                            )
                        if not isinstance(d["container"], str):
                            raise TypeError(
                                f"Decoder 'container' at index {i} for satellite '{sat['name']}' must be a string"
                            )

                        # Optional fields validation
                        if (
                            "name" in d
                            and d["name"] is not None
                            and not isinstance(d["name"], str)
                        ):
                            raise TypeError(
                                f"Decoder 'name' at index {i} for satellite '{sat['name']}' must be a string or None"
                            )
                        if (
                            "args" in d
                            and d["args"] is not None
                            and not isinstance(d["args"], str)
                        ):
                            raise TypeError(
                                f"Decoder 'args' at index {i} for satellite '{sat['name']}' must be a string or None"
                            )
                        if (
                            "env" in d
                            and d["env"] is not None
                            and not isinstance(d["env"], dict)
                        ):
                            raise TypeError(
                                f"Decoder 'env' at index {i} for satellite '{sat['name']}' must be a dictionary or None"
                            )
                        if (
                            "min_files" in d
                            and d["min_files"] is not None
                            and not isinstance(d["min_files"], int)
                        ):
                            raise TypeError(
                                f"Decoder 'min_files' at index {i} for satellite '{sat['name']}' must be an integer or None"
                            )
                else:
                    raise TypeError(
                        f"Decoder for satellite '{sat['name']}' must be a string, dictionary, list or None"
                    )

                # Expand any matrix (list-valued env) decoders into concrete runs
                if isinstance(decoder, dict):
                    expanded = _expand_decoder_matrix(decoder)
                    sat["decoder"] = expanded[0] if len(expanded) == 1 else expanded
                elif isinstance(decoder, list):
                    sat["decoder"] = [
                        e for d in decoder for e in _expand_decoder_matrix(d)
                    ]

        # If skip_iq_upload is True, then the decoder must be set
        if sat.get("skip_iq_upload", False) and not sat.get("decoder"):
            raise ValueError(
                "If 'skip_iq_upload' is True, 'decoder' must be set - otherwise pass data is just lost"
            )

    location_env_vars = {
        "lat": "LOCATION_LAT",
        "lon": "LOCATION_LON",
        "alt": "LOCATION_ALT",
    }

    cfg["location"] = {}
    for key, env_var in location_env_vars.items():
        env_value = os.getenv(env_var)
        if env_value is None:
            raise ValueError(f"Missing required environment variable: '{env_var}'")
        try:
            cfg["location"][key] = float(env_value)
        except ValueError:
            raise ValueError(
                f"Environment variable '{env_var}' must be a valid number, got: '{env_value}'"
            )

    # Validate optional keys with defaults
    cfg["pass_elevation_threshold_deg"] = cfg.get("pass_elevation_threshold_deg", 5.0)
    if not isinstance(cfg["pass_elevation_threshold_deg"], (int, float)):
        raise TypeError("'pass_elevation_threshold_deg' must be a number")

    cfg["update_interval_hours"] = cfg.get("update_interval_hours", 2)
    if not isinstance(cfg["update_interval_hours"], (int, float)):
        raise TypeError("'update_interval_hours' must be a number")

    return cfg
