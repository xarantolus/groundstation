import json
import os
import queue
import threading
import time
import tempfile
import argparse
import datetime
import requests
from typing import List, Tuple, Dict, Any, Union
from common import Decoder, Satellite, PassInfo, IQ_DATA_FILE_EXTENSION
from config import load_config

import logging
from external import run_recorder, run_decoder
from transfer import TransferQueueManager, file_transfer_worker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def fetch_tle(norad: str, n2yo_api_key: str) -> Tuple[str, str]:
    try:
        r = requests.get(f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad}&FORMAT=TLE", timeout=10)
        r.raise_for_status()
        tle_text = r.text
    except requests.RequestException as e:
        logging.error(f"Failed to fetch TLE for {norad} from Celestrak: {e}")
        # Try fetching from N2YO if Celestrak fails
        if n2yo_api_key:
            r = requests.get(f"https://api.n2yo.com/rest/v1/satellite/tle/{norad}/?apiKey={n2yo_api_key}", timeout=10)
            r.raise_for_status()
            # parse json response, get "tle" field that is a tle string
            data = r.json()
            tle_text = data.get("tle", "")
        else:
            logging.error("N2YO API key not provided, cannot fetch TLE from N2YO")
            raise RuntimeError(f"Failed to fetch TLE for {norad} from Celestrak: {e}")

    lines = [l.strip() for l in tle_text.splitlines() if l.strip()]
    # celestrak returns name + line1 + line2
    if len(lines) >= 3:
        return lines[-2], lines[-1]
    elif len(lines) == 2:
        return lines[0], lines[1]
    else:
        raise RuntimeError(f"Unexpected TLE format for {norad}")

def get_next_passes(sat_name: str, tle1: str, tle2: str, geo: Dict[str, Any], threshold_deg: float, pass_start_threshold: float, hours: int = 24) -> List[PassInfo]:
    """
    Returns a list of passes over the next specified hours where the elevation exceeds threshold_deg.
    Each pass is a dictionary with keys for times, elevations and azimuths at key points.

    threshold_deg: Minimum elevation for a pass to be considered viable
    pass_start_threshold: Elevation at which to consider the pass to have started/ended
    """
    import ephem
    passes = []

    # Set up the observer at the specified location
    observer = ephem.Observer()
    observer.lat = str(geo["lat"])
    observer.lon = str(geo["lon"])
    observer.elev = geo["alt"] / 1000  # Convert meters to kilometers
    observer.horizon = '0'  # Set horizon to actual horizon to find true rise/set

    # Start time for predictions
    start_date = ephem.now() - ephem.minute * 5  # Start 5 minutes in the past
    observer.date = start_date

    # Calculate end time for predictions
    end_date = start_date + ephem.hour * hours

    # Create satellite object
    satellite = ephem.readtle(sat_name, tle1, tle2)

    # Use a sliding window approach to find all passes
    current_date = start_date

    while current_date < end_date:
        try:
            observer.date = current_date
            next_pass = observer.next_pass(satellite)

            # If no pass found or pass beyond our time window
            if next_pass is None or next_pass[0] > end_date:
                break

            rise_time, rise_az, max_time, max_el, set_time, set_az = next_pass

            # Move current_date beyond this pass to find next one
            current_date = set_time + ephem.minute  # Add a minute to avoid finding same pass

            # Skip passes that don't meet our criteria
            if max_el * 180/ephem.pi < threshold_deg:
                continue

            # Find when the satellite crosses the pass_start_threshold (ascending)
            # Create time points between rise_time and max_time
            crossing_time_ascending = None
            time_step = (max_time - rise_time) / 100  # Divide into 100 steps
            for i in range(100):
                check_time = rise_time + time_step * i
                observer.date = check_time
                satellite.compute(observer)
                el_deg = float(satellite.alt) * 180 / ephem.pi
                if el_deg >= pass_start_threshold:
                    crossing_time_ascending = check_time
                    break

            # Find when the satellite crosses the pass_start_threshold (descending)
            crossing_time_descending = None
            time_step = (set_time - max_time) / 100  # Divide into 100 steps
            for i in range(100):
                check_time = max_time + time_step * i
                observer.date = check_time
                satellite.compute(observer)
                el_deg = float(satellite.alt) * 180 / ephem.pi
                if el_deg <= pass_start_threshold:
                    crossing_time_descending = check_time
                    break

            # Use the crossing times if found, otherwise use rise/set times
            start_time = crossing_time_ascending if crossing_time_ascending else rise_time
            end_time = crossing_time_descending if crossing_time_descending else set_time

            # Convert times to local datetime objects
            start_datetime = ephem.localtime(start_time)
            max_datetime = ephem.localtime(max_time)
            end_datetime = ephem.localtime(end_time)

            # Calculate detailed information about the pass
            # Start point
            observer.date = start_time
            satellite.compute(observer)
            start_elevation_deg = float(satellite.alt) * 180 / ephem.pi
            start_azimuth_deg = float(satellite.az) * 180 / ephem.pi

            # Max elevation point
            observer.date = max_time
            satellite.compute(observer)
            max_elevation_deg = float(satellite.alt) * 180 / ephem.pi
            max_azimuth_deg = float(satellite.az) * 180 / ephem.pi

            # End point
            observer.date = end_time
            satellite.compute(observer)
            end_elevation_deg = float(satellite.alt) * 180 / ephem.pi
            end_azimuth_deg = float(satellite.az) * 180 / ephem.pi

            # Store the pass information
            passes.append({
                "start_time": start_datetime,
                "max_time": max_datetime,
                "end_time": end_datetime,
                "start_elevation": start_elevation_deg,
                "max_elevation": max_elevation_deg,
                "end_elevation": end_elevation_deg,
                "start_azimuth": start_azimuth_deg,
                "max_azimuth": max_azimuth_deg,
                "end_azimuth": end_azimuth_deg,
                "duration_minutes": (end_time - start_time) * 24 * 60,  # Convert to minutes
                "tle1": tle1,
                "tle2": tle2,
            })
        except Exception as e:
            logging.error(f"Error calculating pass: {e}")
            current_date += ephem.minute

    # Sort passes by start time
    passes.sort(key=lambda x: x["start_time"])

    return passes


def azimuth_degrees_to_direction(azimuth: float) -> str:
    """
    Converts azimuth in degrees to compass direction.
    """
    if 0 <= azimuth < 22.5 or 337.5 <= azimuth < 360:
        return "N"
    elif 22.5 <= azimuth < 67.5:
        return "NE"
    elif 67.5 <= azimuth < 112.5:
        return "E"
    elif 112.5 <= azimuth < 157.5:
        return "SE"
    elif 157.5 <= azimuth < 202.5:
        return "S"
    elif 202.5 <= azimuth < 247.5:
        return "SW"
    elif 247.5 <= azimuth < 292.5:
        return "W"
    elif 292.5 <= azimuth < 337.5:
        return "NW"
    else:
        raise ValueError("Invalid azimuth value")

# File to store the transfer queue state
QUEUE_STATE_FILE = "transfer_queue.state"

def stage_directory(queue: TransferQueueManager, nas_dir: str, pass_dir: str, pass_info: PassInfo, sat: Satellite) -> List[Tuple[str, str, int]]:
    """
    Stages the directory for transfer to NAS.
    Returns a list of tuples (source_path, destination_path, attempt_nr)
    """
    transfer_requests = []

    dst_prefix = os.path.join(nas_dir, pass_info["start_time"].strftime("%Y"), pass_info["start_time"].strftime("%Y-%m-%d"),
                    f"{sat['name']}_{pass_info['start_time'].strftime('%Y-%m-%d_%H-%M-%S')}")

    file_sizes : Dict[str, int] = {}

    for root, _, files in os.walk(pass_dir):
        for file in files:
            source_path = os.path.join(root, file)

            # Calculate relative path from pass_dir to preserve directory structure
            rel_path = os.path.relpath(root, pass_dir)
            # If we're in the root directory, don't create an extra subdirectory
            if rel_path == ".":
                destination_path = os.path.join(dst_prefix, file)
            else:
                destination_path = os.path.join(dst_prefix, rel_path, file)


            if sat.get("skip_iq_upload", False) and file.endswith(IQ_DATA_FILE_EXTENSION):
                logging.info(f"Skipping upload of {file} as per skip_iq_upload setting")
                try:
                    os.remove(source_path)
                except Exception as e:
                    logging.error(f"Error deleting file {source_path}: {e}")
                continue

            transfer_requests.append((source_path, destination_path, 0))
            fs = os.path.getsize(source_path)
            logging.info(f"Queued file for transfer: {os.path.basename(file)} | "
                         f"Source: {source_path} | Destination: {destination_path} | Size: {fs / (1024 * 1024):.2f} MB")
            file_sizes[source_path] = fs

    # Sort by file size ascending
    transfer_requests.sort(key=lambda x: file_sizes[x[0]])

    for source_path, destination_path, attempt_nr in transfer_requests:
        queue.add_item(source_path, destination_path, attempt_nr)

    return transfer_requests

# (pass_dir, pass_info, satellite)
decode_queue = queue.Queue()

NEXT_OVERPASS: Union[PassInfo, None] = None

def decode_worker(queue: TransferQueueManager, nas_dir: str) -> None:
    """Worker thread that processes decoding tasks sequentially"""
    while True:
        try:
            pass_dir, pass_info, satellite = decode_queue.get()
            satellite: Satellite

            # If the next overpass is within the next 5 minutes, wait until it is over and only then start decoding
            global NEXT_OVERPASS
            while NEXT_OVERPASS and (NEXT_OVERPASS["start_time"] - datetime.datetime.now()).total_seconds() < 300:
                time_until_end = (NEXT_OVERPASS["end_time"] - datetime.datetime.now()).total_seconds()

                logging.info(f"Decoder: Next overpass is within 5 minutes, waiting at least {time_until_end:.1f} seconds until it is over before decoder start")

                if time_until_end <= 0:
                    break
                else:
                    time.sleep(time_until_end + 60)

            logging.info(f"Starting decoding for {satellite['name']} in {pass_dir}")

            decoders: List[Decoder] = (satellite["decoder"] if isinstance(satellite["decoder"], list) else [satellite["decoder"]])
            for decoder in decoders:
                try:
                    decoder_name = decoder.get("name")
                    logging.info(f"Running decoder {(decoder_name + ' ') if decoder_name else ''}for {satellite['name']} in {pass_dir}")
                    run_decoder(satellite, decoder,  pass_dir)
                    logging.info(f"Decoder {(decoder_name + ' ') if decoder_name else ''}for {satellite['name']} completed successfully")
                except Exception as e:
                    logging.error(f"Error running decoder {(decoder_name + ' ') if decoder_name else ''} for {satellite['name']}: {e}")
                    continue

            files = stage_directory(queue, nas_dir, pass_dir, pass_info, satellite)
            logging.info(f"Decoding complete for {satellite['name']} in {pass_dir}, queued {len(files)} files for transfer to NAS")

            decode_queue.task_done()
        except Exception as e:
            logging.error(f"Error during decoding: {str(e)}")

            files = stage_directory(queue, nas_dir, pass_dir, pass_info, satellite)

            decode_queue.task_done()

def pass_to_string(sat:Satellite, pass_info: PassInfo) -> str:
    duration_str = f"{pass_info['duration_minutes']:.1f}"
    if pass_info['duration_minutes'] < 10:
        duration_str = f" {duration_str}"

    # Format times to show only HH:MM:SS
    start_time_str = pass_info['start_time'].strftime("%H:%M:%S")
    end_time_str = pass_info['end_time'].strftime("%H:%M:%S")

    start_az = azimuth_degrees_to_direction(pass_info['start_azimuth']).ljust(2)
    max_az = azimuth_degrees_to_direction(pass_info['max_azimuth']).ljust(2)
    end_az = azimuth_degrees_to_direction(pass_info['end_azimuth']).ljust(2)

    return f"{sat['name'].ljust(20)} {pass_info['start_time'].date()} {start_time_str} - {end_time_str} | " + \
           f"{duration_str} min | Max Elevation: {pass_info['max_elevation']:.1f}° | " + \
           f"{start_az} -> {max_az} -> {end_az}"

def prioritize_overlapping_passes(next_passes: List[Tuple[Satellite, PassInfo]]) -> List[Tuple[Satellite, PassInfo]]:
    """
    Prioritizes passes when there are overlapping time slots.
    First tries to use satellite priority if available,
    otherwise uses maximum elevation as the criterion.

    If two satellites have the same priority, max elevation is used as a tiebreaker.
    Higher priority numbers indicate higher priority (2 is higher priority than 1).

    Args:
        next_passes: List of (satellite, pass_info) tuples sorted by start time

    Returns:
        List of prioritized passes with overlaps removed
    """
    prioritized_passes = []
    current_index = 0

    while current_index < len(next_passes):
        current_sat, current_pass = next_passes[current_index]

        # Find all overlapping passes with this one
        overlapping_passes = []
        for i in range(current_index, len(next_passes)):
            other_sat, other_pass = next_passes[i]

            # Check if the passes overlap in time
            if (other_pass["start_time"] < current_pass["end_time"] and
                other_pass["end_time"] > current_pass["start_time"]):
                overlapping_passes.append((i, other_sat, other_pass))

        if overlapping_passes:
            # First, check if any satellite has a priority set
            priority_candidates = [(i, sat, pass_info) for i, sat, pass_info in overlapping_passes
                                  if "priority" in sat and sat["priority"] is not None]

            if priority_candidates:
                # Group by priority to find satellites with same priority value
                priority_dict = {}
                for i, sat, pass_info in priority_candidates:
                    priority = sat.get("priority", 0)
                    if priority not in priority_dict:
                        priority_dict[priority] = []
                    priority_dict[priority].append((i, sat, pass_info))

                # Get the highest priority group (highest number = highest priority)
                highest_priority = max(priority_dict.keys())
                highest_priority_group = priority_dict[highest_priority]

                # If there's a tie in priority, use max elevation as tiebreaker
                if len(highest_priority_group) > 1:
                    best_idx, best_sat, best_pass = max(highest_priority_group,
                                                      key=lambda x: x[2]["max_elevation"])
                    selection_criterion = "priority (with elevation tiebreaker)"
                else:
                    best_idx, best_sat, best_pass = highest_priority_group[0]
                    selection_criterion = "priority"
            else:
                # No priorities set, fall back to max elevation
                best_idx, best_sat, best_pass = max(overlapping_passes,
                                                   key=lambda x: x[2]["max_elevation"])
                selection_criterion = "elevation"

            # Log the overlap situation and which pass was selected
            if len(overlapping_passes) > 1:
                logging.info(f"Found {len(overlapping_passes)} overlapping passes (selected by {selection_criterion}):")
                for idx, sat, pass_info in overlapping_passes:
                    priority_str = f" | Priority: {sat.get('priority', 'None')}" if "priority" in sat else ""
                    status = "SELECTED" if idx == best_idx else "SKIPPED "
                    logging.info(f"  {status}: {pass_to_string(sat, pass_info)}{priority_str}")

            prioritized_passes.append((best_sat, best_pass))

            # Skip all overlapping passes
            next_overlap_end = max(p[2]["end_time"] for p in overlapping_passes)

            # Move index to first pass that starts after all overlapping passes end
            while (current_index < len(next_passes) and
                  next_passes[current_index][1]["start_time"] <= next_overlap_end):
                current_index += 1
        else:
            # No overlap, just add the current pass
            prioritized_passes.append((current_sat, current_pass))
            current_index += 1

    return prioritized_passes

def main() -> None:
    p: argparse.ArgumentParser = argparse.ArgumentParser()
    p.add_argument("-c", "--config", default="config.yml",
                   help="Path to YAML config file")
    n2yo_api_key: str = os.getenv("N2YO_API_KEY")
    args: argparse.Namespace = p.parse_args()

    transfer_queue = TransferQueueManager(QUEUE_STATE_FILE)

    cfg: Dict[str, Any] = load_config(args.config)
    sats: List[Satellite] = cfg["satellites"]
    geo: Dict[str, Any] = cfg["location"]
    threshold: float = cfg.get("pass_elevation_threshold_deg", 5.0)
    # Start time should be calculated such that the start
    pass_start_threshold: float = cfg.get("pass_start_elevation_threshold_deg", 5.0)
    update_interval: float = cfg.get("update_interval_hours", 8) * 3600
    nas_dir: str = cfg["nas_directory"]

    os.makedirs(nas_dir, exist_ok=True)

    transfer_thread = threading.Thread(target=file_transfer_worker, daemon=True, args=(transfer_queue,))
    transfer_thread.start()
    decode_thread = threading.Thread(target=decode_worker, daemon=True, args=(transfer_queue, nas_dir))
    decode_thread.start()

    logging.info(f"Loaded config, tracking {len(sats)} satellites, logging to {nas_dir}")

    while True:
        next_passes: List[Tuple[Satellite, PassInfo]] = []
        logging.info(f"Checking for next passes...")

        next_passes = []
        for sat in sats:
            try:
                tle1, tle2 = fetch_tle(sat["norad"], n2yo_api_key)
                logging.info(f"Fetched TLE for {sat['name']}: {tle1.strip()} | {tle2.strip()}")
                passes = get_next_passes(sat["name"], tle1, tle2, geo, threshold, pass_start_threshold, hours=int(1.25 * update_interval / 3600))

                logging.info(f"Found {len(passes)} passes for {sat['name']} in the next {update_interval / 3600:.1f} hours")
                for pass_info in passes:
                    next_passes.append((sat, pass_info))

                if not passes:
                    logging.warning(f"No viable passes for {sat['name']} in next 24 hours")
            except Exception as e:
                logging.error(f"[ERROR] Generating pass info for {sat['name']}: {e}")

        next_passes.sort(key=lambda x: x[1]["start_time"])

        if not next_passes:
            logging.info(f"No passes found in the next {update_interval / 3600:.1f} hours, sleeping for 15 minutes...")
            time.sleep(15 * 60)
            continue

        next_passes = prioritize_overlapping_passes(next_passes)
        next_passes.sort(key=lambda x: x[1]["start_time"])

        for i, (sat, pass_info) in enumerate(next_passes):
            # If the pass is already in the past, skip it, unless we are still before the max time
            if pass_info["end_time"] < datetime.datetime.now():
                logging.info(f"Pass for {sat['name']} is already in the past ({pass_info['start_time']}), skipping...")
                continue

            # If the pass is too far in the future, skip it
            if pass_info["start_time"] > datetime.datetime.now() + datetime.timedelta(seconds=update_interval):
                logging.info(f"Pass for {sat['name']} is too far in the future, skipping...")
                continue

            global NEXT_OVERPASS
            NEXT_OVERPASS = pass_info

            # Wait until ~30 seconds before the pass starts
            wait_time: float = (pass_info["start_time"] - datetime.datetime.now()).total_seconds() - 30
            if wait_time > 0:
                logging.info(f"Waiting {wait_time:.1f} seconds until pass of {sat['name']} starts...")

                # Break the wait into 5-minute intervals and print the next passes
                reminder_interval = 5 * 60
                time_waited = 0

                while time_waited < wait_time:
                    # If we have more than 30 seconds to go, print a reminder
                    time_left = wait_time - time_waited
                    if time_left > 30:
                        logging.info(f"Next passes:")
                        for next_sat, next_pass_info in next_passes[i:i+5]:
                            logging.info(f"  {pass_to_string(next_sat, next_pass_info)}")
                        logging.info(f"Waiting {time_left:.1f} seconds ({time_left/60:.1f} minutes) until pass of {sat['name']}...")

                    # Calculate how long to sleep in this iteration
                    sleep_time = min(reminder_interval, wait_time - time_waited)
                    time.sleep(sleep_time)
                    time_waited += sleep_time
            logging.info(f"Starting pass for {sat['name']} with max elevation {pass_info['max_elevation']:.1f}°...")

            try:
                tmp_dir = tempfile.mkdtemp(prefix='recorder')
                logging.info(f"Recording satellite {sat['name']} to {tmp_dir}")
                # Basically add a bit of buffer time to the recording
                run_recorder(sat, pass_info["duration_minutes"] + 1, tmp_dir)

                pass_info_file = os.path.join(tmp_dir, f"info.json")
                with open(pass_info_file, "w") as f:
                    pass_info_serializable = pass_info.copy()
                    for key in ['start_time', 'max_time', 'end_time']:
                        pass_info_serializable[key] = pass_info_serializable[key].isoformat()

                    json.dump({
                        "satellite": sat,
                        "info": pass_info_serializable,
                    }, f, indent=4)

                if "decoder" in sat and sat["decoder"] is not None:
                    decode_queue.put((tmp_dir, pass_info, sat))
                    logging.info(f"Queued decoding {sat['name']} in {tmp_dir}")
                else:
                    files = stage_directory(transfer_queue, nas_dir, tmp_dir, pass_info, sat)
                    logging.info(f"Recording complete, queued {len(files)} files for transfer to NAS")
            except Exception as e:
                logging.error(f"Error during pass for {sat['name']}: {e}")
                continue


        logging.info(f"All passes processed, waiting for {update_interval} seconds...")
        time.sleep(10)

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
