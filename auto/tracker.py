import json
import os
import queue
import threading
import time
import tempfile
import argparse
import datetime
import requests
import logging
import collections
from typing import List, Tuple, Dict, Any, Optional

# Rich UI imports
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.logging import RichHandler
from rich.console import Console, Group, ConsoleOptions, RenderResult

from common import Satellite, PassInfo, IQ_DATA_FILE_EXTENSION
from config import load_config
from external import run_recorder, run_decoder
from transfer import TransferQueueManager, file_transfer_worker

# Constants
QUEUE_STATE_FILE = "transfer_queue.state"


class LogBufferHandler(logging.Handler):
    """A logging handler that keeps the last N log messages in a buffer."""

    def __init__(self, capacity: int = 100):
        super().__init__()
        self.buffer = collections.deque(maxlen=capacity)

    def emit(self, record):
        try:
            msg = self.format(record)
            self.buffer.append(msg)
        except Exception:
            self.handleError(record)


# Configure logging
console = Console()
log_handler = LogBufferHandler(capacity=50)
log_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(message)s", datefmt="%H:%M:%S")
)

file_handler = logging.FileHandler("tracker.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        log_handler,
        file_handler,
        RichHandler(console=console, rich_tracebacks=True),
    ],
)
logger = logging.getLogger("groundstation")


def azimuth_degrees_to_direction(azimuth: float) -> str:
    """Converts azimuth in degrees to compass direction."""
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


class PassManager:
    """Manages satellite TLEs and pass predictions."""

    def __init__(self, n2yo_api_key: Optional[str] = None):
        self.n2yo_api_key = n2yo_api_key
        self.tle_cache: Dict[str, Tuple[str, str]] = {}

    def fetch_tle(self, norad: str) -> Tuple[str, str]:
        """Fetches TLE for a satellite by its NORAD ID."""
        if norad in self.tle_cache:
            return self.tle_cache[norad]

        try:
            r = requests.get(
                f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad}&FORMAT=TLE",
                timeout=10,
            )
            r.raise_for_status()
            tle_text = r.text
        except requests.RequestException as e:
            logger.error(f"Failed to fetch TLE for {norad} from Celestrak: {e}")
            if self.n2yo_api_key:
                try:
                    r = requests.get(
                        f"https://api.n2yo.com/rest/v1/satellite/tle/{norad}/?apiKey={self.n2yo_api_key}",
                        timeout=10,
                    )
                    r.raise_for_status()
                    data = r.json()
                    tle_text = data.get("tle", "")
                except Exception as ex:
                    logger.error(f"Failed to fetch TLE for {norad} from N2YO: {ex}")
                    raise RuntimeError(f"Could not fetch TLE for {norad}") from ex
            else:
                raise RuntimeError(
                    f"Failed to fetch TLE for {norad} from Celestrak and no N2YO API key"
                ) from e

        lines = [line.strip() for line in tle_text.splitlines() if line.strip()]
        if len(lines) >= 3:
            tle = (lines[-2], lines[-1])
        elif len(lines) == 2:
            tle = (lines[0], lines[1])
        else:
            raise RuntimeError(f"Unexpected TLE format for {norad}")

        self.tle_cache[norad] = tle
        return tle

    def get_satellite_passes(
        self,
        sat: Satellite,
        geo: Dict[str, Any],
        threshold_deg: float,
        pass_start_threshold: float,
        hours: int = 24,
    ) -> List[PassInfo]:
        """Calculates passes for a single satellite over the next few hours."""
        import ephem

        tle1, tle2 = self.fetch_tle(sat["norad"])

        passes = []
        observer = ephem.Observer()
        observer.lat = str(geo["lat"])
        observer.lon = str(geo["lon"])
        observer.elev = geo["alt"] / 1000
        observer.horizon = "0"

        start_date = ephem.now() - ephem.minute * 5
        observer.date = start_date
        end_date = start_date + ephem.hour * hours
        satellite = ephem.readtle(sat["name"], tle1, tle2)

        current_date = start_date
        while current_date < end_date:
            try:
                observer.date = current_date
                next_pass = observer.next_pass(satellite)
                if next_pass is None or next_pass[0] > end_date:
                    break

                rise_time, rise_az, max_time, max_el, set_time, set_az = next_pass
                current_date = set_time + ephem.minute

                if max_el * 180 / ephem.pi < threshold_deg:
                    continue

                # Find crossing times
                crossing_time_ascending = None
                time_step = (max_time - rise_time) / 100
                for i in range(100):
                    check_time = rise_time + time_step * i
                    observer.date = check_time
                    satellite.compute(observer)
                    el_deg = float(satellite.alt) * 180 / ephem.pi
                    if el_deg >= pass_start_threshold:
                        crossing_time_ascending = check_time
                        break

                crossing_time_descending = None
                time_step = (set_time - max_time) / 100
                for i in range(100):
                    check_time = max_time + time_step * i
                    observer.date = check_time
                    satellite.compute(observer)
                    el_deg = float(satellite.alt) * 180 / ephem.pi
                    if el_deg <= pass_start_threshold:
                        crossing_time_descending = check_time
                        break

                start_time = (
                    crossing_time_ascending if crossing_time_ascending else rise_time
                )
                end_time = (
                    crossing_time_descending if crossing_time_descending else set_time
                )

                observer.date = start_time
                satellite.compute(observer)
                start_elevation_deg = float(satellite.alt) * 180 / ephem.pi
                start_azimuth_deg = float(satellite.az) * 180 / ephem.pi

                observer.date = max_time
                satellite.compute(observer)
                max_elevation_deg = float(satellite.alt) * 180 / ephem.pi
                max_azimuth_deg = float(satellite.az) * 180 / ephem.pi

                observer.date = end_time
                satellite.compute(observer)
                end_elevation_deg = float(satellite.alt) * 180 / ephem.pi
                end_azimuth_deg = float(satellite.az) * 180 / ephem.pi

                passes.append(
                    {
                        "start_time": ephem.localtime(start_time),
                        "max_time": ephem.localtime(max_time),
                        "end_time": ephem.localtime(end_time),
                        "start_elevation": start_elevation_deg,
                        "max_elevation": max_elevation_deg,
                        "end_elevation": end_elevation_deg,
                        "start_azimuth": start_azimuth_deg,
                        "max_azimuth": max_azimuth_deg,
                        "end_azimuth": end_azimuth_deg,
                        "duration_minutes": (end_time - start_time) * 24 * 60,
                        "tle1": tle1,
                        "tle2": tle2,
                    }
                )
            except Exception as e:
                logger.error(f"Error calculating pass: {e}")
                current_date += ephem.minute

        return passes

    def predict_all(
        self,
        sats: List[Satellite],
        geo: Dict[str, Any],
        threshold: float,
        pass_start_threshold: float,
        hours: int,
    ) -> List[Tuple[Satellite, PassInfo]]:
        """Predicts and prioritizes passes for all satellites."""
        all_passes = []
        for sat in sats:
            try:
                passes = self.get_satellite_passes(
                    sat, geo, threshold, pass_start_threshold, hours
                )
                all_passes.extend([(sat, p) for p in passes])
            except Exception as e:
                logger.error(f"Error predicting for {sat['name']}: {e}")

        all_passes.sort(key=lambda x: x[1]["start_time"])
        return self.prioritize(all_passes)

    def prioritize(
        self, next_passes: List[Tuple[Satellite, PassInfo]]
    ) -> List[Tuple[Satellite, PassInfo]]:
        """Handles overlapping passes based on priority and elevation."""
        prioritized_passes = []
        current_index = 0

        while current_index < len(next_passes):
            current_sat, current_pass = next_passes[current_index]
            overlapping_passes = []
            for i in range(current_index, len(next_passes)):
                other_sat, other_pass = next_passes[i]
                if (
                    other_pass["start_time"] < current_pass["end_time"]
                    and other_pass["end_time"] > current_pass["start_time"]
                ):
                    overlapping_passes.append((i, other_sat, other_pass))

            if overlapping_passes:
                priority_candidates = [
                    p for p in overlapping_passes if p[1].get("priority") is not None
                ]
                if priority_candidates:
                    highest_priority = max(
                        p[1]["priority"] for p in priority_candidates
                    )
                    highest_priority_group = [
                        p
                        for p in priority_candidates
                        if p[1]["priority"] == highest_priority
                    ]
                    if len(highest_priority_group) > 1:
                        best_idx, best_sat, best_pass = max(
                            highest_priority_group, key=lambda x: x[2]["max_elevation"]
                        )
                    else:
                        best_idx, best_sat, best_pass = highest_priority_group[0]
                else:
                    best_idx, best_sat, best_pass = max(
                        overlapping_passes, key=lambda x: x[2]["max_elevation"]
                    )

                if len(overlapping_passes) > 1:
                    logger.info(
                        f"Overlap detected: {len(overlapping_passes)} passes. Selected {best_sat['name']}."
                    )

                prioritized_passes.append((best_sat, best_pass))
                next_overlap_end = max(p[2]["end_time"] for p in overlapping_passes)
                while (
                    current_index < len(next_passes)
                    and next_passes[current_index][1]["start_time"] <= next_overlap_end
                ):
                    current_index += 1
            else:
                prioritized_passes.append((current_sat, current_pass))
                current_index += 1

        return prioritized_passes


class RichTUI:
    """Manages the Rich Terminal User Interface."""

    def __init__(self, transfer_manager: TransferQueueManager):
        self.transfer_manager = transfer_manager
        self.next_passes: List[Tuple[Satellite, PassInfo]] = []
        self.decoder_logs = collections.deque(maxlen=20)
        self.is_decoding = False
        self.layout = Layout()
        self._setup_layout()

    def _setup_layout(self):
        self.layout.split(Layout(name="top", size=15), Layout(name="bottom"))
        self.layout["top"].split_row(
            Layout(name="passes", ratio=2), Layout(name="transfers", ratio=1)
        )
        self.layout["bottom"].split_row(
            Layout(name="main_log"), Layout(name="decoder_log", visible=False)
        )

    def update_passes(self, passes: List[Tuple[Satellite, PassInfo]]):
        self.next_passes = passes

    def add_decoder_log(self, msg: str):
        self.decoder_logs.append(msg)

    def _generate_passes_table(self) -> Table:
        table = Table(title="Next Overpasses", expand=True)
        table.add_column("Satellite")
        table.add_column("Start")
        table.add_column("Duration")
        table.add_column("Max El.")
        table.add_column("Direction")

        now = datetime.datetime.now()
        for sat, p in self.next_passes[:10]:
            start_str = p["start_time"].strftime("%H:%M:%S")
            if p["start_time"] < now < p["end_time"]:
                start_str = f"[bold green]{start_str} (LIVE)[/bold green]"

            table.add_row(
                sat["name"],
                start_str,
                f"{p['duration_minutes']:.1f}m",
                f"{p['max_elevation']:.1f}°",
                f"{azimuth_degrees_to_direction(p['start_azimuth'])}->{azimuth_degrees_to_direction(p['max_azimuth'])}->{azimuth_degrees_to_direction(p['end_azimuth'])}",
            )
        return table

    def _generate_transfers_panel(self) -> Panel:
        active = self.transfer_manager.active_transfers
        if not active:
            return Panel("[dim]No active transfers[/dim]", title="Active Transfers")

        progress_group = []
        for path, data in active.items():
            fname = os.path.basename(path)
            progress_group.append(f"{fname}")
            prog = data["progress"]
            bar = "█" * int(prog / 5) + "░" * (20 - int(prog / 5))
            progress_group.append(f"[{bar}] {prog:>3.0f}%")

        return Panel(Group(*progress_group), title="Active Transfers")

    def _generate_log_panel(self) -> Panel:
        return Panel("\n".join(list(log_handler.buffer)), title="Log")

    def _generate_decoder_log_panel(self) -> Panel:
        return Panel(
            "\n".join(list(self.decoder_logs)), title="Decoder Log", border_style="cyan"
        )

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        yield self.__call__()

    def __call__(self) -> Layout:
        self.layout["passes"].update(self._generate_passes_table())
        self.layout["transfers"].update(self._generate_transfers_panel())
        self.layout["main_log"].update(self._generate_log_panel())

        if self.is_decoding:
            self.layout["decoder_log"].visible = True
            self.layout["decoder_log"].update(self._generate_decoder_log_panel())
        else:
            self.layout["decoder_log"].visible = False

        return self.layout


class Orchestrator:
    """Coordinates the overall operation of the groundstation."""

    def __init__(self, config_path: str):
        self.cfg = load_config(config_path)
        self.sats = self.cfg["satellites"]
        self.geo = self.cfg["location"]
        self.threshold = self.cfg.get("pass_elevation_threshold_deg", 5.0)
        self.pass_start_threshold = self.cfg.get(
            "pass_start_elevation_threshold_deg", 5.0
        )
        self.update_interval_hours = self.cfg.get("update_interval_hours", 8)
        self.nas_dir = self.cfg["nas_directory"]
        os.makedirs(self.nas_dir, exist_ok=True)

        self.pass_manager = PassManager(n2yo_api_key=os.getenv("N2YO_API_KEY"))
        self.transfer_manager = TransferQueueManager(QUEUE_STATE_FILE)
        self.tui = RichTUI(self.transfer_manager)

        self.decode_queue = queue.Queue()
        self.next_overpass: Optional[PassInfo] = None

    def start(self):
        """Starts the orchestrator."""
        threading.Thread(
            target=file_transfer_worker, daemon=True, args=(self.transfer_manager,)
        ).start()
        threading.Thread(target=self.decode_worker, daemon=True).start()

        with Live(self.tui, console=console, screen=True, refresh_per_second=4):
            while True:
                logger.info("Updating pass predictions...")
                next_passes = self.pass_manager.predict_all(
                    self.sats,
                    self.geo,
                    self.threshold,
                    self.pass_start_threshold,
                    hours=int(1.25 * self.update_interval_hours),
                )
                self.tui.update_passes(next_passes)

                if not next_passes:
                    logger.info("No passes found. Sleeping for 15m.")
                    time.sleep(15 * 60)
                    continue

                for i, (sat, pass_info) in enumerate(next_passes):
                    now = datetime.datetime.now()
                    if pass_info["end_time"] < now:
                        continue

                    if pass_info["start_time"] > now + datetime.timedelta(
                        hours=self.update_interval_hours
                    ):
                        break

                    self.next_overpass = pass_info

                    wait_time = (
                        pass_info["start_time"] - datetime.datetime.now()
                    ).total_seconds() - 30
                    if wait_time > 0:
                        logger.info(
                            f"Waiting for {sat['name']} (starts in {wait_time / 60:.1f}m)"
                        )
                        chunk = 5
                        while wait_time > 0:
                            time.sleep(min(chunk, wait_time))
                            wait_time -= chunk

                    self.record_pass(sat, pass_info)

                logger.info("Cycle complete. Waiting 10s...")
                time.sleep(10)

    def record_pass(self, sat: Satellite, pass_info: PassInfo):
        """Handles the recording of a satellite pass."""
        logger.info(
            f"Starting pass for {sat['name']} (Max Elev: {pass_info['max_elevation']:.1f}°)"
        )
        try:
            tmp_dir = tempfile.mkdtemp(prefix="recorder")

            def recorder_log(line: str):
                logger.info(f"Recorder: {line}")

            run_recorder(
                sat,
                pass_info["duration_minutes"] + 1,
                tmp_dir,
                log_callback=recorder_log,
            )

            pass_info_file = os.path.join(tmp_dir, "info.json")
            with open(pass_info_file, "w") as f:
                p_copy = pass_info.copy()
                for k in ["start_time", "max_time", "end_time"]:
                    p_copy[k] = p_copy[k].isoformat()
                json.dump({"satellite": sat, "info": p_copy}, f, indent=4)

            if sat.get("decoder"):
                self.decode_queue.put((tmp_dir, pass_info, sat))
                logger.info(f"Queued decoding for {sat['name']}")
            else:
                self.stage_for_transfer(tmp_dir, pass_info, sat)
        except Exception as e:
            logger.error(f"Error during pass for {sat['name']}: {e}")

    def stage_for_transfer(self, source_dir: str, pass_info: PassInfo, sat: Satellite):
        """Stages files for transfer to NAS."""
        dst_prefix = os.path.join(
            self.nas_dir,
            pass_info["start_time"].strftime("%Y"),
            pass_info["start_time"].strftime("%Y-%m-%d"),
            f"{sat['name']}_{pass_info['start_time'].strftime('%Y-%m-%d_%H-%M-%S')}",
        )

        queued_count = 0
        for root, _, files in os.walk(source_dir):
            for file in files:
                src = os.path.join(root, file)
                rel = os.path.relpath(root, source_dir)
                dst = os.path.join(
                    dst_prefix, file if rel == "." else os.path.join(rel, file)
                )

                if sat.get("skip_iq_upload") and file.endswith(IQ_DATA_FILE_EXTENSION):
                    try:
                        os.remove(src)
                    except Exception:
                        pass
                    continue

                self.transfer_manager.add_item(src, dst, 0)
                queued_count += 1

        logger.info(f"Queued {queued_count} files for transfer for {sat['name']}")

    def decode_worker(self):
        """Processes decoding tasks sequentially."""
        while True:
            try:
                pass_dir, pass_info, sat = self.decode_queue.get()

                while True:
                    time_to_next = (
                        (
                            self.next_overpass["start_time"] - datetime.datetime.now()
                        ).total_seconds()
                        if self.next_overpass
                        else 999
                    )
                    if 0 < time_to_next < 300:
                        logger.info("Decoder: Next pass soon, waiting...")
                        time.sleep(time_to_next + 60)
                    else:
                        break

                logger.info(f"Starting decoding for {sat['name']}")
                self.tui.is_decoding = True
                self.tui.decoder_logs.clear()

                def decoder_log(line: str):
                    self.tui.add_decoder_log(line)
                    # Also write to log file, but not to the TUI main log panel
                    # We can use the logger's file handler directly or just logger.info and filter it?
                    # Actually user said "decoder output should only be shown in the log file and in a second log window"
                    # So we should avoid logger.info() because that goes to primary TUI log.
                    # We can log to the file handler specifically.
                    record = logging.LogRecord(
                        "decoder", logging.INFO, __file__, 0, line, (), None
                    )
                    file_handler.emit(record)

                decoders = (
                    sat["decoder"]
                    if isinstance(sat["decoder"], list)
                    else [sat["decoder"]]
                )
                for decoder in decoders:
                    try:
                        run_decoder(sat, decoder, pass_dir, log_callback=decoder_log)
                    except Exception as e:
                        logger.error(f"Decoder error for {sat['name']}: {e}")

                self.tui.is_decoding = False
                self.stage_for_transfer(pass_dir, pass_info, sat)
                self.decode_queue.task_done()
            except Exception as e:
                logger.error(f"Decode worker error: {e}")
                self.tui.is_decoding = False


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-c", "--config", default="config.yml", help="Path to YAML config file"
    )
    args = p.parse_args()

    from dotenv import load_dotenv

    load_dotenv()

    orch = Orchestrator(args.config)
    try:
        orch.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    main()
