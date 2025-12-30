from typing import List, Tuple, Dict, Any, Optional, Callable
import logging
import os
import pickle
import queue
import subprocess
import threading
import time

from common import IQ_DATA_FILE_EXTENSION

# Type alias for a transfer item: (source_path, destination_path, attempt_nr)
TransferItem = Tuple[str, str, int]


class TransferQueueManager:
    """
    Manages the file transfer queue with persistent state tracking.
    Thread-safe implementation for adding/removing items and updating state file.
    """

    def __init__(self, state_file_path: str = "transfer_queue.state"):
        self.state_file_path = state_file_path
        self._queue: queue.Queue[TransferItem] = queue.Queue()
        self._items: List[TransferItem] = []  # Internal tracking of queue items
        self._lock = threading.Lock()  # For thread-safety

        # Active transfers for progress reporting: {source_path: {"progress": float, "total": int, "current": int}}
        self.active_transfers: Dict[str, Dict[str, Any]] = {}
        self.on_progress_update: Optional[Callable[[], None]] = None

        # Load any previously queued items at startup
        self._load_state()

    def add_item(
        self, source_path: str, destination_path: str, attempt_nr: int = 0
    ) -> bool:
        """Add an item to the transfer queue and update state file"""
        item = (source_path, destination_path, attempt_nr)

        with self._lock:
            self._queue.put(item)
            if item not in self._items:
                self._items.append(item)
                self._save_state()

        logging.debug(
            f"Added to transfer queue: {os.path.basename(source_path)} -> {destination_path}"
        )
        return True

    def get_next_item(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> TransferItem:
        """Get the next item from the queue (doesn't modify state file)"""
        return self._queue.get(block=block, timeout=timeout)

    def task_done(self) -> None:
        """Mark task as done"""
        self._queue.task_done()

    def update_item(self, old_item: TransferItem, new_item: TransferItem) -> bool:
        """Update an item in the queue (e.g., when compressing files)"""
        with self._lock:
            if old_item in self._items:
                self._items.remove(old_item)

            self._items.append(new_item)
            self._queue.put(new_item)
            self._save_state()

        return True

    def remove_item(self, item: TransferItem) -> bool:
        """Remove an item from the tracking list and update state file"""
        with self._lock:
            if item in self._items:
                self._items.remove(item)
                self._save_state()
                return True

        return False

    def get_all_items(self) -> List[TransferItem]:
        """Get all items currently in the queue"""
        with self._lock:
            return list(self._items)  # Return a copy to prevent modification

    def _save_state(self) -> None:
        """Save the current queue state to a file (internal use)"""
        try:
            with open(self.state_file_path, "wb") as f:
                pickle.dump(self._items, f)

            logging.debug(f"Saved transfer queue state: {len(self._items)} items")
        except Exception as e:
            logging.error(f"Failed to save transfer queue state: {e}")

    def _load_state(self) -> None:
        """Load the queue state from file if it exists (internal use)"""
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, "rb") as f:
                    self._items = pickle.load(f)

                # Add any loaded items to the queue
                for item in self._items:
                    self._queue.put(item)

                logging.info(f"Loaded transfer queue state: {len(self._items)} items")
        except Exception as e:
            logging.error(f"Failed to load transfer queue state: {e}")

    def update_progress(self, source_path: str, current: int, total: int) -> None:
        """Update progress for an active transfer"""
        with self._lock:
            if total > 0:
                self.active_transfers[source_path] = {
                    "current": current,
                    "total": total,
                    "progress": (current / total) * 100,
                }
            else:
                self.active_transfers[source_path] = {
                    "current": current,
                    "total": total,
                    "progress": 0,
                }
            if self.on_progress_update:
                self.on_progress_update()

    def end_transfer(self, source_path: str) -> None:
        """Remove a transfer from active tracking"""
        with self._lock:
            if source_path in self.active_transfers:
                del self.active_transfers[source_path]
            if self.on_progress_update:
                self.on_progress_update()


def copy_with_progress(
    src: str, dst: str, manager: TransferQueueManager, buffer_size: int = 1024 * 1024
) -> None:
    """Copies a file while reporting progress to the manager"""
    total_size = os.path.getsize(src)
    copied = 0

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        while True:
            buf = fsrc.read(buffer_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
            manager.update_progress(src, copied, total_size)


def file_transfer_worker(transfer_queue: TransferQueueManager) -> None:
    """Worker thread that processes file transfers sequentially and maintains state"""
    while True:
        source_path = ""
        destination_path = ""
        attempt_nr = 0
        try:
            item = transfer_queue.get_next_item()
            source_path, destination_path, attempt_nr = item
            original_source_path = source_path
            original_destination_path = destination_path

            # Skip files that don't exist anymore
            if not os.path.exists(source_path):
                logging.warning(
                    f"Source file no longer exists: {source_path}. Skipping transfer."
                )
                transfer_queue.remove_item((source_path, destination_path, attempt_nr))
                transfer_queue.task_done()
                continue

            # Can sometimes reduce recordings to 1/4 of the size
            if source_path.endswith(IQ_DATA_FILE_EXTENSION):
                try:
                    compressed_path = source_path + ".zst"
                    logging.info(f"Compressing {source_path}...")
                    subprocess.run(
                        [
                            "zstd",
                            "--no-progress",
                            "-9",
                            "-f",
                            "-v",
                            source_path,
                            "-o",
                            compressed_path,
                        ],
                        check=True,
                        capture_output=True,
                    )
                    logging.info(f"Compressed {source_path} to {compressed_path}")
                    os.remove(source_path)

                    # Update entry in queue with new paths
                    old_item = (source_path, destination_path, attempt_nr)
                    new_item = (compressed_path, destination_path + ".zst", attempt_nr)
                    transfer_queue.update_item(old_item, new_item)

                    # Update our local variables to use the compressed paths
                    source_path = compressed_path
                    destination_path = destination_path + ".zst"
                except Exception as e:
                    logging.error(
                        f"Error compressing {source_path} - uploading anyways: {e}"
                    )

            # Get file size before transfer
            file_size_bytes = os.path.getsize(source_path)
            file_size_mb = file_size_bytes / (1024 * 1024)

            logging.info(
                f"Starting file transfer to NAS: {os.path.basename(destination_path)} | Size: {file_size_mb:.2f} MB"
                + (f" | Attempt: {attempt_nr}" if attempt_nr > 0 else "")
            )

            start_time = time.time()
            # Use manual copy to report progress
            copy_with_progress(source_path, destination_path, transfer_queue)
            # After copy, remove source
            os.remove(source_path)

            end_time = time.time()

            transfer_duration_seconds = end_time - start_time
            transfer_speed_mbps = (
                file_size_mb / transfer_duration_seconds
                if transfer_duration_seconds > 0
                else 0
            )

            logging.info(
                f"Completed file transfer to NAS: {destination_path} | "
                f"Size: {file_size_mb:.2f} MB | "
                f"Duration: {transfer_duration_seconds:.2f} seconds | "
                f"Speed: {transfer_speed_mbps:.2f} MB/s"
            )

            # Remove from tracking list once completed
            transfer_queue.remove_item(
                (original_source_path, original_destination_path, attempt_nr)
            )
            transfer_queue.end_transfer(source_path)
            transfer_queue.task_done()

        except Exception as e:
            logging.error(f"Error transferring file to NAS: {str(e)}")
            if source_path:
                transfer_queue.end_transfer(source_path)
            transfer_queue.task_done()

            if attempt_nr < 3 and source_path and destination_path:
                # Update attempt count in the queue
                new_attempt = (source_path, destination_path, attempt_nr + 1)
                transfer_queue.remove_item((source_path, destination_path, attempt_nr))
                transfer_queue.add_item(*new_attempt)

                logging.info(
                    f"Transfer will be retried for {os.path.basename(destination_path)}"
                )
            elif source_path:
                logging.error(
                    f"Failed to transfer file after 3 attempts: {os.path.basename(destination_path)}"
                )
                # Remove from tracking list
                transfer_queue.remove_item((source_path, destination_path, attempt_nr))

                # Delete the file if transfer fails after 3 attempts
                try:
                    os.remove(source_path)
                    logging.info(f"Deleted file after failed transfer: {source_path}")
                except Exception as ex:
                    logging.error(
                        f"Error deleting file after failed transfer: {str(ex)}"
                    )

        # If the dir is empty, remove it
        if source_path:
            try:
                source_dir = os.path.dirname(source_path)

                def remove_empty_dirs(directory: str) -> None:
                    """Recursively removes empty directories"""
                    if not os.path.exists(directory):
                        return

                    # First remove empty subdirectories
                    for item in os.listdir(directory):
                        item_path = os.path.join(directory, item)
                        if os.path.isdir(item_path):
                            remove_empty_dirs(item_path)

                    # Check if directory is now empty after processing subdirectories
                    if not os.listdir(directory):
                        try:
                            os.rmdir(directory)
                            logging.info(f"Removed empty directory: {directory}")
                        except OSError:
                            pass

                remove_empty_dirs(source_dir)
            except Exception as e:
                logging.error(f"Error removing directory: {str(e)}")
