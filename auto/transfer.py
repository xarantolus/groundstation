import logging
import os
import pickle
import queue
import shutil
import subprocess
import threading
import time

from common import IQ_DATA_FILE_EXTENSION

class TransferQueueManager:
    """
    Manages the file transfer queue with persistent state tracking.
    Thread-safe implementation for adding/removing items and updating state file.
    """
    def __init__(self, state_file_path="transfer_queue.state"):
        self.state_file_path = state_file_path
        self._queue = queue.Queue()
        self._items = []  # Internal tracking of queue items
        self._lock = threading.Lock()  # For thread-safety

        # Load any previously queued items at startup
        self._load_state()

    def add_item(self, source_path, destination_path, attempt_nr=0):
        """Add an item to the transfer queue and update state file"""
        item = (source_path, destination_path, attempt_nr)

        with self._lock:
            self._queue.put(item)
            if item not in self._items:
                self._items.append(item)
                self._save_state()

        logging.debug(f"Added to transfer queue: {os.path.basename(source_path)} -> {destination_path}")
        return True

    def get_next_item(self, block=True, timeout=None):
        """Get the next item from the queue (doesn't modify state file)"""
        return self._queue.get(block=block, timeout=timeout)

    def task_done(self):
        """Mark task as done"""
        self._queue.task_done()

    def update_item(self, old_item, new_item):
        """Update an item in the queue (e.g., when compressing files)"""
        with self._lock:
            if old_item in self._items:
                self._items.remove(old_item)

            self._items.append(new_item)
            self._queue.put(new_item)
            self._save_state()

        return True

    def remove_item(self, item):
        """Remove an item from the tracking list and update state file"""
        with self._lock:
            if item in self._items:
                self._items.remove(item)
                self._save_state()
                return True

        return False

    def get_all_items(self):
        """Get all items currently in the queue"""
        with self._lock:
            return list(self._items)  # Return a copy to prevent modification

    def _save_state(self):
        """Save the current queue state to a file (internal use)"""
        try:
            with open(self.state_file_path, 'wb') as f:
                pickle.dump(self._items, f)

            logging.debug(f"Saved transfer queue state: {len(self._items)} items")
        except Exception as e:
            logging.error(f"Failed to save transfer queue state: {e}")

    def _load_state(self):
        """Load the queue state from file if it exists (internal use)"""
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, 'rb') as f:
                    self._items = pickle.load(f)

                # Add any loaded items to the queue
                for item in self._items:
                    self._queue.put(item)

                logging.info(f"Loaded transfer queue state: {len(self._items)} items")
        except Exception as e:
            logging.error(f"Failed to load transfer queue state: {e}")

def file_transfer_worker(transfer_queue: TransferQueueManager):
    """Worker thread that processes file transfers sequentially and maintains state"""
    while True:
        try:
            source_path, destination_path, attempt_nr = transfer_queue.get_next_item()
            original_source_path = source_path
            original_destination_path = destination_path

            # Skip files that don't exist anymore
            if not os.path.exists(source_path):
                logging.warning(f"Source file no longer exists: {source_path}. Skipping transfer.")
                transfer_queue.remove_item((source_path, destination_path, attempt_nr))
                transfer_queue.task_done()
                continue

            # Can sometimes reduce recordings to 1/4 of the size
            if source_path.endswith(IQ_DATA_FILE_EXTENSION):
                try:
                    compressed_path = source_path + ".zst"
                    subprocess.run(["zstd", "--no-progress", "-9", "-f", "-v", source_path, "-o", compressed_path], check=True)
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
                    logging.error(f"Error compressing {source_path} - uploading anyways: {e}")

            # Get file size before transfer
            file_size_bytes = os.path.getsize(source_path)
            file_size_mb = file_size_bytes / (1024 * 1024)

            logging.info(f"Starting file transfer to NAS: {os.path.basename(destination_path)} | Size: {file_size_mb:.2f} MB"
                         + (f" | Attempt: {attempt_nr}" if attempt_nr > 0 else ""))

            os.makedirs(os.path.dirname(destination_path), exist_ok=True)

            start_time = time.time()
            shutil.move(source_path, destination_path)
            end_time = time.time()

            transfer_duration_seconds = end_time - start_time
            transfer_speed_mbps = file_size_mb / transfer_duration_seconds if transfer_duration_seconds > 0 else 0

            logging.info(f"Completed file transfer to NAS: {destination_path} | "
                         f"Size: {file_size_mb:.2f} MB | "
                         f"Duration: {transfer_duration_seconds:.2f} seconds | "
                         f"Speed: {transfer_speed_mbps:.2f} MB/s")

            # Remove from tracking list once completed
            transfer_queue.remove_item((original_source_path, destination_path, attempt_nr))
            transfer_queue.task_done()

        except Exception as e:
            logging.error(f"Error transferring file to NAS: {str(e)}")
            transfer_queue.task_done()

            if attempt_nr < 3:
                # Update attempt count in the queue
                new_attempt = (source_path, destination_path, attempt_nr + 1)
                transfer_queue.remove_item((source_path, destination_path, attempt_nr))
                transfer_queue.add_item(*new_attempt)

                logging.info(f"Transfer will be retried for {os.path.basename(destination_path)}")
            else:
                logging.error(f"Failed to transfer file after 3 attempts: {os.path.basename(destination_path)}")
                # Remove from tracking list
                transfer_queue.remove_item((source_path, original_destination_path, attempt_nr))

                # Delete the file if transfer fails after 3 attempts
                try:
                    os.remove(source_path)
                    logging.info(f"Deleted file after failed transfer: {source_path}")
                except Exception as e:
                    logging.error(f"Error deleting file after failed transfer: {str(e)}")

        # If the dir is empty, remove it
        try:
            source_dir = os.path.dirname(source_path)

            def remove_empty_dirs(directory):
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
                    os.rmdir(directory)
                    logging.info(f"Removed empty directory: {directory}")

            remove_empty_dirs(source_dir)
        except Exception as e:
            logging.error(f"Error removing directory: {str(e)}")
