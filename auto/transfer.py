from typing import List, Tuple, Dict, Any, Optional, Callable
import logging
import os
import pickle
import asyncio
import time

from common import IQ_DATA_FILE_EXTENSION

# Type alias for a transfer item: (source_path, destination_path, attempt_nr)
TransferItem = Tuple[str, str, int]


class TransferQueueManager:
    """
    Manages the file transfer queue with persistent state tracking.
    Async implementation.
    """

    def __init__(self, state_file_path: str = "transfer_queue.state"):
        self.state_file_path = state_file_path
        self._queue: asyncio.Queue[TransferItem] = asyncio.Queue()
        self._items: List[TransferItem] = []  # Internal tracking of queue items
        self._lock = asyncio.Lock()  # For async thread-safety

        # Active transfers for progress reporting: {source_path: {"progress": float, "total": int, "current": int}}
        self.active_transfers: Dict[str, Dict[str, Any]] = {}
        self.on_progress_update: Optional[Callable[[], None]] = None

        # Load any previously queued items at startup
        # Note: _load_state is sync but that's fine for init
        self._load_state()

    async def add_item(
        self, source_path: str, destination_path: str, attempt_nr: int = 0
    ) -> bool:
        """Add an item to the transfer queue and update state file"""
        item = (source_path, destination_path, attempt_nr)

        async with self._lock:
            await self._queue.put(item)
            if item not in self._items:
                self._items.append(item)
                self._save_state()

        logging.debug(
            f"Added to transfer queue: {os.path.basename(source_path)} -> {destination_path}"
        )
        return True

    async def get_next_item(self) -> TransferItem:
        """Get the next item from the queue (doesn't modify state file)"""
        return await self._queue.get()

    def task_done(self) -> None:
        """Mark task as done"""
        self._queue.task_done()

    async def update_item(self, old_item: TransferItem, new_item: TransferItem) -> bool:
        """Update an item in the queue (e.g., when compressing files)"""
        async with self._lock:
            if old_item in self._items:
                self._items.remove(old_item)

            self._items.append(new_item)
            # Re-queueing logic is tricky because we peeled it off via get_next_item.
            # In the original code, update_item was called while holding the item.
            # The worker calls update_item.
            # Since the worker hasn't called task_done/finished yet, the item is technically "in progress".
            # But the worker wants to continue processing `new_item` immediately?
            # Actually, the original worker code:
            #   get_next_item -> doing stuff -> update_item -> continue processing (with new vars)
            #   original update_item also put it back in queue?
            #   "self._queue.put(new_item)"
            # Wait, if we put it back in queue, another worker (if multiple) could pick it up.
            # But we only have one worker.
            # If we put it back in queue, the CURRENT worker loop will treat it as a new item in next iteration?
            # No, the current worker already has `source_path` updated.
            # Re-reading original Code:
            #   transfer_queue.update_item(old_item, new_item)
            #   ...
            #   source_path = compressed_path
            # The worker continues with the new path.
            # But update_item puts it in queue.
            # So it would be processed AGAIN?
            # Original code: `self._queue.put(new_item)`. Yes.
            # But the worker continues to process `source_path = compressed_path`.
            # So it processes it, finishes, calls process_done.
            # THEN, it loops around, gets the item we just put back?
            # Duplicate transfer?
            # Actually, `remove_item` is called at the end with `original_source_path`?
            # "transfer_queue.remove_item((original_source_path, ...))"
            # This is messy.

            # Let's fix this logic. We shouldn't re-queue if we are continuing to process it.
            # But we want to persist the state change in case of crash.
            # So just update `self._items` and save state.
            pass
            self._save_state()

        return True

    async def remove_item(self, item: TransferItem) -> bool:
        """Remove an item from the tracking list and update state file"""
        async with self._lock:
            if item in self._items:
                self._items.remove(item)
                self._save_state()
                return True

        return False

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
                    self._queue.put_nowait(item)

                logging.info(f"Loaded transfer queue state: {len(self._items)} items")
        except Exception as e:
            logging.error(f"Failed to load transfer queue state: {e}")

    async def update_progress(self, source_path: str, current: int, total: int) -> None:
        """Update progress for an active transfer"""
        # No need for lock for simple dict update, but good for consistency
        async with self._lock:
            if total > 0:
                progress = (current / total) * 100
                self.active_transfers[source_path] = {
                    "current": current,
                    "total": total,
                    "progress": progress,
                }
            else:
                self.active_transfers[source_path] = {
                    "current": current,
                    "total": total,
                    "progress": 0,
                }

            # Callback might be sync (RichTUI update)
            if self.on_progress_update:
                self.on_progress_update()

    async def end_transfer(self, source_path: str) -> None:
        """Remove a transfer from active tracking"""
        async with self._lock:
            if source_path in self.active_transfers:
                del self.active_transfers[source_path]
            if self.on_progress_update:
                self.on_progress_update()


def copy_with_progress(
    src: str, dst: str, manager: TransferQueueManager, loop: asyncio.AbstractEventLoop, buffer_size: int = 32 * 1024
) -> None:
    """Copies a file while reporting progress to the manager. Runs in thread executor."""
    total_size = os.path.getsize(src)
    copied = 0

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    def report_progress(c, t):
        asyncio.run_coroutine_threadsafe(manager.update_progress(src, c, t), loop)

    with open(src, "rb") as fsrc, open(dst, "wb") as fdst:
        while True:
            buf = fsrc.read(buffer_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
            report_progress(copied, total_size)


async def compress_file(source_path: str) -> str:
    """Compresses file using zstd in a subprocess"""
    compressed_path = source_path + ".zst"
    logging.info(f"Compressing {source_path}...")

    proc = await asyncio.create_subprocess_exec(
        "zstd",
        "--no-progress",
        "-9",
        "-f",
        "-v",
        source_path,
        "-o",
        compressed_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"zstd failed with return code {proc.returncode}")

    logging.info(f"Compressed {source_path} to {compressed_path}")
    os.remove(source_path)
    return compressed_path


async def file_transfer_worker(transfer_queue: TransferQueueManager):
    """Worker task that processes file transfers."""
    loop = asyncio.get_running_loop()

    while True:
        source_path = ""
        destination_path = ""
        attempt_nr = 0
        try:
            item = await transfer_queue.get_next_item()
            source_path, destination_path, attempt_nr = item
            original_source_path = source_path
            original_destination_path = destination_path

            # Skip files that don't exist anymore
            if not os.path.exists(source_path):
                logging.warning(
                    f"Source file no longer exists: {source_path}. Skipping transfer."
                )
                await transfer_queue.remove_item(item)
                transfer_queue.task_done()
                continue

            # Compression
            if source_path.endswith(IQ_DATA_FILE_EXTENSION):
                try:
                    compressed_path = await compress_file(source_path)

                    # Update entry in queue (persist state)
                    # Note: We are NOT putting it back in queue to be picked up again, just updating tracking
                    new_item = (compressed_path, destination_path + ".zst", attempt_nr)
                    await transfer_queue.update_item(item, new_item)

                    source_path = compressed_path
                    destination_path = destination_path + ".zst"
                except Exception as e:
                    logging.error(
                        f"Error compressing {source_path} - uploading anyways: {e}"
                    )

            # File Transfer with Retry
            file_size_bytes = os.path.getsize(source_path)
            file_size_mb = file_size_bytes / (1024 * 1024)

            logging.info(
                f"Starting file transfer to NAS: {os.path.basename(destination_path)} | Size: {file_size_mb:.2f} MB"
                + (f" | Attempt: {attempt_nr + 1}" if attempt_nr > 0 else "")
            )

            start_time = time.time()

            # Retry logic for the copy operation
            max_retries = 3
            for copy_attempt in range(max_retries):
                try:
                    await loop.run_in_executor(
                        None,
                        copy_with_progress,
                        source_path,
                        destination_path,
                        transfer_queue,
                        loop
                    )
                    break  # Success
                except Exception as e:
                    if copy_attempt < max_retries - 1:
                        wait_time = 10 * (copy_attempt + 1)
                        logging.warning(
                            f"Copy failed ({e}). Retrying in {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        raise e  # Re-raise after last attempt

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
            await transfer_queue.remove_item(
                (original_source_path, original_destination_path, attempt_nr)
            )
            await transfer_queue.end_transfer(source_path)
            transfer_queue.task_done()

        except Exception as e:
            logging.error(f"Error transferring file to NAS: {str(e)}")
            if source_path:
                await transfer_queue.end_transfer(source_path)
            transfer_queue.task_done()

            if attempt_nr < 3 and source_path and destination_path:
                # Re-queue for later retry (application level retry)
                # Wait, we already did retries for the copy.
                # If we are here, it means even the retries inside failed OR something else failed.
                # The user asked for "retry file copies if they fail...".
                # I implemented the inner loop.
                # Keeping the outer loop logic (attempt_nr) as a fallback is good practice?
                # The original code had attempt_nr.
                # Let's keep it but maybe increase delay.

                new_attempt = (source_path, destination_path, attempt_nr + 1)
                await transfer_queue.remove_item(
                    (original_source_path, original_destination_path, attempt_nr)
                )
                await transfer_queue.add_item(*new_attempt)

                logging.info(
                    f"Transfer task failed. Re-queued for retry {attempt_nr + 2}."
                )
                await asyncio.sleep(5)  # Small delay before next loop picks it up?
            elif source_path:
                logging.error(
                    f"Failed to transfer file after multiple attempts: {os.path.basename(destination_path)}"
                )
                await transfer_queue.remove_item(
                    (original_source_path, original_destination_path, attempt_nr)
                )

                # Delete the file if transfer fails
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
                # Cleaning up empty dirs is fast, do it in executor or just sync?
                # Sync is fine for metadata ops usually, but strict async would verify.
                # stick to sync for simplicity on cleanup

                def remove_empty_dirs(directory: str) -> None:
                    if not os.path.exists(directory):
                        return
                    for item in os.listdir(directory):
                        item_path = os.path.join(directory, item)
                        if os.path.isdir(item_path):
                            remove_empty_dirs(item_path)
                    if not os.listdir(directory):
                        try:
                            os.rmdir(directory)
                            logging.info(f"Removed empty directory: {directory}")
                        except OSError:
                            pass

                remove_empty_dirs(source_dir)
            except Exception as e:
                logging.error(f"Error removing directory: {str(e)}")
