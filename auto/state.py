from __future__ import annotations

import datetime
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, Iterable, List

from pydantic import BaseModel, ValidationError

from .models import Pass, TransferRequest

logger = logging.getLogger("groundstation.state")


def _fsync_dir(path: Path) -> None:
    """Flush a directory's entry table to disk. Without this, an fsync'd
    file can still vanish after a power loss: the inode is durable but the
    directory's link to it isn't, so the filename won't reappear on reboot.
    No-op on Windows (opening a directory for fsync isn't supported there).
    """
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _atomic_append_line(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    first_write = not path.exists()
    with open(path, "a", encoding="utf-8") as f:
        f.write(text.rstrip("\n") + "\n")
        f.flush()
        os.fsync(f.fileno())
    if first_write:
        # First append creates the file; subsequent fsyncs on an existing
        # file already propagate size changes, but the initial dirent needs
        # an explicit dir fsync.
        _fsync_dir(path.parent)


def _atomic_rewrite_jsonl(path: Path, entries: Iterable[Dict]) -> None:
    """Replace `path` with one JSON line per entry. Written to a sibling .tmp
    then renamed — a crash mid-write leaves the previous (uncompacted) file
    intact rather than a truncated one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, default=_json_default) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


class StateStore:
    """Owns all on-disk persistence. JSON + JSONL, atomic writes.

    Layout:
      {root}/passes/<pass_id>.json
      {root}/transfer_queue.jsonl       append-only log (put/tombstone)
      {root}/transfer_completed.jsonl   append-only, bounded tail
      {root}/pending_decodes.jsonl      append-only log (queue/tombstone)
      {root}/corrupt/                   quarantine for invalid files
    """

    def __init__(self, root: str = "state") -> None:
        self.root = Path(root)
        self.passes_dir = self.root / "passes"
        self.transfer_queue_log = self.root / "transfer_queue.jsonl"
        self.transfer_completed_log = self.root / "transfer_completed.jsonl"
        self.pending_decodes_log = self.root / "pending_decodes.jsonl"
        self.corrupt_dir = self.root / "corrupt"
        self.root.mkdir(parents=True, exist_ok=True)
        self.passes_dir.mkdir(parents=True, exist_ok=True)

    # ---------- Passes ----------

    def save_pass(self, p: Pass) -> None:
        p.updated_at = datetime.datetime.now()
        path = self.passes_dir / f"{p.id}.json"
        try:
            _atomic_write_text(path, p.model_dump_json(indent=2))
        except OSError:
            logger.exception("state_write_failed: could not persist pass %s", p.id)

    def delete_pass(self, pass_id: str) -> None:
        path = self.passes_dir / f"{pass_id}.json"
        try:
            path.unlink(missing_ok=True)
        except OSError:
            logger.exception("could not delete pass %s", pass_id)

    def load_passes(self) -> List[Pass]:
        results: List[Pass] = []
        for f in sorted(self.passes_dir.glob("*.json")):
            try:
                text = f.read_text(encoding="utf-8")
                results.append(Pass.model_validate_json(text))
            except (ValidationError, json.JSONDecodeError, OSError) as e:
                logger.warning("quarantining corrupt pass file %s: %s", f, e)
                self._quarantine(f)
        if results:
            counts: Dict[str, int] = {}
            for p in results:
                counts[p.status.value] = counts.get(p.status.value, 0) + 1
            summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
            logger.info("state: loaded %d pass(es) — %s", len(results), summary)
        else:
            logger.info("state: no persisted passes")
        return results

    def _quarantine(self, path: Path) -> None:
        try:
            self.corrupt_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            dest = self.corrupt_dir / f"{stamp}-{path.name}"
            shutil.move(str(path), dest)
        except OSError:
            logger.exception("failed to quarantine %s", path)

    # ---------- Transfer queue ----------

    def transfer_put(self, req: TransferRequest) -> None:
        self._append_log(self.transfer_queue_log, {"op": "put", "req": req.model_dump(mode="json")})

    def transfer_tombstone(self, req_id: str) -> None:
        self._append_log(self.transfer_queue_log, {"op": "done", "id": req_id})

    def load_transfer_queue(self) -> List[TransferRequest]:
        entries = self._read_jsonl(self.transfer_queue_log)
        by_id: Dict[str, TransferRequest] = {}
        for e in entries:
            op = e.get("op")
            if op == "put":
                try:
                    req = TransferRequest.model_validate(e["req"])
                    by_id[req.id] = req
                except (ValidationError, KeyError) as err:
                    logger.warning("dropping invalid transfer queue entry: %s", err)
            elif op == "done":
                by_id.pop(e.get("id"), None)
        pending = list(by_id.values())
        logger.info(
            "state: loaded %d pending transfer(s) from %s",
            len(pending),
            self.transfer_queue_log.name,
        )
        return pending

    # ---------- Decode queue ----------

    def decode_put(self, pass_id: str, decoder_index: int) -> None:
        self._append_log(
            self.pending_decodes_log,
            {"op": "put", "pass_id": pass_id, "decoder_index": decoder_index},
        )

    def decode_tombstone(self, pass_id: str, decoder_index: int) -> None:
        self._append_log(
            self.pending_decodes_log,
            {"op": "done", "pass_id": pass_id, "decoder_index": decoder_index},
        )

    def load_decode_queue(self) -> List[tuple[str, int]]:
        entries = self._read_jsonl(self.pending_decodes_log)
        pending: Dict[tuple[str, int], bool] = {}
        for e in entries:
            try:
                key = (e["pass_id"], int(e["decoder_index"]))
            except (KeyError, ValueError, TypeError):
                continue
            if e.get("op") == "put":
                pending[key] = True
            elif e.get("op") == "done":
                pending.pop(key, None)
        pending_keys = list(pending.keys())
        logger.info(
            "state: loaded %d pending decode(s) from %s",
            len(pending_keys),
            self.pending_decodes_log.name,
        )
        return pending_keys

    # ---------- Transfer completed history ----------

    def record_completed(self, info: Dict[str, object]) -> None:
        self._append_log(self.transfer_completed_log, info)

    def tail_completed(self, limit: int = 20) -> List[Dict]:
        entries = self._read_jsonl(self.transfer_completed_log)
        return entries[-limit:]

    # ---------- Compaction ----------

    def compact_transfer_queue(self) -> int:
        """Rewrite transfer_queue.jsonl to hold only one `put` line per open
        request. Returns the number of lines dropped (0 if the file did not
        exist or was already minimal)."""
        if not self.transfer_queue_log.exists():
            return 0
        before = _count_lines(self.transfer_queue_log)
        open_reqs = self.load_transfer_queue()
        entries = [
            {"op": "put", "req": r.model_dump(mode="json")} for r in open_reqs
        ]
        if len(entries) >= before:
            return 0
        try:
            _atomic_rewrite_jsonl(self.transfer_queue_log, entries)
        except OSError:
            logger.exception("transfer queue compaction failed")
            return 0
        return before - len(entries)

    def compact_decode_queue(self) -> int:
        """Rewrite pending_decodes.jsonl to hold only one `put` line per open
        (pass_id, decoder_index). Returns the number of lines dropped."""
        if not self.pending_decodes_log.exists():
            return 0
        before = _count_lines(self.pending_decodes_log)
        open_keys = self.load_decode_queue()
        entries = [
            {"op": "put", "pass_id": pid, "decoder_index": idx}
            for (pid, idx) in open_keys
        ]
        if len(entries) >= before:
            return 0
        try:
            _atomic_rewrite_jsonl(self.pending_decodes_log, entries)
        except OSError:
            logger.exception("decode queue compaction failed")
            return 0
        return before - len(entries)

    def compact_completed(self, keep_last: int = 200) -> int:
        """Truncate transfer_completed.jsonl to the last `keep_last` lines.
        Returns the number of lines dropped."""
        if not self.transfer_completed_log.exists():
            return 0
        entries = self._read_jsonl(self.transfer_completed_log)
        before = len(entries)
        if before <= keep_last:
            return 0
        kept = entries[-keep_last:]
        try:
            _atomic_rewrite_jsonl(self.transfer_completed_log, kept)
        except OSError:
            logger.exception("completed log compaction failed")
            return 0
        return before - len(kept)

    # ---------- helpers ----------

    def _append_log(self, path: Path, payload: Dict) -> None:
        try:
            _atomic_append_line(path, json.dumps(payload, default=_json_default))
        except OSError:
            logger.exception("state_write_failed: could not append to %s", path)

    def _read_jsonl(self, path: Path) -> List[Dict]:
        if not path.exists():
            return []
        out: List[Dict] = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for lineno, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.warning(
                            "truncated/invalid JSONL at %s:%d (%s) — stopping read",
                            path,
                            lineno,
                            e,
                        )
                        break
        except OSError:
            logger.exception("could not read %s", path)
        return out


def _json_default(obj: object) -> object:
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    raise TypeError(f"not JSON serializable: {type(obj)!r}")


def _count_lines(path: Path) -> int:
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    except OSError:
        return 0
