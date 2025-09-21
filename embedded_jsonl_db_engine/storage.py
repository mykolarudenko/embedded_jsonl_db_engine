from __future__ import annotations
import io
import os
import json
from typing import Dict, Iterator, Tuple
import time
from .errors import LockError, IOCorruptionError

HEADER_T = "header"
SCHEMA_T = "schema"
TAXO_T   = "taxonomies"
BEGIN_T  = "begin"
META_T   = "meta"

class FileStorage:
    """
    Low-level I/O for JSONL DB: file lock, header R/W, append, scan, atomic replace.
    """
    def __init__(self, path: str) -> None:
        self.path = path
        self._fh: io.TextIOWrapper | None = None
        # Legacy field kept for backward compatibility; not used for process lock anymore
        self._lock_impl: str | None = None
        # Process-level locking state (uses separate .lock file)
        self._lock_fh: io.TextIOWrapper | None = None
        self._plock_impl: str | None = None
        self._lock_mode: str | None = None  # "read" | "write" | "maint"
        self._lock_depth: int = 0

    def open_exclusive(self, mode: str = "+") -> None:
        """
        Open the DB file handle. No process-level locking here.
        Process-level locks are managed per-operation via acquire_lock()/release_lock().
        """
        if self._fh is not None:
            return
        file_mode = "a+" if "+" in mode else "r"
        # Ensure parent dir exists
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        self._fh = open(self.path, file_mode, encoding="utf-8", newline="\n")
        # Normalize pointer to BOF for subsequent reads
        try:
            self._fh.seek(0)
        except Exception:
            pass

    def close(self) -> None:
        """
        Close DB file handle (process-level lock is managed separately).
        """
        if not self._fh:
            return
        try:
            self._fh.close()
        finally:
            self._fh = None
            self._lock_impl = None

    # ----- Process-level locking (per operation) -----

    def acquire_lock(self, kind: str, attempts: int, sleep_ms: int, allow_shared_read: bool = True) -> bool:
        """
        Acquire a process-level lock on a separate .lock file.
        kind: "read" (shared if supported), "write" (exclusive), "maint" (exclusive).
        Returns True on success, False if attempts exhausted.
        Nested acquisitions are reference-counted.
        """
        if self._lock_depth > 0:
            # Nested acquisition within the same process/thread
            self._lock_depth += 1
            return True

        lock_path = self.path + ".lock"
        # Ensure lock file exists
        parent = os.path.dirname(os.path.abspath(lock_path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        # Open lock file handle
        self._lock_fh = open(lock_path, "a+", encoding="utf-8", newline="\n")

        mode = "write" if kind in ("write", "maint") else "read"
        self._lock_mode = mode

        # Try POSIX flock shared/exclusive first
        last_err: Exception | None = None
        for _ in range(max(1, int(attempts))):
            try:
                try:
                    import fcntl  # type: ignore
                    flags = fcntl.LOCK_EX | fcntl.LOCK_NB
                    if mode == "read" and allow_shared_read:
                        flags = fcntl.LOCK_SH | fcntl.LOCK_NB
                    fcntl.flock(self._lock_fh.fileno(), flags)
                    self._plock_impl = "fcntl"
                    self._lock_depth = 1
                    return True
                except Exception as e_fcntl:
                    last_err = e_fcntl
                    # Try Windows fallback
                    try:
                        import msvcrt  # type: ignore
                        # msvcrt has no shared variant; lock 1 byte exclusively
                        msvcrt.locking(self._lock_fh.fileno(), msvcrt.LK_NBLCK, 1)
                        self._plock_impl = "msvcrt"
                        self._lock_depth = 1
                        return True
                    except Exception as e_win:
                        last_err = e_win
            except Exception as e:
                last_err = e
            time.sleep(max(0, int(sleep_ms)) / 1000.0)

        # Failed to acquire
        try:
            if self._lock_fh:
                self._lock_fh.close()
        finally:
            self._lock_fh = None
            self._plock_impl = None
            self._lock_mode = None
            self._lock_depth = 0
        return False

    def release_lock(self) -> None:
        """
        Release the process-level lock if held (supports nested acquisitions).
        """
        if self._lock_depth <= 0:
            return
        self._lock_depth -= 1
        if self._lock_depth > 0:
            return
        try:
            if self._plock_impl == "fcntl":
                import fcntl  # type: ignore
                fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)  # type: ignore[arg-type]
            elif self._plock_impl == "msvcrt":
                import msvcrt  # type: ignore
                try:
                    msvcrt.locking(self._lock_fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[arg-type]
                except OSError:
                    pass
        finally:
            try:
                if self._lock_fh:
                    self._lock_fh.close()
            finally:
                self._lock_fh = None
                self._plock_impl = None
                self._lock_mode = None
                self._lock_depth = 0

    # ----- Header / schema / taxonomies -----

    def read_header_and_schema(self) -> Tuple[Dict, Dict, Dict]:
        """
        Return (header, schema_fields, taxonomies). Validates first 4 lines.
        """
        if not self._fh:
            raise IOCorruptionError("file is not open")
        self._fh.seek(0)
        lines = [self._fh.readline() for _ in range(4)]
        if any(line == "" for line in lines):
            raise IOCorruptionError("incomplete header (expected 4 lines)")
        def parse_line(s: str, expected_t: str) -> Dict:
            try:
                obj = json.loads(s)
            except Exception as e:
                raise IOCorruptionError(f"invalid JSON in header: {e}") from e
            if obj.get("_t") != expected_t:
                raise IOCorruptionError(f"invalid header marker: expected '{expected_t}', got {obj.get('_t')!r}")
            return obj
        hdr = parse_line(lines[0], HEADER_T)
        sch = parse_line(lines[1], SCHEMA_T)
        txo = parse_line(lines[2], TAXO_T)
        _ = parse_line(lines[3], BEGIN_T)
        return hdr, sch.get("fields", {}), txo.get("items", {})

    def write_header_and_schema(self, header: Dict, schema: Dict, taxonomies: Dict) -> None:
        """
        Overwrite file with new header: header/schema/taxonomies/begin lines.
        """
        if not self._fh:
            raise IOCorruptionError("file is not open")
        self._fh.seek(0)
        self._fh.truncate(0)
        lines = [
            {"_t": HEADER_T, **header},
            {"_t": SCHEMA_T, "fields": schema},
            {"_t": TAXO_T, "items": taxonomies},
            {"_t": BEGIN_T},
        ]
        for obj in lines:
            s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
            self._fh.write(s + "\n")
        self._fh.flush()
        try:
            os.fsync(self._fh.fileno())
        except Exception:
            # Not all FS support fsync on text handles; ignore for now
            pass

    def rewrite_header(self, header: Dict, schema: Dict, taxonomies: Dict) -> None:
        """
        Rewrite only the 4-line header by creating a temporary file, copying data, and atomically replacing.
        """
        tmp_path = self.path + ".tmp"
        with open(self.path, "rb") as src, open(tmp_path, "wb") as dst:
            # Write new header lines
            for obj in ({"_t": HEADER_T, **header},
                        {"_t": SCHEMA_T, "fields": schema},
                        {"_t": TAXO_T, "items": taxonomies},
                        {"_t": BEGIN_T}):
                s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                dst.write(s + b"\n")
            # Skip 4 existing header lines
            for _ in range(4):
                if src.readline() == b"":
                    break
            # Copy remainder
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        self.replace_file(tmp_path)

    # ----- Append / read meta+data -----

    def append_meta_data(self, meta: Dict, data_str: str | None) -> Tuple[int, int | None]:
        """
        Append meta JSONL line (+data line if provided). Return (offset_meta, offset_data).
        """
        if not self._fh:
            raise IOCorruptionError("file is not open")
        # Seek to EOF for append
        self._fh.seek(0, os.SEEK_END)
        offset_meta = self._fh.tell()
        meta_line = json.dumps({"_t": META_T, **meta}, ensure_ascii=False, separators=(",", ":")) + "\n"
        self._fh.write(meta_line)
        offset_data: int | None = None
        if data_str is not None:
            if not data_str.endswith("\n"):
                data_str = data_str + "\n"
            offset_data = self._fh.tell()
            self._fh.write(data_str)
        self._fh.flush()
        try:
            os.fsync(self._fh.fileno())
        except Exception:
            pass
        return offset_meta, offset_data

    def iter_meta_offsets(self, attempts: int = 1, sleep_ms: int = 0) -> Iterator[Tuple[int, str]]:
        """
        Stream-scan file and yield (offset, meta_line_str) for each meta line.
        If the tail line is incomplete (no trailing newline), retry reading the tail
        up to `attempts` times with `sleep_ms` pauses to avoid truncated reads.
        A process-level read lock is held during the scan.
        """
        if not os.path.exists(self.path):
            return
        # Hold a read lock for the duration of the scan
        if not self.acquire_lock("read", attempts=max(1, attempts), sleep_ms=max(0, sleep_ms), allow_shared_read=True):
            # If lock cannot be acquired, just return (caller may retry)
            return
        try:
            with open(self.path, "rb") as fh:
                # Skip header (4 lines)
                for _ in range(4):
                    if not fh.readline():
                        return
                while True:
                    offset = fh.tell()
                    line = fh.readline()
                    if not line:
                        break
                    if not line.endswith(b"\n"):
                        # Tail might be incomplete; retry a few times
                        ok = False
                        for _ in range(max(1, attempts)):
                            time.sleep(max(0, sleep_ms) / 1000.0)
                            fh.seek(offset)
                            line = fh.readline()
                            if not line or line.endswith(b"\n"):
                                ok = True
                                break
                        if not ok:
                            # Give up on tail; stop iteration without error
                            break
                    if line.startswith(b'{"_t":"meta"'):
                        try:
                            yield offset, line.decode("utf-8")
                        except UnicodeDecodeError:
                            # Fallback replacement to avoid breaking scan
                            yield offset, line.decode("utf-8", errors="replace")
        finally:
            self.release_lock()

    def read_line_at(self, offset: int, attempts: int = 1, sleep_ms: int = 0) -> str:
        """
        Read one line at absolute byte offset and return as str (utf-8).
        If the line is incomplete (no trailing newline), retry a few times.
        A process-level read lock is acquired for the duration of the call.
        Returns empty string if the line could not be read completely.
        """
        if not self.acquire_lock("read", attempts=max(1, attempts), sleep_ms=max(0, sleep_ms), allow_shared_read=True):
            return ""
        try:
            for _ in range(max(1, attempts)):
                with open(self.path, "rb") as fh:
                    fh.seek(offset)
                    line = fh.readline()
                if not line:
                    time.sleep(max(0, sleep_ms) / 1000.0)
                    continue
                if line.endswith(b"\n"):
                    return line.decode("utf-8")
                time.sleep(max(0, sleep_ms) / 1000.0)
            return ""
        finally:
            self.release_lock()

    def replace_file(self, tmp_path: str) -> None:
        """
        Atomically replace the DB file with tmp_path and fsync directory.
        """
        os.replace(tmp_path, self.path)
        dirpath = os.path.dirname(os.path.abspath(self.path)) or "."
        dfd = os.open(dirpath, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
