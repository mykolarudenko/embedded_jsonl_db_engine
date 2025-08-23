from __future__ import annotations
import io
import os
import json
from typing import Any, Dict, Iterator, Tuple
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
        self._lock_impl: str | None = None

    def open_exclusive(self, mode: str = "+") -> None:
        """
        Open file and acquire an exclusive lock (non-blocking).
        mode: "+" for write-intent, otherwise read-only lock (still EX for simplicity).
        """
        if self._fh is not None:
            return
        file_mode = "a+" if "+" in mode else "r"
        # Ensure parent dir exists
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        self._fh = open(self.path, file_mode, encoding="utf-8", newline="\n")
        try:
            try:
                import fcntl  # type: ignore
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._lock_impl = "fcntl"
            except Exception:
                # Try Windows msvcrt
                try:
                    import msvcrt  # type: ignore
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                    self._lock_impl = "msvcrt"
                except Exception as e:
                    try:
                        self._fh.close()
                    finally:
                        self._fh = None
                    raise LockError(f"failed to acquire exclusive lock: {e}") from e
        finally:
            if self._fh:
                # Normalize pointer to BOF for subsequent reads
                try:
                    self._fh.seek(0)
                except Exception:
                    pass

    def close(self) -> None:
        """
        Release lock and close file handle.
        """
        if not self._fh:
            return
        try:
            if self._lock_impl == "fcntl":
                import fcntl  # type: ignore
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            elif self._lock_impl == "msvcrt":
                import msvcrt  # type: ignore
                try:
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    # Best-effort unlock on Windows; ignore if file already closed/rotated
                    pass
        finally:
            try:
                self._fh.close()
            finally:
                self._fh = None
                self._lock_impl = None

    # ----- Header / schema / taxonomies -----

    def read_header_and_schema(self) -> Tuple[Dict, Dict, Dict]:
        """
        Return (header, schema_fields, taxonomies). Validates first 4 lines.
        """
        if not self._fh:
            raise IOCorruptionError("file is not open")
        self._fh.seek(0)
        lines = [self._fh.readline() for _ in range(4)]
        if any(l == "" for l in lines):
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

    def iter_meta_offsets(self) -> Iterator[Tuple[int, str]]:
        """
        Stream-scan file and yield (offset, meta_line_str) for each meta line.
        """
        # Read via separate binary handle to avoid disturbing main fh position
        if not os.path.exists(self.path):
            return
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
                if line.startswith(b'{"_t":"meta"'):
                    try:
                        yield offset, line.decode("utf-8")
                    except UnicodeDecodeError:
                        # Fallback replacement to avoid breaking scan
                        yield offset, line.decode("utf-8", errors="replace")

    def read_line_at(self, offset: int) -> str:
        """
        Read one line at absolute byte offset and return as str (utf-8).
        """
        with open(self.path, "rb") as fh:
            fh.seek(offset)
            line = fh.readline()
            return line.decode("utf-8")

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
