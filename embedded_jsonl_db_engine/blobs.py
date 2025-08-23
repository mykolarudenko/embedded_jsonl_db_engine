from __future__ import annotations
import os
import io
import hashlib
from typing import BinaryIO, Dict, Tuple

class BlobManager:
    """
    External BLOBs (sha256 CAS) stored alongside DB: <basename>.blobs/sha256/ab/cdef...
    """
    def __init__(self, basepath: str) -> None:
        self.base = basepath

    def put_blob(self, stream: BinaryIO, mime: str, filename: str | None = None) -> Dict:
        """
        Read stream, compute sha256, write to a temp file and atomically move into the store.
        Return a dict ref: {"$blob":"sha256:<hex>", "size":..., "mime":..., "filename":...}
        """
        root = f"{self.base}.blobs/sha256"
        os.makedirs(root, exist_ok=True)
        tmp_name = f".tmp-{os.getpid()}-{os.urandom(8).hex()}"
        tmp_path = os.path.join(root, tmp_name)

        hasher = hashlib.sha256()
        size = 0

        # Ensure binary interface
        if isinstance(stream, io.TextIOBase):
            raise ValueError("stream must be binary")

        with open(tmp_path, "wb") as tmp:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, (bytes, bytearray)):
                    raise ValueError("stream must produce bytes")
                hasher.update(chunk)
                size += len(chunk)
                tmp.write(chunk)
            tmp.flush()
            try:
                os.fsync(tmp.fileno())
            except Exception:
                pass

        hex_digest = hasher.hexdigest()
        subdir = hex_digest[:2]
        leaf = hex_digest[2:]
        dest_dir = os.path.join(root, subdir)
        dest_path = os.path.join(dest_dir, leaf)
        os.makedirs(dest_dir, exist_ok=True)

        # If already exists, discard temp, otherwise move atomically
        if os.path.exists(dest_path):
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass
        else:
            os.replace(tmp_path, dest_path)

        return {
            "$blob": f"sha256:{hex_digest}",
            "size": size,
            "mime": mime,
            "filename": filename,
        }

    def open_blob(self, ref: Dict) -> BinaryIO:
        """
        Open blob by reference for reading.
        """
        token = ref.get("$blob") if isinstance(ref, dict) else None
        if not (isinstance(token, str) and token.startswith("sha256:")):
            raise ValueError("invalid blob ref")
        hex_digest = token.split("sha256:", 1)[1]
        if not hex_digest:
            raise ValueError("invalid blob ref")
        root = f"{self.base}.blobs/sha256"
        path = os.path.join(root, hex_digest[:2], hex_digest[2:])
        return open(path, "rb")

    def gc(self, used_hashes: set[str]) -> Tuple[int, int]:
        """
        Remove unused files. Return (files_removed, bytes_freed).
        """
        root = f"{self.base}.blobs/sha256"
        if not os.path.isdir(root):
            return (0, 0)
        removed = 0
        freed = 0
        for sub in os.listdir(root):
            subdir = os.path.join(root, sub)
            if not os.path.isdir(subdir):
                continue
            for leaf in os.listdir(subdir):
                path = os.path.join(subdir, leaf)
                if not os.path.isfile(path):
                    continue
                hex_digest = f"{sub}{leaf}"
                if hex_digest in used_hashes:
                    continue
                try:
                    stat = os.stat(path)
                    os.remove(path)
                    removed += 1
                    freed += stat.st_size
                except FileNotFoundError:
                    continue
                except Exception:
                    continue
        return (removed, freed)
