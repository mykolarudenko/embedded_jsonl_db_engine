from __future__ import annotations
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
        raise NotImplementedError("Implement blob streaming write with sha256 and atomic rename.")

    def open_blob(self, ref: Dict) -> BinaryIO:
        """
        Open blob by reference for reading.
        """
        raise NotImplementedError("Open blob from sha256 store and return a file-like.")

    def gc(self, used_hashes: set[str]) -> Tuple[int, int]:
        """
        Remove unused files. Return (files_removed, bytes_freed).
        """
        raise NotImplementedError("Walk blob store and remove orphaned files.")
