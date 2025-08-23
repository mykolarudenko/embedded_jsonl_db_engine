from __future__ import annotations
from typing import BinaryIO, Dict, Tuple

class BlobManager:
    """
    Внешние BLOB'ы (по sha256) рядом с БД: <basename>.blobs/sha256/ab/cdef...
    """
    def __init__(self, basepath: str) -> None:
        self.base = basepath

    def put_blob(self, stream: BinaryIO, mime: str, filename: str | None = None) -> Dict:
        """
        Читает stream, считает sha256, пишет во временный файл и атомарно перемещает в хранилище.
        Возвращает dict-реф: {"$blob":"sha256:<hex>", "size":..., "mime":..., "filename":...}
        """
        raise NotImplementedError("Implement blob streaming write with sha256 and atomic rename.")

    def open_blob(self, ref: Dict) -> BinaryIO:
        """
        Открывает blob по референсу на чтение.
        """
        raise NotImplementedError("Open blob from sha256 store and return a file-like.")

    def gc(self, used_hashes: set[str]) -> Tuple[int, int]:
        """
        Удаляет неиспользуемые файлы. Возвращает (files_removed, bytes_freed).
        """
        raise NotImplementedError("Walk blob store and remove orphaned files.")
