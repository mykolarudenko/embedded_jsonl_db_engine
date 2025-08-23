from __future__ import annotations
from typing import Any, Dict, Iterator, Tuple

HEADER_T = "header"
SCHEMA_T = "schema"
TAXO_T   = "taxonomies"
BEGIN_T  = "begin"
META_T   = "meta"

class FileStorage:
    """
    Low-level I/O scaffold. Implementation will be added in the next step.
    """
    def __init__(self, path: str) -> None:
        self.path = path

    def open_exclusive(self, mode: str = "+") -> None:
        raise NotImplementedError("Implement cross-platform exclusive lock and open file handle.")

    def close(self) -> None:
        pass

    def read_header_and_schema(self) -> Tuple[Dict, Dict, Dict]:
        raise NotImplementedError("Read first 4 lines and parse header/schema/taxonomies/begin.")

    def write_header_and_schema(self, header: Dict, schema: Dict, taxonomies: Dict) -> None:
        raise NotImplementedError("Write header/schema/taxonomies/begin lines into a temp file.")

    def append_meta_data(self, meta: Dict, data_str: str | None) -> Tuple[int, int | None]:
        raise NotImplementedError("Append meta+data, flush, maybe fsync. Return offsets.")

    def iter_meta_offsets(self) -> Iterator[Tuple[int, str]]:
        raise NotImplementedError("Stream scan of file to yield (offset, meta_line).")

    def read_line_at(self, offset: int) -> str:
        raise NotImplementedError("Seek to offset and readline().")

    def replace_file(self, tmp_path: str) -> None:
        raise NotImplementedError("Atomic os.replace and fsync directory.")
