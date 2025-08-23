from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, Set, Tuple

@dataclass
class MetaEntry:
    id: str
    offset_meta: int
    offset_data: Optional[int]
    deleted: bool
    ts_ms: int

class InMemoryIndex:
    """
    Базовый индекс по meta + вторичные индексы по скалярным полям + обратные индексы для taxonomy multi.
    """
    def __init__(self) -> None:
        self.meta: Dict[str, MetaEntry] = {}
        self.secondary: Dict[Tuple[str, str], Set[str]] = {}  # (path, value_str) -> ids
        self.reverse: Dict[Tuple[str, str], Set[str]] = {}    # (taxonomy_name, key) -> ids

    def add_meta(self, e: MetaEntry) -> None:
        self.meta[e.id] = e

    def add_secondary(self, path: str, value: str, rec_id: str) -> None:
        self.secondary.setdefault((path, value), set()).add(rec_id)

    def remove_secondary(self, path: str, value: str, rec_id: str) -> None:
        s = self.secondary.get((path, value))
        if s:
            s.discard(rec_id)
            if not s:
                self.secondary.pop((path, value), None)

    def add_reverse(self, taxo: str, key: str, rec_id: str) -> None:
        self.reverse.setdefault((taxo, key), set()).add(rec_id)

    def remove_reverse(self, taxo: str, key: str, rec_id: str) -> None:
        s = self.reverse.get((taxo, key))
        if s:
            s.discard(rec_id)
            if not s:
                self.reverse.pop((taxo, key), None)
