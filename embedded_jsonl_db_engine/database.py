from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
from .schema import Schema
from .taxonomy import TaxonomyAPI
from .index import InMemoryIndex
from .storage import FileStorage
from .progress import Progress
from .errors import ValidationError, ConflictError

class TDBRecord(dict):
    """
    dict-подобная запись, связанная с конкретной Database и id (после save()).
    Контролирует валидацию и отслеживает изменения.
    """
    __slots__ = ("_db", "_id", "_meta_offset", "_orig_hash", "_dirty_fields")

    def __init__(self, db: "Database", initial: Dict[str, Any]) -> None:
        super().__init__(initial)
        self._db = db
        self._id: Optional[str] = None
        self._meta_offset: Optional[int] = None
        self._orig_hash = repr(sorted(initial.items()))
        self._dirty_fields: set[str] = set()

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def dirty(self) -> bool:
        return repr(sorted(self.items())) != self._orig_hash

    @property
    def modified_fields(self) -> List[str]:
        return list(self._dirty_fields)

    def __setitem__(self, key: str, value: Any) -> None:
        self._db._validate_assign(key, value, self)
        super().__setitem__(key, value)
        self._dirty_fields.add(key)

    def save(self, force: bool = False) -> None:
        self._db._record_save(self, force=force)

    def reload(self) -> None:
        if not self._id:
            raise ValidationError("record has no id; save() it first")
        rec = self._db.get(self._id)
        if rec is None:
            raise ConflictError("record not found")
        super().clear()
        super().update(rec)
        self._orig_hash = repr(sorted(self.items()))
        self._dirty_fields.clear()

class Database:
    def __init__(
        self,
        path: str,
        schema: Dict[str, Any],
        mode: str = "+",
        on_progress = None,
        maintenance: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.path = path
        self._schema = Schema(schema)
        self._taxonomies: Dict[str, Any] = { }
        self._fs = FileStorage(path)
        self._progress = Progress(on_progress)
        self._index = InMemoryIndex()
        self._maintenance = maintenance or {}
        self._open(mode)

    def _open(self, mode: str) -> None:
        """
        Заглушка открытия. Реализация последует после FileStorage.
        """
        raise NotImplementedError("Implement open: lock, read header, scan meta, build indexes.")

    def new(self) -> TDBRecord:
        rec: Dict[str, Any] = {}
        self._schema.apply_defaults(rec)
        return TDBRecord(self, rec)

    def get(self, rec_id: str, *, include_meta: bool = False) -> TDBRecord | None:
        raise NotImplementedError("Implement get() using index offsets and json.loads of data line.")

    def find(
        self,
        query: Dict[str, Any],
        *,
        limit: Optional[int] = None,
        skip: int = 0,
        order_by: List[Tuple[str, str]] | None = None,
        fields: List[str] | None = None,
    ) -> Iterable[TDBRecord]:
        raise NotImplementedError("Implement find(): prefilter by in-mem indexes, then Fast/Full.")

    def update(self, query: Dict[str, Any], patch: Dict[str, Any]) -> int:
        n = 0
        for rec in self.find(query):
            self._deep_update(rec, patch)
            rec.save()
            n += 1
        return n

    def delete(self, query: Dict[str, Any]) -> int:
        raise NotImplementedError("Implement delete(): iterate candidates and append del meta.")

    def taxonomy(self, name: str) -> TaxonomyAPI:
        return TaxonomyAPI(self, name)

    def _taxonomy_header_update(self, name: str, *, op: str, key: str, attrs: Dict[str, Any]) -> None:
        raise NotImplementedError("Rewrite header with updated taxonomies, do rolling backup.")

    def _taxonomy_migrate(self, name: str, **kwargs: Any) -> None:
        raise NotImplementedError("Full-file migration for taxonomy changes with progress and backup.")

    def compact_now(self) -> None:
        raise NotImplementedError("Implement compact with progress and rolling/daily backups.")

    def backup_now(self, kind: str = "rolling") -> None:
        raise NotImplementedError("Implement rolling and daily backups with atomic replace.")

    def put_blob(self, stream_or_bytes, *, mime: str, filename: str | None = None) -> Dict[str, Any]:
        raise NotImplementedError("Delegate to BlobManager and return ref dict.")

    def open_blob(self, ref: Dict[str, Any]):
        raise NotImplementedError("Delegate to BlobManager and return file-like.")

    def gc_blobs(self) -> Dict[str, int]:
        raise NotImplementedError("GC orphaned blobs based on references in live records.")

    def _validate_assign(self, key: str, value: Any, rec: Dict[str, Any]) -> None:
        # Полная валидация будет в save(); здесь оставляем минимум.
        return

    def _record_save(self, rec: TDBRecord, *, force: bool) -> None:
        raise NotImplementedError("Implement record persistence with optimistic check by meta offset.")

    @staticmethod
    def _deep_update(rec: Dict[str, Any], patch: Dict[str, Any]) -> None:
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(rec.get(k), dict):
                Database._deep_update(rec[k], v)
            else:
                rec[k] = v
