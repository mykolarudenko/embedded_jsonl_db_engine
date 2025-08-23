from __future__ import annotations
import os
import json
from typing import Any, Dict, Iterable, List, Optional, Tuple
from .schema import Schema
from .taxonomy import TaxonomyAPI
from .index import InMemoryIndex, MetaEntry
from .storage import FileStorage
from .progress import Progress
from .utils import now_iso, canonical_json, sha256_hex, new_ulid, iso_to_epoch_ms
from .errors import ValidationError, ConflictError

class TDBRecord(dict):
    """
    Dict-like record bound to a specific Database and id (after save()).
    Handles validation and tracks changes.
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
        Open DB file: acquire lock, ensure header exists, scan meta to build in-memory index.
        """
        # Acquire lock/open
        self._fs.open_exclusive(mode)

        # Ensure header exists; if not, initialize a new header using provided schema
        try:
            _hdr, _schema_fields, taxonomies = self._fs.read_header_and_schema()
            # Keep taxonomies from file (schema migration is out of scope here)
            self._taxonomies = taxonomies or {}
        except Exception:
            # Initialize a new file
            hdr = {
                "format": "ejl1",
                "table": os.path.splitext(os.path.basename(self.path))[0],
                "created": now_iso(),
                "defaults_always_materialized": True,
            }
            self._taxonomies = {}
            self._fs.write_header_and_schema(hdr, self._schema._fields, self._taxonomies)

        # Rebuild in-memory index from meta stream
        self._index = InMemoryIndex()
        for offset, line in self._fs.iter_meta_offsets():
            try:
                meta = json.loads(line)
            except Exception:
                continue
            if meta.get("_t") != "meta":
                continue
            rec_id = meta.get("id")
            if not rec_id:
                continue
            op = meta.get("op")
            ts_iso = meta.get("ts") or now_iso()
            ts_ms = iso_to_epoch_ms(ts_iso)
            offset_data = None
            if op == "put":
                # Data line immediately follows meta line
                offset_data = offset + len(line.encode("utf-8"))
            entry = MetaEntry(
                id=rec_id,
                offset_meta=offset,
                offset_data=offset_data if op == "put" else None,
                deleted=(op == "del"),
                ts_ms=ts_ms,
            )
            self._index.add_meta(entry)

    def new(self) -> TDBRecord:
        rec: Dict[str, Any] = {}
        self._schema.apply_defaults(rec)
        return TDBRecord(self, rec)

    def get(self, rec_id: str, *, include_meta: bool = False) -> TDBRecord | None:
        entry = self._index.meta.get(rec_id)
        if not entry or entry.deleted or entry.offset_data is None:
            return None
        line = self._fs.read_line_at(entry.offset_data)
        try:
            obj = json.loads(line)
        except Exception:
            return None
        rec = TDBRecord(self, obj)
        rec._id = rec_id
        rec._meta_offset = entry.offset_meta
        rec._orig_hash = repr(sorted(rec.items()))
        rec._dirty_fields.clear()
        return rec

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
        # Full validation will run in save(); keep minimal checks here.
        return

    def _record_save(self, rec: TDBRecord, *, force: bool) -> None:
        # Assign id/createdAt on new records
        if rec._id is None:
            rec._id = new_ulid()
            if "id" not in rec:
                rec["id"] = rec._id
            if "createdAt" not in rec:
                rec["createdAt"] = now_iso()

        if not force and not rec.dirty:
            return

        # Full validation
        self._schema.validate(rec)

        # Serialize and compute meta
        data_str = canonical_json(dict(rec))
        data_bytes = data_str.encode("utf-8")
        ts_iso = now_iso()
        meta = {
            "id": rec._id,
            "op": "put",
            "ts": ts_iso,
            "len_data": len(data_bytes),
            "sha256_data": sha256_hex(data_bytes),
        }

        # Append and get offsets
        off_meta, off_data = self._fs.append_meta_data(meta, data_str)

        # Update index
        entry = MetaEntry(
            id=rec._id,
            offset_meta=off_meta,
            offset_data=off_data,
            deleted=False,
            ts_ms=iso_to_epoch_ms(ts_iso),
        )
        self._index.add_meta(entry)

        # Sync state
        rec._meta_offset = off_meta
        rec._orig_hash = repr(sorted(rec.items()))
        rec._dirty_fields.clear()

    @staticmethod
    def _deep_update(rec: Dict[str, Any], patch: Dict[str, Any]) -> None:
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(rec.get(k), dict):
                Database._deep_update(rec[k], v)
            else:
                rec[k] = v
