from __future__ import annotations
import os
import json
import io
import gzip
import shutil
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set
from .schema import Schema
from .taxonomy import TaxonomyAPI
from .index import InMemoryIndex, MetaEntry
from .storage import FileStorage
from .blobs import BlobManager
from .progress import Progress
from .utils import now_iso, canonical_json, sha256_hex, new_ulid, iso_to_epoch_ms
from .errors import ValidationError, ConflictError, IOCorruptionError

# Scalar types used for building secondary indexes
_SCALAR_TYPES = {"str", "int", "float", "bool", "datetime"}

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
        self._orig_hash = self._hash_data()
        self._dirty_fields: set[str] = set()

    def _hash_data(self) -> str:
        # Canonical JSON string is enough for dirty detection (sha256 not required here)
        return canonical_json(self)

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def dirty(self) -> bool:
        return self._hash_data() != self._orig_hash

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
        self._orig_hash = self._hash_data()
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
        self._header: Dict[str, Any] = {}
        # Precompute index specs from schema hints
        self._sec_paths: List[str] = []
        self._rev_list_paths: List[Tuple[str, str]] = []
        self._rev_single_paths: List[Tuple[str, str]] = []
        self._rev_map: Dict[str, str] = {}
        self._compute_index_specs()
        self._open(mode)

    def _open(self, mode: str) -> None:
        """
        Open DB file: acquire lock, ensure header exists, scan meta to build in-memory index.
        """
        # Acquire lock/open
        self._fs.open_exclusive(mode)
        self._progress.emit("open.start", 0, path=self.path)

        # Ensure header exists; if not, initialize a new header using provided schema
        try:
            _hdr, _schema_fields, taxonomies = self._fs.read_header_and_schema()
            # Keep taxonomies from file (schema migration is out of scope here)
            self._header = _hdr
            self._taxonomies = taxonomies or {}
        except IOCorruptionError:
            # Initialize a new file
            hdr = {
                "format": "ejl1",
                "table": os.path.splitext(os.path.basename(self.path))[0],
                "created": now_iso(),
                "defaults_always_materialized": True,
            }
            self._header = hdr
            self._taxonomies = {}
            self._fs.write_header_and_schema(hdr, self._schema._fields, self._taxonomies)

        # Rebuild in-memory index from meta stream
        self._index = InMemoryIndex()
        meta_lines = list(self._fs.iter_meta_offsets())
        total = len(meta_lines)
        self._progress.emit("open.scan_meta", 0, total=total)
        for i, (offset, line) in enumerate(meta_lines, 1):
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
            self._progress.emit("open.scan_meta", int(i * 100 / max(1, total)), scanned=i, total=total)

        # Build secondary & reverse indexes from live records
        self._progress.emit("open.build_indexes", 0, total=len(self._index.meta))
        built = 0
        for rid, ent in self._index.meta.items():
            if ent.deleted or ent.offset_data is None:
                continue
            try:
                obj_line = self._fs.read_line_at(ent.offset_data)
                obj = json.loads(obj_line)
            except Exception:
                continue
            self._index_add_from_obj(rid, obj)
            built += 1
            if built % 100 == 0:
                self._progress.emit("open.build_indexes", int(built * 100 / max(1, len(self._index.meta))), built=built)
        self._progress.emit("open.done", 100, msg="Open complete")

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
        # Optional integrity check against meta
        try:
            meta_line = self._fs.read_line_at(entry.offset_meta)
            meta_obj = json.loads(meta_line)
            data_bytes = line.encode("utf-8")
            if "len_data" in meta_obj and meta_obj["len_data"] != len(data_bytes):
                raise IOCorruptionError("data length mismatch at read")
            if "sha256_data" in meta_obj:
                if meta_obj["sha256_data"] != sha256_hex(data_bytes):
                    raise IOCorruptionError("data hash mismatch at read")
        except Exception:
            # Do not fail hard on meta read/parse; only strict mismatch raises above
            pass
        rec = TDBRecord(self, obj)
        rec._id = rec_id
        rec._meta_offset = entry.offset_meta
        rec._orig_hash = rec._hash_data()
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
        # Simple full-scan plan with basic predicate evaluation.
        # Supports:
        # - equality on scalars
        # - nested dicts like {"address": {"city": "Wien"}}
        # - simple ops: $eq/$ne/$gt/$gte/$lt/$lte
        # - $contains for list[str] or substring for str
        def is_op_key(k: str) -> bool:
            return k in ("$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$contains")

        def match_obj(obj: Dict[str, Any], q: Dict[str, Any]) -> bool:
            for k, v in q.items():
                if k.startswith("$"):
                    return False  # unsupported top-level operators
                if isinstance(v, dict) and any(is_op_key(op) for op in v.keys()):
                    val = obj.get(k)
                    for op, arg in v.items():
                        if op == "$eq":
                            if val != arg:
                                return False
                        elif op == "$ne":
                            if val == arg:
                                return False
                        elif op == "$gt":
                            try:
                                if not (val > arg):
                                    return False
                            except Exception:
                                return False
                        elif op == "$gte":
                            try:
                                if not (val >= arg):
                                    return False
                            except Exception:
                                return False
                        elif op == "$lt":
                            try:
                                if not (val < arg):
                                    return False
                            except Exception:
                                return False
                        elif op == "$lte":
                            try:
                                if not (val <= arg):
                                    return False
                            except Exception:
                                return False
                        elif op == "$contains":
                            if isinstance(val, list):
                                if arg not in val:
                                    return False
                            elif isinstance(val, str):
                                if str(arg) not in val:
                                    return False
                            else:
                                return False
                        else:
                            return False
                elif isinstance(v, dict):
                    sub = obj.get(k)
                    if not isinstance(sub, dict):
                        return False
                    if not match_obj(sub, v):
                        return False
                else:
                    if obj.get(k) != v:
                        return False
            return True

        # Prefilter with in-memory indexes where possible
        cand_ids = self._prefilter_ids(query)
        if cand_ids is None:
            items_iter = self._index.meta.items()
        else:
            items_iter = ((rid, self._index.meta.get(rid)) for rid in cand_ids)

        recs: List[TDBRecord] = []
        for rec_id, entry in items_iter:
            if not entry or entry.deleted or entry.offset_data is None:
                continue
            line = self._fs.read_line_at(entry.offset_data)
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if not match_obj(obj, query):
                continue
            rec = TDBRecord(self, obj)
            rec._id = rec_id
            rec._meta_offset = entry.offset_meta
            rec._orig_hash = rec._hash_data()
            rec._dirty_fields.clear()
            recs.append(rec)

        # Sorting
        if order_by:
            def norm(v):
                if v is None:
                    return ("", "")
                if isinstance(v, (int, float, bool)):
                    return ("0", str(v))
                if isinstance(v, str):
                    return ("1", v)
                try:
                    return ("2", json.dumps(v, sort_keys=True, ensure_ascii=False))
                except Exception:
                    return ("2", str(v))
            for field, direction in reversed(order_by):
                reverse = (str(direction).lower() == "desc")
                recs.sort(key=lambda r: norm(r.get(field)), reverse=reverse)

        # Skip / limit
        start = max(0, int(skip)) if isinstance(skip, int) else 0
        if limit is None:
            selected = recs[start:]
        else:
            selected = recs[start:start + int(limit)]
        for r in selected:
            if fields:
                field_set = set(fields)
                field_set.add("id")
                obj = {k: r.get(k) for k in field_set if k in r}
                r2 = TDBRecord(self, obj)
                r2._id = r.id
                r2._meta_offset = r._meta_offset
                r2._orig_hash = r2._hash_data()
                r2._dirty_fields.clear()
                yield r2
            else:
                yield r

    def update(self, query: Dict[str, Any], patch: Dict[str, Any]) -> int:
        n = 0
        for rec in self.find(query):
            self._deep_update(rec, patch)
            rec.save()
            n += 1
        return n

    def delete(self, query: Dict[str, Any]) -> int:
        """
        Logical deletion: append meta(op:"del") for matched records and update index.
        """
        n = 0
        for rec in self.find(query):
            if not rec._id:
                continue
            # Remove record from secondary/reverse indexes before marking deleted
            self._index_remove_from_obj(rec._id, rec)
            ts_iso = now_iso()
            meta = {"id": rec._id, "op": "del", "ts": ts_iso}
            off_meta, _ = self._fs.append_meta_data(meta, None)
            entry = MetaEntry(
                id=rec._id,
                offset_meta=off_meta,
                offset_data=None,
                deleted=True,
                ts_ms=iso_to_epoch_ms(ts_iso),
            )
            self._index.add_meta(entry)
            n += 1
        return n

    # ----- Index helpers -----

    def _prefilter_ids(self, query: Dict[str, Any]) -> Optional[Set[str]]:
        """
        Use in-memory indexes to preselect candidate ids.
        Supports:
          - equality on scalar indexed paths
          - equality on single-taxonomy string paths
          - $contains on list[str] taxonomy paths
        Returns:
          - set of ids if at least one indexable predicate found
          - None otherwise (caller should full-scan)
        """
        terms: List[Tuple[str, str, Any]] = []

        def walk(obj: Dict[str, Any], base: Tuple[str, ...]) -> None:
            for k, v in obj.items():
                if k.startswith("$"):
                    continue
                new_base = base + (k,)
                if isinstance(v, dict):
                    ops = [op for op in v.keys() if isinstance(op, str) and op.startswith("$")]
                    if ops:
                        if "$eq" in v:
                            terms.append(("/".join(new_base), "$eq", v["$eq"]))
                        if "$contains" in v:
                            terms.append(("/".join(new_base), "$contains", v["$contains"]))
                    else:
                        walk(v, new_base)
                else:
                    terms.append(("/".join(new_base), "$eq", v))

        walk(query, ())

        candidate_ids: Optional[Set[str]] = None

        for path, op, arg in terms:
            ids: Optional[Set[str]] = None
            if op == "$eq":
                if path in self._sec_paths:
                    key = self._canonicalize_value(arg)
                    ids = set(self._index.secondary.get((path, key), set()))
                elif path in self._rev_map:
                    taxo = self._rev_map[path]
                    ids = set(self._index.reverse.get((taxo, str(arg)), set()))
            elif op == "$contains":
                if path in self._rev_map:
                    taxo = self._rev_map[path]
                    ids = set(self._index.reverse.get((taxo, str(arg)), set()))
            if ids is not None:
                candidate_ids = ids if candidate_ids is None else (candidate_ids & ids)
                if candidate_ids is not None and len(candidate_ids) == 0:
                    break

        return candidate_ids

    def _compute_index_specs(self) -> None:
        # Build lists of paths for secondary and reverse indexes based on schema hints
        self._sec_paths.clear()
        self._rev_list_paths.clear()
        self._rev_single_paths.clear()
        self._rev_map.clear()
        flat = getattr(self._schema, "_flat", {})
        for path_tuple, fspec in flat.items():
            path = "/".join(path_tuple)
            t = getattr(fspec, "type", None)
            if not path or not t:
                continue
            if t in _SCALAR_TYPES and getattr(fspec, "index", False):
                self._sec_paths.append(path)
            if t == "list" and getattr(fspec, "index_membership", False) and getattr(fspec, "taxonomy", None):
                taxo = getattr(fspec, "taxonomy")
                self._rev_list_paths.append((path, taxo))
                self._rev_map[path] = taxo
            if t in ("str",) and getattr(fspec, "taxonomy", None) and getattr(fspec, "taxonomy_mode", None) == "single":
                taxo = getattr(fspec, "taxonomy")
                self._rev_single_paths.append((path, taxo))
                self._rev_map[path] = taxo

    def _extract_at_path(self, obj: Dict[str, Any], path: str):
        cur: Any = obj
        for key in (p for p in path.split("/") if p):
            if not isinstance(cur, dict) or key not in cur:
                return None
            cur = cur[key]
        return cur

    def _canonicalize_value(self, v: Any) -> str:
        return canonical_json(v)

    def _index_add_from_obj(self, rec_id: str, obj: Dict[str, Any]) -> None:
        # Secondary scalar indexes
        for path in self._sec_paths:
            v = self._extract_at_path(obj, path)
            if isinstance(v, (str, int, float, bool)):
                self._index.add_secondary(path, self._canonicalize_value(v), rec_id)
        # Reverse taxonomy for list
        for path, taxo in self._rev_list_paths:
            lst = self._extract_at_path(obj, path)
            if isinstance(lst, list):
                for item in lst:
                    if isinstance(item, str):
                        self._index.add_reverse(taxo, item, rec_id)
        # Reverse taxonomy for single scalar
        for path, taxo in self._rev_single_paths:
            val = self._extract_at_path(obj, path)
            if isinstance(val, str):
                self._index.add_reverse(taxo, val, rec_id)

    def _index_remove_from_obj(self, rec_id: str, obj: Dict[str, Any]) -> None:
        # Secondary scalar indexes
        for path in self._sec_paths:
            v = self._extract_at_path(obj, path)
            if isinstance(v, (str, int, float, bool)):
                self._index.remove_secondary(path, self._canonicalize_value(v), rec_id)
        # Reverse taxonomy for list
        for path, taxo in self._rev_list_paths:
            lst = self._extract_at_path(obj, path)
            if isinstance(lst, list):
                for item in lst:
                    if isinstance(item, str):
                        self._index.remove_reverse(taxo, item, rec_id)
        # Reverse taxonomy for single scalar
        for path, taxo in self._rev_single_paths:
            val = self._extract_at_path(obj, path)
            if isinstance(val, str):
                self._index.remove_reverse(taxo, val, rec_id)

    def _set_at_path(self, obj: Dict[str, Any], path: str, value: Any) -> bool:
        cur: Any = obj
        parts = [p for p in path.split("/") if p]
        if not parts:
            return False
        for key in parts[:-1]:
            if not isinstance(cur, dict) or key not in cur:
                return False
            cur = cur[key]
        if not isinstance(cur, dict):
            return False
        cur[parts[-1]] = value
        return True

    def _delete_at_path(self, obj: Dict[str, Any], path: str) -> bool:
        cur: Any = obj
        parts = [p for p in path.split("/") if p]
        if not parts:
            return False
        for key in parts[:-1]:
            if not isinstance(cur, dict) or key not in cur:
                return False
            cur = cur[key]
        if isinstance(cur, dict) and parts[-1] in cur:
            del cur[parts[-1]]
            return True
        return False

    def _transform_taxonomy_in_obj(self, obj: Dict[str, Any], *, list_paths: List[str], scalar_paths: List[str], mapping: Dict[str, Optional[str]]) -> None:
        # Transform list[str] taxonomy memberships
        for path in list_paths:
            v = self._extract_at_path(obj, path)
            if isinstance(v, list):
                new_list: List[str] = []
                seen = set()
                changed = False
                for item in v:
                    if isinstance(item, str):
                        if item in mapping:
                            new_item = mapping[item]
                            changed = True
                            if new_item is None:
                                continue
                            item = new_item
                    if isinstance(item, str) and item not in seen:
                        seen.add(item)
                        new_list.append(item)
                if changed:
                    self._set_at_path(obj, path, new_list)
        # Transform single string taxonomy refs
        for path in scalar_paths:
            v = self._extract_at_path(obj, path)
            if isinstance(v, str) and v in mapping:
                new_val = mapping[v]
                if new_val is None:
                    self._delete_at_path(obj, path)
                else:
                    self._set_at_path(obj, path, new_val)

    def taxonomy(self, name: str) -> TaxonomyAPI:
        return TaxonomyAPI(self, name)

    def _taxonomy_header_update(self, name: str, *, op: str, key: str, attrs: Dict[str, Any]) -> None:
        """
        Update taxonomy metadata in header only (no data migration). Rewrites header and rebuilds indexes.
        Supported ops: "upsert", "set_attrs".
        """
        if not name or not isinstance(name, str):
            raise ValidationError("taxonomy name must be non-empty string")
        if not key or not isinstance(key, str):
            raise ValidationError("taxonomy key must be non-empty string")
        taxo = self._taxonomies.setdefault(name, {"list": []})
        items = taxo.get("list")
        if not isinstance(items, list):
            items = []
            taxo["list"] = items

        idx = None
        for i, item in enumerate(items):
            if isinstance(item, dict) and item.get("key") == key:
                idx = i
                break

        if op == "upsert":
            if idx is not None:
                # merge attrs into existing
                cur = dict(items[idx])
                cur.update(attrs or {})
                cur["key"] = key
                items[idx] = cur
            else:
                new_item = {"key": key}
                if attrs:
                    new_item.update(attrs)
                items.append(new_item)
        elif op == "set_attrs":
            if idx is None:
                raise ValidationError("taxonomy key not found for set_attrs")
            cur = dict(items[idx])
            cur.update(attrs or {})
            cur["key"] = key
            items[idx] = cur
        else:
            raise ValidationError(f"unsupported taxonomy header op: {op!r}")

        # Rewrite header safely (close handle to avoid appending to unlinked inode)
        self._fs.close()
        self._fs.rewrite_header(self._header, self._schema._fields, self._taxonomies)
        # Reopen and rebuild indexes (offsets changed after header rewrite)
        self._open("+")

    def _taxonomy_migrate(self, name: str, **kwargs: Any) -> None:
        """
        Perform full-file rewrite to apply taxonomy key changes across records.
        Supported actions:
          - action="rename", old_key, new_key, collision="merge"
          - action="merge", source_keys: List[str], target_key: str
          - action="delete", key: str, strategy="detach"
        """
        action = kwargs.get("action")
        if action not in ("rename", "merge", "delete"):
            raise ValidationError("unsupported taxonomy migration action")

        taxo = self._taxonomies.setdefault(name, {"list": []})
        items = taxo.get("list") or []
        if not isinstance(items, list):
            items = []
        items_by_key = {item.get("key"): dict(item) for item in items if isinstance(item, dict) and "key" in item}

        mapping: Dict[str, Optional[str]] = {}

        if action == "rename":
            old_key = kwargs.get("old_key")
            new_key = kwargs.get("new_key")
            collision = kwargs.get("collision", "merge")
            if not old_key or not new_key:
                raise ValidationError("rename requires old_key and new_key")
            mapping[old_key] = new_key
            if collision != "merge" and new_key in items_by_key and old_key in items_by_key:
                raise ValidationError("rename collision not supported other than 'merge'")
            if old_key in items_by_key:
                src = items_by_key.pop(old_key)
                if new_key not in items_by_key:
                    src["key"] = new_key
                    items_by_key[new_key] = src

        elif action == "merge":
            source_keys = kwargs.get("source_keys") or []
            target_key = kwargs.get("target_key")
            if not isinstance(source_keys, list) or not target_key:
                raise ValidationError("merge requires source_keys list and target_key")
            for sk in source_keys:
                if sk == target_key:
                    continue
                mapping[sk] = target_key
            if target_key not in items_by_key:
                items_by_key[target_key] = {"key": target_key}
            for sk in source_keys:
                items_by_key.pop(sk, None)

        elif action == "delete":
            key = kwargs.get("key")
            strategy = kwargs.get("strategy", "detach")
            if not key:
                raise ValidationError("delete requires key")
            if strategy != "detach":
                raise ValidationError("only 'detach' delete strategy is supported")
            mapping[key] = None
            items_by_key.pop(key, None)

        # Update in-memory taxonomies and write migrated file
        new_items = list(items_by_key.values())
        self._taxonomies[name] = {"list": new_items}

        # Determine schema paths referencing this taxonomy
        list_paths = [p for (p, t) in self._rev_list_paths if t == name]
        scalar_paths = [p for (p, t) in self._rev_single_paths if t == name]

        # Close handle, then rewrite file
        self._fs.close()

        tmp_path = f"{self.path}.migrate.tmp"
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as dst:
            # Write header with updated taxonomies
            header_lines = [
                {"_t": "header", **self._header},
                {"_t": "schema", "fields": self._schema._fields},
                {"_t": "taxonomies", "items": self._taxonomies},
                {"_t": "begin"},
            ]
            for obj in header_lines:
                dst.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")

            # Copy and transform live records by ts order
            live_entries = [e for e in self._index.meta.values() if (not e.deleted) and (e.offset_data is not None)]
            live_entries.sort(key=lambda e: e.ts_ms)
            total = len(live_entries)
            for i, e in enumerate(live_entries, 1):
                line = self._fs.read_line_at(e.offset_data)
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                self._transform_taxonomy_in_obj(obj, list_paths=list_paths, scalar_paths=scalar_paths, mapping=mapping)
                data_str = canonical_json(obj)
                data_bytes = data_str.encode("utf-8")
                ts_iso = now_iso()
                meta_obj = {
                    "_t": "meta",
                    "id": e.id,
                    "op": "put",
                    "ts": ts_iso,
                    "len_data": len(data_bytes),
                    "sha256_data": sha256_hex(data_bytes),
                }
                dst.write(json.dumps(meta_obj, ensure_ascii=False, separators=(",", ":")) + "\n")
                dst.write(data_str + "\n")
                self._progress.emit("taxonomy.migrate", int(i * 100 / max(1, total)), key=name, action=action)

            dst.flush()
            try:
                os.fsync(dst.fileno())
            except Exception:
                pass

        self._fs.replace_file(tmp_path)
        self._open("+")

    def compact_now(self) -> None:
        """
        Rewrite file to remove garbage records based on current in-memory index.
        Runs only if garbage_ratio >= 0.30.
        """
        # Compute garbage ratio as (total_meta - live_count) / total_meta
        total_meta = 0
        for _ in self._fs.iter_meta_offsets():
            total_meta += 1
        live_entries = [e for e in self._index.meta.values() if (not e.deleted) and (e.offset_data is not None)]
        live_count = len(live_entries)
        if total_meta <= 0:
            return
        garbage_ratio = (total_meta - live_count) / max(1, total_meta)
        if garbage_ratio < 0.30:
            return

        self._progress.emit("compact.start", 0, msg="Starting compaction", total_meta=total_meta, live=live_count)

        # Close file handle to ensure stable rename on replace
        self._fs.close()

        tmp_path = f"{self.path}.compact.tmp"
        with open(tmp_path, "w", encoding="utf-8", newline="\n") as dst:
            # Write header
            lines = [
                {"_t": "header", **self._header},
                {"_t": "schema", "fields": self._schema._fields},
                {"_t": "taxonomies", "items": self._taxonomies},
                {"_t": "begin"},
            ]
            for obj in lines:
                s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
                dst.write(s + "\n")

            # Copy live records in ts order
            live_entries_sorted = sorted(live_entries, key=lambda e: e.ts_ms)
            total = len(live_entries_sorted)
            for i, e in enumerate(live_entries_sorted, 1):
                line = self._fs.read_line_at(e.offset_data)
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                data_str = canonical_json(obj)
                data_bytes = data_str.encode("utf-8")
                ts_iso = now_iso()
                meta_obj = {
                    "_t": "meta",
                    "id": e.id,
                    "op": "put",
                    "ts": ts_iso,
                    "len_data": len(data_bytes),
                    "sha256_data": sha256_hex(data_bytes),
                }
                dst.write(json.dumps(meta_obj, ensure_ascii=False, separators=(",", ":")) + "\n")
                dst.write(data_str + "\n")
                self._progress.emit("compact.copy", int(i * 100 / max(1, total)), copied=i, total=total)

            dst.flush()
            try:
                os.fsync(dst.fileno())
            except Exception:
                pass

        # Atomically replace and reopen
        self._fs.replace_file(tmp_path)
        self._open("+")

    def backup_now(self, kind: str = "rolling") -> None:
        """
        Create backups: rolling (.bak.N) or daily gz snapshot.
        """
        backup_conf = self._maintenance.get("backup", {}) if isinstance(self._maintenance, dict) else {}
        keep = int(backup_conf.get("rolling_keep", 3))
        daily_dirname = str(backup_conf.get("daily_dir", "daily"))

        root_dir = os.path.join(os.path.dirname(os.path.abspath(self.path)), "embedded_jsonl_db_backup")
        os.makedirs(root_dir, exist_ok=True)

        # Ensure data is flushed by closing handle temporarily
        self._fs.close()

        base = os.path.basename(self.path)

        if kind == "rolling":
            self._progress.emit("backup.rolling", 0, msg="Rolling backup")
            # Rotate .bak.N files
            last_path = os.path.join(root_dir, f"{base}.bak.{keep}")
            if os.path.exists(last_path):
                try:
                    os.remove(last_path)
                except Exception:
                    pass
            for i in range(keep, 1, -1):
                src = os.path.join(root_dir, f"{base}.bak.{i-1}")
                dst = os.path.join(root_dir, f"{base}.bak.{i}")
                if os.path.exists(src):
                    try:
                        os.replace(src, dst)
                    except Exception:
                        try:
                            shutil.copy2(src, dst)
                            os.remove(src)
                        except Exception:
                            pass
            dest1 = os.path.join(root_dir, f"{base}.bak.1")
            with open(self.path, "rb") as src_f, open(dest1, "wb") as dst_f:
                shutil.copyfileobj(src_f, dst_f, length=1024 * 1024)
                dst_f.flush()
                try:
                    os.fsync(dst_f.fileno())
                except Exception:
                    pass
            self._progress.emit("backup.rolling", 100, msg="Rolling backup complete", path=dest1)

        elif kind == "daily":
            self._progress.emit("backup.daily", 0, msg="Daily backup")
            daily_dir = os.path.join(root_dir, daily_dirname)
            os.makedirs(daily_dir, exist_ok=True)
            date_str = now_iso().split("T", 1)[0]
            dest = os.path.join(daily_dir, f"{base}.{date_str}.jsonl.gz")
            with open(self.path, "rb") as src_f, gzip.open(dest, "wb") as gz:
                shutil.copyfileobj(src_f, gz, length=1024 * 1024)
                try:
                    gz.flush()
                except Exception:
                    pass
            self._progress.emit("backup.daily", 100, msg="Daily backup complete", path=dest)
        else:
            raise ValidationError(f"Unknown backup kind: {kind!r}")

        # Reacquire exclusive lock without full reopen
        self._fs.open_exclusive("+")

    def put_blob(self, stream_or_bytes, *, mime: str, filename: str | None = None) -> Dict[str, Any]:
        """
        Store a BLOB in external CAS and return a reference dict.
        Accepts bytes or a binary file-like object.
        """
        mgr = BlobManager(self.path)
        if isinstance(stream_or_bytes, (bytes, bytearray)):
            bio = io.BytesIO(stream_or_bytes)
            return mgr.put_blob(bio, mime, filename)
        if hasattr(stream_or_bytes, "read"):
            return mgr.put_blob(stream_or_bytes, mime, filename)
        raise ValidationError("put_blob expects bytes or a binary stream")

    def open_blob(self, ref: Dict[str, Any]):
        """
        Open blob by reference for reading.
        """
        mgr = BlobManager(self.path)
        return mgr.open_blob(ref)

    def gc_blobs(self) -> Dict[str, int]:
        """
        Garbage-collect orphaned blobs based on references in live records.
        """
        # Collect referenced hashes from all live records
        def collect(obj, out: Set[str]) -> None:
            if isinstance(obj, dict):
                if "$blob" in obj and isinstance(obj.get("$blob"), str) and obj["$blob"].startswith("sha256:"):
                    out.add(obj["$blob"].split("sha256:", 1)[1])
                for v in obj.values():
                    collect(v, out)
            elif isinstance(obj, list):
                for it in obj:
                    collect(it, out)

        used: Set[str] = set()
        for e in self._index.meta.values():
            if e.deleted or e.offset_data is None:
                continue
            try:
                line = self._fs.read_line_at(e.offset_data)
                obj = json.loads(line)
            except Exception:
                continue
            collect(obj, used)

        mgr = BlobManager(self.path)
        removed, freed = mgr.gc(used)
        return {"files_removed": removed, "bytes_freed": freed}

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

        # Remove old index entries if any
        old_entry = self._index.meta.get(rec._id) if rec._id else None
        if old_entry and not old_entry.deleted and old_entry.offset_data is not None:
            try:
                old_line = self._fs.read_line_at(old_entry.offset_data)
                old_obj = json.loads(old_line)
                self._index_remove_from_obj(rec._id, old_obj)
            except Exception:
                pass

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

        # Update secondary/reverse indexes for new content
        self._index_add_from_obj(rec._id, rec)

        # Sync state
        rec._meta_offset = off_meta
        rec._orig_hash = rec._hash_data()
        rec._dirty_fields.clear()

    @staticmethod
    def _deep_update(rec: Dict[str, Any], patch: Dict[str, Any]) -> None:
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(rec.get(k), dict):
                Database._deep_update(rec[k], v)
            else:
                rec[k] = v
