# Embedded JSONL DB Engine — AI Coder API

This document is a concise, engineer-focused overview of the public API and conventions, tailored for code-generation agents.

## Key concepts
- Storage format: append-only JSON Lines with a 4-line header (`header`, `schema`, `taxonomies`, `begin`).
- Concurrency: per-operation process-level file locks with retries, plus in-process writer serialization and a maintenance barrier.
- Taxonomies: controlled vocabularies stored in the header; changing structure may trigger a full-file rewrite.
- Schema: nested type spec with defaults, validation, and index hints.

## Install and import
- Package name: `embedded_jsonl_db_engine`
- Typical imports:
  ```python
  from embedded_jsonl_db_engine import Database
  from embedded_jsonl_db_engine.database import Options
  ```

## Database lifecycle
- Open:
  ```python
  db = Database("/path/to/data.jsonl", schema=SCHEMA, mode="+")
  ```
  - If file is new, header is initialized.
  - If the provided `schema` differs from on-disk, a migration is performed automatically.
- Close:
  ```python
  db.close()
  ```

## Options (runtime tuning)
Create explicitly or pass a dict as `options=`. Defaults provide ~2s of wait for locks/tail reads.

- Locking and tail reads:
  - `process_lock_attempts: int = 40`
  - `process_lock_sleep_ms: int = 50`
  - `read_tail_retry_attempts: int = 40`
  - `read_tail_sleep_ms: int = 50`
  - `maintenance_attempts: int = 40`
  - `maintenance_sleep_ms: int = 50`
  - `allow_shared_read: bool = True` (POSIX shared read; Windows falls back to exclusive)

- Backups:
  - `backup_root_dir: str = "embedded_jsonl_db_backup"`
  - `backup_rolling_keep: int = 3`
  - `backup_daily_dir: str = "daily"`
  - `backup_daily_keep: int = 7`

You can also override backup settings via `maintenance={"backup": {...}}` on `Database(...)`. Values in `maintenance["backup"]` take precedence over `Options`.

## Schema
- Scalar types: `str`, `int`, `float`, `bool`, `datetime`
- Complex: `object`, `list`, `blob`
- Common field keys:
  - `type` (required), `mandatory`, `default`, `index` (for scalar fields)
- Taxonomy fields:
  - list membership: `{"type": "list", "index_membership": True, "taxonomy": "<name>", "strict": <bool>}`
  - single reference: `{"type": "str", "taxonomy": "<name>", "taxonomy_mode": "single", "strict": <bool>}`

## CRUD
- Create:
  ```python
  r = db.new()
  r["name"] = "Alice"
  r.save()
  ```
- Read:
  ```python
  got = db.get("some-id", include_meta=False)
  ```
- Find (query DSL):
  - Supported operators: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$contains` (for list or substring), `$in`, `$nin`, `$regex` with optional `$flags` (`i`, `m`, `s`).
  ```python
  for rec in db.find({"age": {"$gte": 18}}, limit=10, order_by=[("age", "desc")], fields=["id", "name", "age"]):
      ...
  ```
- Update:
  ```python
  db.update({"id": some_id}, {"age": 42})
  ```
- Delete (logical):
  ```python
  db.delete({"id": some_id})
  ```

## Taxonomy API
Use `db.taxonomy("<name>")`:
- `list() -> List[Dict]`
- `get(key: str) -> Optional[Dict]`
- `stats() -> List[Dict]` (per-key counts)
- `upsert(key: str, **attrs)`
- `set_attrs(key: str, **attrs)`
- `rename(old_key: str, new_key: str, collision: str = "merge")`
- `merge(source_keys: List[str], target_key: str)`
- `delete(key: str, strategy: str = "detach")`

Notes:
- `upsert`/`set_attrs` modify only the header (fast).
- `rename`/`merge`/`delete` can trigger a full-file rewrite (maintenance).

## Backups and compaction
- Rolling:
  ```python
  db.backup_now("rolling")  # keeps .bak.1..N in backup_root_dir
  ```
- Daily gz:
  ```python
  db.backup_now("daily")
  ```
- Compaction:
  ```python
  db.compact_now()
  ```
  - Runs only if garbage ratio >= 0.30.

## Blobs (Content-addressed storage)
- Put:
  ```python
  ref = db.put_blob(b"hello", mime="text/plain", filename="hello.txt")
  ```
- Open:
  ```python
  with db.open_blob(ref) as fh:
      data = fh.read()
  ```
- GC:
  ```python
  stats = db.gc_blobs()
  ```

## Concurrency model
- Each public operation performs a short "connect":
  - In-process: serialize writers and block during maintenance.
  - Multi-process: file lock on `path + ".lock"` with retries. Read is shared on POSIX; Windows uses exclusive fallback.
  - Safe tail reads: readers retry if the last line is incomplete (no newline) to avoid truncated reads.
- Maintenance (`schema migration`, `taxonomy rename/merge/delete`, `compact`, `backup`):
  - Fully blocks reads and writes; other operations wait up to configured `maintenance_*` limits.

## Errors to handle
- `ValidationError`, `DuplicateIdError`, `ConflictError`, `QueryError`, `SchemaError`, `IOCorruptionError`, `LockError`.

## Examples
See:
- `examples/taxonomy_quickstart.py`
- `examples/schema_migration_on_the_fly.py`
