# Embedded JSONL DB Engine

Human-readable and human-editable storage for serializable objects. A single JSONL file acts as a database with a typed schema and taxonomies stored in the header. Appends-only write model means you don't rewrite the whole file on every change. The schema exists but can evolve freely: add new fields at any time, and the database will self-adapt by materializing defaults and preserving existing data without manual migrations. For simple predicates we use a fast regex plan; otherwise we fall back to full JSON parsing. Single-writer model with explicit compaction, rolling & daily backups, and external BLOB storage.

Status: alpha (0.1.0a2). Core features implemented: file I/O, in-memory indexes, CRUD, compaction, backups, taxonomy header + migrations, external BLOBs. Fast-regex plan is integrated for simple queries; complex queries fall back to full parse.

Test status
- Full test suite passes locally (CRUD, queries, performance, backups retention, corruption handling, schema migration, taxonomy ops, blobs GC).
- Note on parallelism: current engine favors simplicity and deterministic ordering; regex fast-path and index build operate single-threaded to avoid GIL/IPC overhead. Parallel scan/parse across CPU cores can be added later via multiprocessing if workloads demand it.
- Run: ./run-tests.sh
- Reference run (on Dev machine):
  - 21 passed in ~9s
  - reopen and build indexes for 10000 records: ~0.47s
  - fast-plan query (>= 5000) matched=5000: ~0.63s
  - full-parse query (same predicate via $or) matched=5000: ~0.72s

Install
- pip install embedded_jsonl_db_engine


Quick start

Install
- pip install embedded_jsonl_db_engine

Minimal example (quick_start.py)
```python
from embedded_jsonl_db_engine import Database

SCHEMA = {
    "id": {"type": "str", "mandatory": False, "index": True},
    "name": {"type": "str", "mandatory": True, "index": True},
    "age": {"type": "int", "mandatory": False, "default": 0, "index": True},
    "flags": {
        "type": "object",
        "fields": {
            "active": {"type": "bool", "mandatory": False, "default": True, "index": True},
        },
    },
    "createdAt": {"type": "datetime", "mandatory": False, "index": True},
}

db = Database(path="demo.jsonl", schema=SCHEMA, mode="+")
rec = db.new()
rec["name"] = "Alice"
rec["age"] = 33
rec.save()

loaded = db.get(rec.id)
print("Loaded:", loaded)

for r in db.find({"flags": {"active": True}, "age": {"$gte": 18}}):
    print("Adult active:", r["name"], r["age"])
```

Run the example
- python examples/quick_start.py

Contributing
- Development setup: run ./setup.sh to install dev extras, then ruff and pytest locally.
- Roadmap: implement storage I/O, open/index build, CRUD, compaction/backups, taxonomy migrations, blobs.

What has been implemented so far
- Low-level file I/O (FileStorage): cross-platform exclusive lock, header read/write/rewrite, append meta+data with fsync, meta scan with offsets, atomic replace.
- Database open with progress: lock, header init if missing, base meta index rebuild, secondary/reverse index build.
- In-memory indexes: secondary (scalar) and reverse (taxonomy) indexes; built on open and maintained on save()/delete(); prefilter in find().
- CRUD: new() with defaults, get() (with optional meta), save() with schema validation and canonical JSON, find() with predicate evaluation + index prefilter, update(), delete() (logical).
- Queries: field projection (fields=[...]), ordering (supports nested paths "a/b"), skip/limit; is_simple_query() helper; fast regex plan for simple scalar predicates with fallback to full json.loads.
- Maintenance: compact_now() (garbage ratio ≥ 0.30), backup_now() (rolling and daily .gz) with progress events.
- Taxonomies: header-only updates (rewrite_header), full migrations (rename/merge/delete detach) with progress; strict schema validation for taxonomy-backed fields.
- BLOBs: external CAS by sha256 with put/open/gc and Database wrappers.
- Utilities: ISO timestamps, epoch converters, canonical JSON, sha256, ULID-like ids.

  [![PyPI Downloads](https://static.pepy.tech/badge/embedded-jsonl-db-engine)](https://pepy.tech/projects/embedded-jsonl-db-engine)

License
MIT
