# Embedded JSONL DB Engine

Embedded, single-file JSONL database with typed schema and taxonomies in the file header. In-memory indexes on open, fast regex plan for simple predicates, full JSON parse otherwise. Single-writer model with explicit compaction, rolling & daily backups, and external BLOB storage.

Status: alpha. Core features implemented: file I/O, in-memory indexes, CRUD, compaction, backups, taxonomy header + migrations, external BLOBs. Fast-regex plan is integrated for simple queries; complex queries fall back to full parse.

Test status
- Full test suite is expected to pass locally (CRUD, queries, performance, backups retention, corruption handling, schema migration, taxonomy ops, blobs GC).
- Run: ./run-tests.sh

Install
- pip install embedded_jsonl_db_engine

Quick start
- See docs in package and upcoming examples.

Contributing
- Development setup: run ./setup.sh to install dev extras, then ruff and pytest locally.
- Roadmap: implement storage I/O, open/index build, CRUD, compaction/backups, taxonomy migrations, blobs.

Development bootstrap
- Initialize repository structure and minimal package scaffold:
  - embedded_jsonl_db_engine/ with core modules (database.py, storage.py, schema.py, taxonomy.py, index.py, query.py, fastregex.py, blobs.py, utils.py, progress.py, errors.py)
  - pyproject.toml with project metadata and ruff config
  - tests/ with placeholder tests
  - project_log.md to track decisions and progress
- Next steps:
  1) Implement FileStorage I/O primitives (open/lock, header R/W, append, scan, atomic replace).
  2) Implement Database._open() to build in-memory indexes from meta scan.
  3) Implement minimal CRUD: new/get/save/find(delete as a stub if needed).
  4) Add simple tests for open/new/save/get.

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

License
MIT
