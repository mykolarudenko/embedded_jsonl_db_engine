# Embedded JSONL DB Engine

Embedded, single-file JSONL database with typed schema and taxonomies in the file header. In-memory indexes on open, fast regex plan for simple predicates, full JSON parse otherwise. Single-writer model with explicit compaction, rolling & daily backups, and external BLOB storage.

Status: scaffold. Core I/O and heavy logic are intentionally left as NotImplementedError in code for incremental implementation.

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
- Package scaffold with all core modules and clear NotImplementedError stubs for heavy logic.
- English comments and docstrings across the codebase.
- TYPE_CHECKING import in taxonomy to avoid runtime circular imports.
- Query helper is_simple_query() for fast-plan eligibility.
- Fast regex path compiler scaffold (compile_path_pattern, extract_first).
- Utils for ISO timestamps, epoch conversions, canonical JSON, sha256, and ULID-like ids.
- In-memory index structures (MetaEntry, InMemoryIndex) for meta/secondary/reverse indexes.
- Database/TDBRecord skeleton with validation hooks and change tracking.
- FileStorage scaffold with constants and method signatures for low-level I/O.

License
MIT
