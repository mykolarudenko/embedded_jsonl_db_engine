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

License
MIT
