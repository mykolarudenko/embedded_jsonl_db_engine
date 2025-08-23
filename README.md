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

License
MIT
