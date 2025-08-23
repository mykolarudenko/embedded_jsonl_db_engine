embedded_jsonl_db_engine

Embedded, single-file JSONL database with a typed schema and taxonomies stored in the file header. It builds in-memory indexes on open, executes fast regex queries for simple predicates and falls back to full JSON parse for complex ones. Designed for single-process exclusive use, with built-in compaction, rolling & daily backups, and external BLOB storage.

Why this exists

Most embedded stores are binary, schema-less, or require on-disk indexes. This engine aims to be:

Human-readable & diff-able: JSONL + a small header (schema & taxonomies).

Strictly typed: validation on assignment and before save; defaults are always materialized into data.

Simple & predictable: single writer, no background daemons, explicit compaction/backup with progress.

Practical: fast path without secondary on-disk indexes; taxonomies (categories/tags) are first-class.

Key features

One file = one table with a header:

header (format/table info)

schema (strict types, defaults, mandatory flags, index hints)

taxonomies (editable vocabulary for referenced fields)

begin (data marker)

In-memory indexes built at open:

base meta index (id, offsets, deleted, ts)

optional secondary indexes for scalar fields

reverse membership indexes for list[str] taxonomies

Two query plans:

Fast (regex) for up to 3 simple scalar predicates

Full (json.loads) for the rest ($in/$regex/$or, wildcards, etc.)

Compaction when garbage_ratio ≥ 0.30.

Backups: rolling (.bak.1/.2/.3) + daily gzipped snapshot.

Taxonomies in header with atomic rename/merge/delete (full rewrite migration with backups & progress).

External BLOBs (CAS by SHA-256) with GC on compaction.

TDBRecord: dict-like record with .save(); writes only if content hash changed.

Non-goals: multi-writer concurrency, on-disk B-trees/LSM, cross-record ACID transactions.

Installation
pip install embedded_jsonl_db_engine

Quick start
from embedded_jsonl_db_engine import Database

SCHEMA = {
    "id":        {"type": "str", "mandatory": True, "index": True},
    "name":      {"type": "str", "mandatory": True},
    "age":       {"type": "int", "default": 0, "index": True},
    "active":    {"type": "bool", "default": True},
    "createdAt": {"type": "datetime", "mandatory": True},  # auto-managed by the engine
    "categories": {
        "type": "list", "items": {"type": "str"},
        "taxonomy": "categories", "taxonomy_mode": "multi",
        "strict": True, "index_membership": True
    }
}

db = Database("users.jsonl", schema=SCHEMA)

# Create a new record (defaults are materialized automatically)
rec = db.new()
rec["name"] = "Alice"
rec["age"] = 30
rec["categories"] = ["fitness"]
rec.save()  # assigns id, createdAt, writes meta+data

# Simple query (fast plan)
for r in db.find({"age": {"$gt": 25}, "active": True}, limit=10):
    r["age"] += 1
    r.save()  # only writes if hash changed

# Read by id
r2 = db.get(rec.id)
print(r2["name"])

Queries

Queries are nested dicts that mirror the JSON shape.

Fast plan (regex; no full parse) if:

only scalar fields (str/int/float/bool/datetime),

operators in { $eq, $ne, $gt, $gte, $lt, $lte },

≤ 3 predicates,

lists only with concrete index access (no wildcards).

Otherwise, Full plan with json.loads (cached).

Examples:

# Equals / numeric comparisons
db.find({"active": True, "age": {"$gte": 18}})

# Nested
db.find({"address": {"city": "Wien"}})

# Membership (taxonomy reverse index)
db.find({"categories": {"$contains": "fitness"}})

# Complex (forces Full)
db.find({"$or": [{"name": {"$regex": "^Al", "$flags": "i"}}, {"age": {"$in": [20, 30, 40]}}]})

Taxonomies (in-header)

Define taxonomy-backed fields in the schema:

"categories": {
  "type": "list", "items": {"type": "str"},
  "taxonomy": "categories", "taxonomy_mode": "multi",
  "strict": true, "index_membership": true
}


Manage them via API (any structural change triggers backup → full migration → reopen):

tx = db.taxonomy("categories")
tx.upsert("mobility", title="Mobility")   # header-only update
tx.rename("fitness", "health_and_fitness")  # full rewrite migration with progress
tx.merge(["stretching", "mobility"], "mobility")
tx.delete("deprecated", strategy="detach")


Reverse indexes allow instant "$contains" queries by taxonomy key.

Backups & compaction

Compaction: auto/sync when garbage_ratio ≥ 0.30. Rewrites: header → schema → taxonomies → begin → live meta+data. Recomputes len_data/sha256_data.

Backups:

rolling: keep 3 copies (.bak.1/.2/.3)

daily: gzipped snapshot per day if there were changes

Both emit progress events for CLI/UI integration.

db.compact_now()
db.backup_now("rolling")

BLOBs (external only)

Large attachments are stored outside the JSONL in a CAS directory (<basename>.blobs/sha256/…). Records hold small refs:

{"avatar":{"$blob":"sha256:ab…","size":183742,"mime":"image/png","filename":"alice.png"}}


API:

ref = db.put_blob(open("alice.png","rb"), mime="image/png", filename="alice.png")
rec = db.new(); rec["name"]="Alice"; rec["avatar"]=ref; rec.save()

with db.open_blob(rec["avatar"]) as f:
    data = f.read()


Unused blobs are removed during compaction (GC).

Progress events

Pass a callback to stream progress for open/scan, index build, migration, backups, compaction, and blob GC:

def on_progress(evt):
    print(f"[{evt['phase']}] {evt.get('pct',0)}% - {evt.get('msg','')}")

db = Database("users.jsonl", schema=SCHEMA, on_progress=on_progress)


Example phases: open.scan_meta, open.build_indexes, compact.copy, backup.rolling, backup.daily, taxonomy.migrate, schema.migrate, gc.blobs.

File format

First four lines are the header:

{"_t":"header","format":"ejl1","table":"users","comment":"Human users DB","created":"2025-08-23T12:00:00Z","defaults_always_materialized":true}
{"_t":"schema","fields":{ ... }}
{"_t":"taxonomies","items":{ ... }}
{"_t":"begin"}


Then pairs of meta + data:

{"_t":"meta","id":"01J...","op":"put","ts":"2025-08-23T13:10:00Z","len_data":321,"sha256_data":"..."}
{"id":"u1","name":"Alice","age":30,"active":true,"createdAt":"2025-01-01T00:00:00Z","categories":["fitness"]}


Deletes are meta(op:"del") without data.

Limitations

Single process, exclusive writer; no multi-writer concurrency.

No on-disk secondary indexes; all indexes are in memory (rebuilt on open).

Compaction is stop-the-world (but fast and with progress).

Not a replacement for SQLite/LMDB/RocksDB when you need ACID, multi-writer, or huge query optimizations.

Roadmap

Streaming JSON extraction helpers for edge cases (still optional).

More query operators ($between, case-insensitive collation per field).

Optional ULID/UUID pluggable id generators.

License

MIT
