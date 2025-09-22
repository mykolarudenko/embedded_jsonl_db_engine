#!/usr/bin/env python3
# Example: schema migration on the fly
# - Start with V1 schema and insert data
# - Re-open the same DB with V2 schema that adds new fields with defaults
# - The engine migrates the file and materializes defaults for existing records

import os
from embedded_jsonl_db_engine import Database

SCHEMA_V1 = {
    "id": {"type": "str", "mandatory": False, "index": True},
    "name": {"type": "str", "mandatory": True, "index": True},
    "createdAt": {"type": "datetime", "mandatory": False},
}

SCHEMA_V2 = {
    "id": {"type": "str", "mandatory": False, "index": True},
    "name": {"type": "str", "mandatory": True, "index": True},
    "age": {"type": "int", "mandatory": False, "default": 0, "index": True},
    "flags": {
        "type": "object",
        "fields": {
            "active": {"type": "bool", "mandatory": False, "default": True, "index": True},
        },
    },
    "createdAt": {"type": "datetime", "mandatory": False},
}

def ensure_dir(p: str) -> None:
    d = os.path.dirname(p)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def main() -> None:
    base_dir = os.path.join(os.path.dirname(__file__), "data")
    ensure_dir(base_dir)
    path = os.path.join(base_dir, "schema_migration.jsonl")

    # Create DB with V1 schema and insert some data
    db = Database(path, schema=SCHEMA_V1, mode="+")
    r = db.new()
    r["name"] = "Alice"
    r.save()
    rid = r.id
    print("Before migration:", db.get(rid).__dict__ if rid else None)
    db.close()

    # Re-open with V2 schema; engine detects diff and performs migration
    db2 = Database(path, schema=SCHEMA_V2, mode="+")
    rec2 = db2.get(rid) if rid else None
    print("After migration:", dict(rec2) if rec2 else None)

    # Update some of the new fields using update()
    updated = db2.update({"id": rid}, {"age": 42, "flags": {"active": False}})
    print("Updated count:", updated)
    rec3 = db2.get(rid) if rid else None
    print("After update:", dict(rec3) if rec3 else None)

    db2.close()
    print("Done.")

if __name__ == "__main__":
    main()
