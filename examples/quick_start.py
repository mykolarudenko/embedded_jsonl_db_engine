#!/usr/bin/env python3
# Example usage of embedded_jsonl_db_engine
# Note: Core I/O methods in storage/database are skeletons; this script demonstrates intended API.

from embedded_jsonl_db_engine import Database

# Minimal demo schema: a user with name (str), age (int), and flags.active (bool)
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

def main() -> None:
    # Create/open database file (single-writer). '+' means read/write.
    db = Database(path="demo.jsonl", schema=SCHEMA, mode="+")

    # Create new record with defaults applied
    rec = db.new()
    rec["name"] = "Alice"
    rec["age"] = 33
    rec.save()  # validate by schema, append meta+data

    # Fetch it back by id
    loaded = db.get(rec.id)
    print("Loaded:", loaded)

    # Query by simple predicate (could use fast path)
    for r in db.find({"flags": {"active": True}, "age": {"$gte": 18}}):
        print("Adult active:", r["name"], r["age"])

    # Update a set of records
    modified = db.update({"name": "Alice"}, {"age": 34})
    print("Updated records:", modified)

    # Logical delete example
    deleted = db.delete({"name": "Alice"})
    print("Deleted (logical):", deleted)

if __name__ == "__main__":
    main()
