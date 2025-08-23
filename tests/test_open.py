import os
from embedded_jsonl_db_engine import Database

def make_schema():
    return {
        "id":        {"type": "str", "mandatory": True, "index": True},
        "name":      {"type": "str", "mandatory": True},
        "age":       {"type": "int", "default": 0, "index": True},
        "active":    {"type": "bool", "default": True},
        "createdAt": {"type": "datetime", "mandatory": True},
        "categories": {
            "type": "list", "items": {"type": "str"},
            "taxonomy": "categories", "taxonomy_mode": "multi",
            "strict": True, "index_membership": True
        }
    }

def test_crud_index_backup_compact(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema())

    # Ensure taxonomy keys exist for strict validation
    db.taxonomy("categories").upsert("fitness", title="Fitness")

    # Create and save a record
    r = db.new()
    r["name"] = "Alice"
    r["age"] = 30
    r["categories"] = ["fitness"]
    r.save()
    rid = r.id
    assert rid is not None

    # Read by id
    r2 = db.get(rid)
    assert r2 is not None
    assert r2["name"] == "Alice"
    assert r2["age"] == 30

    # Find by indexed equality and taxonomy membership
    got = list(db.find({"age": {"$gte": 18}, "active": True}))
    assert any(rec.id == rid for rec in got)
    got2 = list(db.find({"categories": {"$contains": "fitness"}}))
    assert any(rec.id == rid for rec in got2)

    # Update via API
    n = db.update({"id": rid}, {"age": 31})
    assert n == 1
    r3 = db.get(rid)
    assert r3 is not None and r3["age"] == 31

    # Delete and ensure not found
    n_del = db.delete({"id": rid})
    assert n_del == 1
    assert db.get(rid) is None
    assert list(db.find({"id": rid})) == []

    # Backup (rolling) and compact should work without exceptions
    db.backup_now("rolling")
    backup_dir = tmp_path / "embedded_jsonl_db_backup"
    assert backup_dir.exists()

    # After one put + one del, compaction should trigger (garbage_ratio >= 0.30)
    db.compact_now()
    # DB still operational
    assert list(db.find({"active": True})) == []

def test_taxonomy_header_update(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema())

    tx = db.taxonomy("categories")
    tx.upsert("fitness", title="Fitness")
    lst = tx.list()
    assert any(item.get("key") == "fitness" for item in lst)

    # Create a record that references the taxonomy key to verify migration
    r = db.new()
    r["name"] = "Bob"
    r["categories"] = ["fitness"]
    r.save()
    rid = r.id

    # Rename taxonomy key (full-file migration)
    tx.rename("fitness", "health_and_fitness")
    lst2 = tx.list()
    assert any(item.get("key") == "health_and_fitness" for item in lst2)

    # Ensure the record was migrated to the new taxonomy key
    r_after = db.get(rid)
    assert r_after is not None
    assert "health_and_fitness" in r_after.get("categories", [])
