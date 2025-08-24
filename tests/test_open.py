import os
from embedded_jsonl_db_engine import Database
from rich.console import Console

_console = Console(force_terminal=True, color_system="standard")

def progress_printer(evt):
    phase = evt.get("phase", "")
    pct = int(evt.get("pct", 0))
    msg = evt.get("msg", "")
    parts = []
    if phase:
        parts.append(phase)
    parts.append(f"{pct}%")
    if msg:
        parts.append(f"- {msg}")
    text = f"[progress] {' '.join(parts)}"
    _console.print(f"\r{text}", end="", highlight=False, soft_wrap=False)
    if pct >= 100:
        _console.print()

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
    db = Database(str(db_path), schema=make_schema(), on_progress=progress_printer)

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

    # $in operator should work with index prefilter
    got_in = list(db.find({"age": {"$in": [10, 31, 99]}}))
    assert any(rec.id == rid for rec in got_in)

    # Delete and ensure not found
    n_del = db.delete({"id": rid})
    assert n_del == 1
    assert db.get(rid) is None
    assert list(db.find({"id": rid})) == []

    # Backup (rolling) and compact should work without exceptions
    db.backup_now("rolling")
    backup_dir = tmp_path / "embedded_jsonl_db_backup"
    assert backup_dir.exists()

    db.backup_now("daily")
    assert (backup_dir / "daily").exists()
    daily_dir = backup_dir / "daily"
    files = sorted(os.listdir(daily_dir))
    assert len(files) == 1
    dest_path = daily_dir / files[0]
    mtime1 = os.path.getmtime(dest_path)
    db.backup_now("daily")
    mtime2 = os.path.getmtime(dest_path)
    assert mtime2 == mtime1

    # After one put + one del, compaction should trigger (garbage_ratio >= 0.30)
    db.compact_now()
    # DB still operational
    assert list(db.find({"active": True})) == []

def test_taxonomy_header_update(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema(), on_progress=progress_printer)

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


def test_blobs_gc(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema(), on_progress=progress_printer)

    # Put and open blob
    ref = db.put_blob(b"hello world", mime="text/plain", filename="hello.txt")
    with db.open_blob(ref) as f:
        data = f.read()
    assert data == b"hello world"

    # No live references, GC should remove it
    stats = db.gc_blobs()
    assert stats["files_removed"] >= 1
