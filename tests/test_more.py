import pytest
from embedded_jsonl_db_engine import Database, DuplicateIdError, ConflictError

def make_schema():
    return {
        "id":        {"type": "str", "mandatory": True, "index": True},
        "name":      {"type": "str", "mandatory": True},
        "age":       {"type": "int", "default": 0, "index": True},
        "active":    {"type": "bool", "default": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }

def test_get_include_meta(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema())

    r = db.new()
    r["name"] = "Alice"
    r.save()

    got = db.get(r.id, include_meta=True)
    assert got is not None
    assert got.meta is not None
    assert got.meta.get("_t") == "meta"
    assert got.meta.get("id") == r.id
    assert got["name"] == "Alice"

def test_duplicate_id(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema())

    r1 = db.new()
    r1["id"] = "fixed-id"
    r1["name"] = "A"
    r1.save()

    r2 = db.new()
    r2["id"] = "fixed-id"
    r2["name"] = "B"
    with pytest.raises(DuplicateIdError):
        r2.save()

def test_conflict_detection(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema())

    r1 = db.new()
    r1["name"] = "Alice"
    r1.save()

    # Load second instance and save a change
    r2 = db.get(r1.id)
    assert r2 is not None
    r2["age"] = 42
    r2.save()

    # Try saving stale r1 (should conflict)
    r1["age"] = 100
    with pytest.raises(ConflictError):
        r1.save()


def test_progress_events(tmp_path):
    events = []

    def collect(evt):
        events.append(evt.get("phase"))

    db_path = tmp_path / "events.jsonl"
    db = Database(str(db_path), schema=make_schema(), on_progress=collect)

    # Open should emit progress
    assert any(p in ("open.start", "open.scan_meta", "open.build_indexes", "open.done") for p in events)

    # Insert few records
    ids = []
    for i in range(3):
        r = db.new()
        r["name"] = f"U{i}"
        r.save()
        ids.append(r.id)

    # Update progress
    events.clear()
    n_upd = db.update({}, {"age": 1})
    assert n_upd == 3
    assert "update.start" in events and "update.done" in events

    # Delete progress
    events.clear()
    n_del = db.delete({"id": ids[0]})
    assert n_del == 1
    assert "delete.start" in events and "delete.done" in events

    # Backup progress
    events.clear()
    db.backup_now("rolling")
    assert "backup.rolling" in events
    events.clear()
    db.backup_now("daily")
    assert "backup.daily" in events

    # Compaction progress (should trigger after one delete out of three puts)
    events.clear()
    db.compact_now()
    assert "compact.start" in events
    assert "compact.copy" in events
    assert "compact.done" in events


def test_taxonomy_merge_and_delete(tmp_path):
    # Schema with taxonomy-backed list field
    schema = {
        "id":        {"type": "str", "mandatory": True, "index": True},
        "name":      {"type": "str", "mandatory": True},
        "createdAt": {"type": "datetime", "mandatory": True},
        "categories": {
            "type": "list", "items": {"type": "str"},
            "taxonomy": "categories", "taxonomy_mode": "multi",
            "strict": True, "index_membership": True
        }
    }
    db_path = tmp_path / "taxo.jsonl"
    db = Database(str(db_path), schema=schema)

    tx = db.taxonomy("categories")
    tx.upsert("a")
    tx.upsert("b")
    tx.upsert("target")

    # Insert records referencing a and b
    r1 = db.new()
    r1["name"] = "A"
    r1["categories"] = ["a"]
    r1.save()
    r2 = db.new()
    r2["name"] = "B"
    r2["categories"] = ["b"]
    r2.save()

    # Merge a,b -> target
    tx.merge(["a", "b"], "target")
    g1 = db.get(r1.id)
    g2 = db.get(r2.id)
    assert g1 is not None and g2 is not None
    assert "target" in g1.get("categories", []) and "a" not in g1.get("categories", [])
    assert "target" in g2.get("categories", []) and "b" not in g2.get("categories", [])
    # Reverse index should find two
    got = list(db.find({"categories": {"$contains": "target"}}))
    assert len(got) == 2

    # Delete target (detach)
    tx.delete("target", strategy="detach")
    g1 = db.get(r1.id)
    g2 = db.get(r2.id)
    assert g1 is not None and g2 is not None
    assert "target" not in g1.get("categories", []) and "target" not in g2.get("categories", [])
    got2 = list(db.find({"categories": {"$contains": "target"}}))
    assert len(got2) == 0


def test_gc_blobs_keeps_referenced(tmp_path):
    db_path = tmp_path / "blobs.jsonl"
    db = Database(str(db_path), schema=make_schema())

    # Put blob and reference it from a record
    ref = db.put_blob(b"data", mime="text/plain", filename="d.txt")
    r = db.new()
    r["name"] = "WithBlob"
    r["avatar"] = ref  # not in schema, but gc scans JSON shapes for $blob
    r.save()

    stats = db.gc_blobs()
    assert stats["files_removed"] == 0


def test_stats(tmp_path):
    db_path = tmp_path / "stats.jsonl"
    db = Database(str(db_path), schema=make_schema())

    r1 = db.new()
    r1["name"] = "A"
    r1.save()
    r2 = db.new()
    r2["name"] = "B"
    r2.save()
    db.delete({"id": r1.id})

    st = db.stats()
    assert st["live"] == 1
    assert st["deleted"] == 1
    # At least some secondary index entries should exist (id/name/age)
    assert st["secondary_index_entries"] >= 1
