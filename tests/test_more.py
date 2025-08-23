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
