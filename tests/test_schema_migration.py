import pytest
from embedded_jsonl_db_engine import Database, ValidationError

def test_schema_migration_adds_default_field(tmp_path):
    # Initial schema without "age"
    schema_v1 = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "name": {"type": "str", "mandatory": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }
    db_path = tmp_path / "migrate.jsonl"
    db = Database(str(db_path), schema=schema_v1)
    r = db.new()
    r["name"] = "Alice"
    r.save()
    rid = r.id
    db.close()

    # New schema introduces mandatory "age" with default
    schema_v2 = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "name": {"type": "str", "mandatory": True},
        "age": {"type": "int", "mandatory": True, "default": 0, "index": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }
    db2 = Database(str(db_path), schema=schema_v2)
    rec = db2.get(rid)
    assert rec is not None
    assert rec.get("age") == 0  # default applied during migration

def test_schema_migration_fails_without_default(tmp_path):
    # Initial schema
    schema_v1 = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "name": {"type": "str", "mandatory": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }
    db_path = tmp_path / "migrate_fail.jsonl"
    db = Database(str(db_path), schema=schema_v1)
    r = db.new()
    r["name"] = "Bob"
    r.save()
    db.close()

    # New schema adds mandatory field without default -> migration should fail validation
    schema_v2_bad = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "name": {"type": "str", "mandatory": True},
        "age": {"type": "int", "mandatory": True},  # no default
        "createdAt": {"type": "datetime", "mandatory": True},
    }
    with pytest.raises(ValidationError):
        Database(str(db_path), schema=schema_v2_bad)
