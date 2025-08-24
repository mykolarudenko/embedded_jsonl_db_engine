import pytest
from embedded_jsonl_db_engine import Database
from embedded_jsonl_db_engine.errors import SchemaError

def test_type_change_is_rejected(tmp_path):
    # Initial schema with int field
    schema_v1 = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "age": {"type": "int", "default": 0, "index": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }
    db_path = tmp_path / "typechg.jsonl"
    db = Database(str(db_path), schema=schema_v1)
    r = db.new()
    r["age"] = 10
    r.save()
    db.close()

    # New schema changes age to str -> should be rejected on open
    schema_v2 = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "age": {"type": "str", "default": "", "index": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }
    with pytest.raises(SchemaError):
        Database(str(db_path), schema=schema_v2)
