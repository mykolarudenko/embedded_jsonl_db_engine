import json
import os
import pytest
from embedded_jsonl_db_engine import Database
from embedded_jsonl_db_engine.errors import IOCorruptionError

def make_schema():
    return {
        "id":        {"type": "str", "mandatory": True, "index": True},
        "name":      {"type": "str", "mandatory": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }

def test_corrupt_meta_len_data(tmp_path):
    db_path = tmp_path / "corrupt.jsonl"
    db = Database(str(db_path), schema=make_schema())
    r = db.new()
    r["name"] = "X"
    r.save()
    rid = r.id
    db.close()

    # Corrupt last meta: increment len_data by 1
    lines = []
    with open(db_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    # Find last meta line index
    meta_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].startswith('{"_t":"meta"'):
            meta_idx = i
            break
    assert meta_idx is not None
    meta = json.loads(lines[meta_idx])
    assert "len_data" in meta
    meta["len_data"] = int(meta["len_data"]) + 1
    lines[meta_idx] = json.dumps(meta, ensure_ascii=False, separators=(",", ":")) + "\n"
    with open(db_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    # Reopen and verify get() raises, find() skips corrupt
    db2 = Database(str(db_path), schema=make_schema())
    with pytest.raises(IOCorruptionError):
        db2.get(rid, include_meta=True)
    # find should skip corrupt record
    assert list(db2.find({"id": rid})) == []
