from embedded_jsonl_db_engine import Database

def make_schema():
    return {
        "id": {"type": "str", "mandatory": True, "index": True},
        "name": {"type": "str", "mandatory": True},
        "age": {"type": "int", "default": 0, "index": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }

def test_fast_in_operator(tmp_path):
    db_path = tmp_path / "fastin.jsonl"
    db = Database(str(db_path), schema=make_schema())

    # Insert records
    for name, age in [("A", 10), ("B", 20), ("C", 30)]:
        r = db.new()
        r["name"] = name
        r["age"] = age
        r.save()

    res = list(db.find({"age": {"$in": [10, 30]}}))
    names = {r["name"] for r in res}
    assert names == {"A", "C"}
