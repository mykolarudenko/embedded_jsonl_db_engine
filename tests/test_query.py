from embedded_jsonl_db_engine import Database

def progress_printer(evt):
    phase = evt.get("phase")
    pct = int(evt.get("pct", 0))
    last = getattr(progress_printer, "_last", {})
    prev = last.get(phase, -1)
    if pct == 100 or pct - prev >= 5 or prev == -1:
        msg = evt.get("msg", "")
        line = f"[progress] {phase} {pct}%"
        if msg:
            line += f" - {msg}"
        print(line, flush=True)
        last[phase] = pct
        progress_printer._last = last

def make_schema():
    return {
        "id":        {"type": "str", "mandatory": True, "index": True},
        "name":      {"type": "str", "mandatory": True, "index": True},
        "age":       {"type": "int", "default": 0, "index": True},
        "active":    {"type": "bool", "default": True},
        "createdAt": {"type": "datetime", "mandatory": True},
        "categories": {
            "type": "list", "items": {"type": "str"},
            "taxonomy": "categories", "taxonomy_mode": "multi",
            "strict": True, "index_membership": True
        }
    }

def test_projection_and_sorting(tmp_path):
    db_path = tmp_path / "users.jsonl"
    db = Database(str(db_path), schema=make_schema(), on_progress=progress_printer)

    # Prepare taxonomy key for strict validation
    db.taxonomy("categories").upsert("general")

    # Create few records
    names_ages = [("Alice", 25), ("Bob", 10), ("Charlie", 50)]
    ids = []
    for n, a in names_ages:
        r = db.new()
        r["name"] = n
        r["age"] = a
        r["categories"] = ["general"]
        r.save()
        ids.append(r.id)

    # Order by age desc and project fields
    it = db.find({"active": True}, order_by=[("age", "desc")], fields=["name"])
    lst = list(it)
    assert [rec["name"] for rec in lst] == ["Charlie", "Alice", "Bob"]
    # Projection keeps only requested fields + id
    assert set(lst[0].keys()) == {"name", "id"}
    assert "age" not in lst[0]

def test_nested_order_by(tmp_path):
    db_path = tmp_path / "users.jsonl"
    schema = make_schema()
    # Extend schema with nested object
    schema["profile"] = {"type": "object", "fields": {
        "score": {"type": "int", "default": 0, "index": True}
    }}
    db = Database(str(db_path), schema=schema, on_progress=progress_printer)
    db.taxonomy("categories").upsert("general")

    # Insert with nested profile.score
    vals = [3, 1, 2]
    for i, s in enumerate(vals):
        r = db.new()
        r["name"] = f"N{i}"
        r["profile"] = {"score": s}
        r["categories"] = ["general"]
        r.save()

    # Sort by nested path
    got = list(db.find({"active": True}, order_by=[("profile/score", "asc")], fields=["name", "profile"]))
    assert [rec["profile"]["score"] for rec in got] == [1, 2, 3]
