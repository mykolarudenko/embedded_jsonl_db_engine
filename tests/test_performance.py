import time
from embedded_jsonl_db_engine import Database

def make_perf_schema():
    # Total 100 fields: 2 required + 3 indexed + 95 generic fields
    fields = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "createdAt": {"type": "datetime", "mandatory": True},
        # Indexed fields
        "ix1": {"type": "int", "default": 0, "index": True},
        "ix2": {"type": "str", "default": "", "index": True},
        "ix3": {"type": "bool", "default": False, "index": True},
    }
    # Add 95 generic (non-indexed) fields
    for i in range(95):
        # Alternate types a bit to simulate mixed schema
        if i % 3 == 0:
            fields[f"f{i:02d}"] = {"type": "str", "default": ""}
        elif i % 3 == 1:
            fields[f"f{i:02d}"] = {"type": "int", "default": 0}
        else:
            fields[f"f{i:02d}"] = {"type": "bool", "default": False}
    return fields

def test_performance_big_dataset(tmp_path):
    db_path = tmp_path / "perf.jsonl"
    schema = make_perf_schema()

    N = 10_000

    # Initial open (creates header)
    t0 = time.perf_counter()
    db = Database(str(db_path), schema=schema)
    t1 = time.perf_counter()
    print(f"[perf] initial open (new file): {(t1 - t0):.3f}s")

    # Populate N records
    t2 = time.perf_counter()
    for i in range(N):
        r = db.new()
        r["ix1"] = i
        r["ix2"] = f"s{i % 1000}"
        r["ix3"] = (i % 2 == 0)
        # Non-indexed fields
        r["f00"] = f"grp{i % 5}"
        r["f01"] = i
        for k in range(2, 95):
            key = f"f{k:02d}"
            if k % 3 == 0:
                r[key] = f"v{i % 10}"
            elif k % 3 == 1:
                r[key] = i % 100
            else:
                r[key] = (i % 3 == 0)
        r.save()
    t3 = time.perf_counter()
    print(f"[perf] insert {N} records: {(t3 - t2):.3f}s")

    # Close and reopen to measure index build time
    db.close()
    t4 = time.perf_counter()
    db2 = Database(str(db_path), schema=schema)
    t5 = time.perf_counter()
    print(f"[perf] reopen and build indexes for {N} records: {(t5 - t4):.3f}s")

    # Fast plan query (~50% match) returning full records
    q_fast = {"ix1": {"$gte": N // 2}}
    t6 = time.perf_counter()
    res_fast = list(db2.find(q_fast))
    t7 = time.perf_counter()
    print(f"[perf] fast-plan query (>= {N//2}) matched={len(res_fast)}: {(t7 - t6):.3f}s")

    # Full parse query (~20% match) on non-indexed field using $in to force full plan
    q_full = {"f00": {"$in": ["grp1"]}}
    t8 = time.perf_counter()
    res_full = list(db2.find(q_full))
    t9 = time.perf_counter()
    print(f"[perf] full-parse query ($in on non-indexed) matched={len(res_full)}: {(t9 - t8):.3f}s")

    # Update all records (empty query matches all)
    t10 = time.perf_counter()
    updated = db2.update({}, {"f01": "updated"})
    t11 = time.perf_counter()
    print(f"[perf] update all {updated} records: {(t11 - t10):.3f}s")

    # Compact (should trigger, garbage_ratio >= 0.30 after updates)
    t12 = time.perf_counter()
    db2.compact_now()
    t13 = time.perf_counter()
    print(f"[perf] compact: {(t13 - t12):.3f}s")

    # Basic sanity (result sizes are plausible)
    assert len(res_fast) >= N // 2
    assert 0 < len(res_full) <= N
