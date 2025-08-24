import time
from embedded_jsonl_db_engine import Database
from rich.console import Console

_console = Console(force_terminal=True, color_system="standard")

def progress_printer(evt):
    phase = evt.get("phase", "")
    pct = int(evt.get("pct", 0))
    msg = evt.get("msg", "")
    state = getattr(progress_printer, "_state", {"last": {}})
    last = state["last"]
    prev = last.get(phase, -1)
    # Reduce spam on large runs; update every 5% and on completion
    if pct < 100 and prev != -1 and (pct - prev) < 5:
        return
    parts = []
    if phase:
        parts.append(phase)
    parts.append(f"{pct}%")
    if msg and pct in (0, 100):
        parts.append(f"- {msg}")
    text = f"[progress] {' '.join(parts)}"
    _console.print("\r\x1b[2K" + text, end="", highlight=False, soft_wrap=False)
    if pct >= 100:
        _console.print()
    last[phase] = pct
    progress_printer._state = state

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
    db = Database(str(db_path), schema=schema, on_progress=progress_printer)
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
        if (i + 1) % 1000 == 0:
            print(f"[perf] inserted {i+1}/{N}", flush=True)
    t3 = time.perf_counter()
    print(f"[perf] insert {N} records: {(t3 - t2):.3f}s")

    # Close and reopen to measure index build time
    db.close()
    t4 = time.perf_counter()
    db2 = Database(str(db_path), schema=schema, on_progress=progress_printer)
    t5 = time.perf_counter()
    print(f"[perf] reopen and build indexes for {N} records: {(t5 - t4):.3f}s")

    # Fast plan query (~50% match) returning full records
    q_fast = {"ix1": {"$gte": N // 2}}
    t6 = time.perf_counter()
    res_fast = list(db2.find(q_fast))
    t7 = time.perf_counter()
    print(f"[perf] fast-plan query (>= {N//2}) matched={len(res_fast)}: {(t7 - t6):.3f}s")

    # Full parse query (forced via $or) with the same logical predicate as fast plan
    q_full = {"$or": [{"ix1": {"$gte": N // 2}}, {"ix1": {"$gte": N // 2}}]}
    t8 = time.perf_counter()
    res_full = list(db2.find(q_full))
    t9 = time.perf_counter()
    print(f"[perf] full-parse query (same predicate via $or) matched={len(res_full)}: {(t9 - t8):.3f}s")
    assert len(res_full) == len(res_fast)

    # Update all records (empty query matches all)
    t10 = time.perf_counter()
    updated = 0
    for idx, rec in enumerate(db2.find({}), 1):
        rec["f01"] = 999
        rec.save()
        updated += 1
        if idx % 1000 == 0:
            print(f"[perf] updated {idx}/{N}", flush=True)
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
