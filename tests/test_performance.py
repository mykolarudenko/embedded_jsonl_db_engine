import time
from embedded_jsonl_db_engine import Database
from rich.console import Console
import sys
import os
from datetime import datetime, timezone

_force_tty = os.environ.get("FORCE_TTY", "").lower() in ("1", "true", "yes", "on")
_isatty = getattr(sys.stderr, "isatty", lambda: False)()
_console = Console(file=sys.stderr, force_terminal=(_isatty or _force_tty), color_system="standard")
_tty_progress = os.environ.get("TTY_PROGRESS", "").lower() in ("1", "true", "yes", "on")

def progress_printer(evt):
    phase = evt.get("phase", "")
    pct = int(evt.get("pct", 0))
    msg = evt.get("msg", "")
    state = getattr(progress_printer, "_state", {"last": {}})
    last = state["last"]
    prev = last.get(phase, -1)
    is_tty = _tty_progress and getattr(_console, "is_terminal", False)

    if not is_tty:
        if pct in (0, 100) and (prev != pct):
            parts = [p for p in (phase, f"{pct}%", (f"- {msg}" if msg else "")) if p]
            _console.print("[progress] " + " ".join(parts))
        last[phase] = pct
        progress_printer._state = state
        return

    if pct < 100 and prev != -1 and (pct - prev) < 5:
        return
    parts = [p for p in (phase, f"{pct}%", (f"- {msg}" if (msg and pct in (0, 100)) else "")) if p]
    text = "[progress] " + " ".join(parts)
    _console.print("\r\x1b[2K" + text, end="")
    if pct >= 100:
        _console.print()
    last[phase] = pct
    progress_printer._state = state

def make_perf_schema(n_fields: int = 5):
    """
    Build a schema with given number of scalar fields (in addition to id and tags).
    Fields are named f0..f{n-1} and alternate types for diversity.
    """
    base = {
        "id": {"type": "str", "mandatory": True, "index": True},
        "tags": {"type": "list", "items": {"type": "str"}, "index_membership": True, "taxonomy": "tags"},
    }
    # Cycle through types: int, str, float, bool, datetime (as str)
    kinds = [
        ("int", {"type": "int", "index": True}),
        ("str", {"type": "str", "index": True}),
        ("float", {"type": "float"}),
        ("bool", {"type": "bool"}),
        ("dt", {"type": "datetime"}),  # ISO strings
    ]
    for i in range(n_fields):
        _kname, spec = kinds[i % len(kinds)]
        base[f"f{i}"] = dict(spec)
    return base

def test_performance_big_dataset(tmp_path):
    """
    Configurable perf test:
    - PERF_N: number of records (default 10_000)
    - PERF_FIELDS: number of extra scalar fields (default 5)
    Example for heavy run: PERF_N=100000 PERF_FIELDS=100
    """
    db_path = tmp_path / "perf.jsonl"
    N = int(os.getenv("PERF_N", "10000"))
    N_FIELDS = int(os.getenv("PERF_FIELDS", "5"))

    schema = make_perf_schema(N_FIELDS)
    db = Database(str(db_path), schema=schema, on_progress=progress_printer)

    # prepare taxonomy
    tax = db.taxonomy("tags")
    for i in range(10):
        tax.upsert(f"t{i}", title=f"Tag {i}")

    # Initial open (creates header)
    t0 = time.perf_counter()
    t1 = time.perf_counter()
    _console.print(f"[perf] initial open (new file): {(t1 - t0):.3f}s")

    # Populate N records
    t2 = time.perf_counter()
    for i in range(N):
        r = db.new()
        r["id"] = f"{i:08d}"
        # Populate generated fields
        for fidx in range(N_FIELDS):
            fname = f"f{fidx}"
            kind = schema[fname]["type"]
            if kind == "int":
                r[fname] = i
            elif kind == "str":
                r[fname] = f"user-{i}-{fidx}"
            elif kind == "float":
                r[fname] = float(i) / (fidx + 1 if fidx >= 0 else 1)
            elif kind == "bool":
                r[fname] = (i + fidx) % 2 == 0
            elif kind == "datetime":
                r[fname] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        r["tags"] = [f"t{(i % 10)}"]
        r.save()
        if (i + 1) % 1000 == 0:
            line = f"[perf] inserted {i+1}/{N}"
            if _tty_progress and _console.is_terminal:
                _console.print("\r\x1b[2K" + line, end="")
    t3 = time.perf_counter()
    _console.print(f"[perf] insert {N} records with {N_FIELDS} fields: {(t3 - t2):.3f}s")

    # Close and reopen to measure index build time
    db.close()
    t4 = time.perf_counter()
    db2 = Database(str(db_path), schema=schema, on_progress=progress_printer)
    t5 = time.perf_counter()
    _console.print(f"[perf] reopen and build indexes for {N} records: {(t5 - t4):.3f}s with {N_FIELDS} fields")

    # Fast plan query (~50% match) returning full records — choose first int field
    int_field = None
    for fidx in range(N_FIELDS):
        if schema[f"f{fidx}"]["type"] == "int":
            int_field = f"f{fidx}"
            break
    if int_field is None:
        int_field = "f0"
    q_fast = {int_field: {"$gte": N // 2}}
    t6 = time.perf_counter()
    res_fast = list(db2.find(q_fast))
    t7 = time.perf_counter()
    _console.print(f"[perf] fast-plan query ({int_field} >= {N//2}) matched={len(res_fast)}: {(t7 - t6):.3f}s")

    # Full parse query (forced via $or) with the same logical predicate as fast plan
    q_full = {"$or": [{int_field: {"$gte": N // 2}}, {int_field: {"$gte": N // 2}}]}
    t8 = time.perf_counter()
    res_full = list(db2.find(q_full))
    t9 = time.perf_counter()
    _console.print(f"[perf] full-parse query (same predicate via $or on {int_field}) matched={len(res_full)}: {(t9 - t8):.3f}s")
    assert len(res_full) == len(res_fast)

    # Update all records (empty query matches all)
    t10 = time.perf_counter()
    updated = 0
    for idx, rec in enumerate(db2.find({}), 1):
        # Update one deterministic field if exists, otherwise skip
        fld = "f1" if "f1" in rec else list(rec.keys())[0]
        rec[fld] = 999 if isinstance(rec.get(fld), int) else rec.get(fld)
        rec.save()
        updated += 1
        if idx % 1000 == 0:
            line = f"[perf] updated {idx}/{N}"
            if _tty_progress and _console.is_terminal:
                _console.print("\r\x1b[2K" + line, end="")
    t11 = time.perf_counter()
    _console.print(f"[perf] update all {updated} records: {(t11 - t10):.3f}s")

    # Compact (should trigger if enough garbage produced)
    t12 = time.perf_counter()
    db2.compact_now()
    t13 = time.perf_counter()
    _console.print(f"[perf] compact: {(t13 - t12):.3f}s")

    # Basic sanity (result sizes are plausible)
    assert len(res_fast) >= N // 2
    assert 0 < len(res_full) <= N
