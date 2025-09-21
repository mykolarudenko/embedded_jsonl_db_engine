import os
import time
import threading
import multiprocessing as mp
import pytest

from embedded_jsonl_db_engine.database import Database, Options
from embedded_jsonl_db_engine.storage import FileStorage

def make_schema():
    return {
        "id": {"type": "str", "mandatory": False, "index": True},
        "name": {"type": "str", "mandatory": True, "index": True},
    }

def _hold_write_lock(path: str, hold_sec: float, evt):
    fs = FileStorage(path)
    fs.open_exclusive("+")
    ok = fs.acquire_lock("write", attempts=40, sleep_ms=50, allow_shared_read=True)
    if not ok:
        return
    try:
        evt.set()
        time.sleep(hold_sec)
    finally:
        fs.release_lock()
        fs.close()

def _try_acquire_read_lock(path: str, attempts: int, sleep_ms: int, q):
    fs = FileStorage(path)
    fs.open_exclusive("r")
    ok = fs.acquire_lock("read", attempts=attempts, sleep_ms=sleep_ms, allow_shared_read=True)
    if ok:
        fs.release_lock()
    fs.close()
    q.put(bool(ok))

def test_multiprocess_write_locking_timeout_then_success(tmp_path):
    db_path = tmp_path / "lockdb.jsonl"
    db = Database(str(db_path), schema=make_schema(), mode="+")
    db.close()

    evt = mp.Event()
    p = mp.Process(target=_hold_write_lock, args=(str(db_path), 1.2, evt))
    p.start()
    assert evt.wait(5.0)

    fs = FileStorage(str(db_path))
    fs.open_exclusive("+")
    # Should fail quickly while another process holds write lock
    got = fs.acquire_lock("write", attempts=5, sleep_ms=50, allow_shared_read=True)
    assert got is False

    p.join(timeout=5.0)
    # After release we should be able to acquire
    got2 = fs.acquire_lock("write", attempts=40, sleep_ms=50, allow_shared_read=True)
    assert got2 is True
    fs.release_lock()
    fs.close()

def test_shared_read_lock_allows_multiple_on_posix(tmp_path):
    if os.name == "nt":
        pytest.skip("Windows fallback uses exclusive-only lock")

    db_path = tmp_path / "lockdb2.jsonl"
    db = Database(str(db_path), schema=make_schema(), mode="+")
    db.close()

    fs1 = FileStorage(str(db_path))
    fs1.open_exclusive("r")
    assert fs1.acquire_lock("read", attempts=40, sleep_ms=25, allow_shared_read=True) is True

    q = mp.Queue()
    p = mp.Process(target=_try_acquire_read_lock, args=(str(db_path), 20, 50, q))
    p.start()
    ok = q.get(timeout=5.0)
    p.join(timeout=5.0)
    assert ok is True

    fs1.release_lock()
    fs1.close()

def test_multithread_writers_serialized(tmp_path):
    db_path = tmp_path / "threaded.jsonl"
    db = Database(str(db_path), schema=make_schema(), mode="+", options=Options())

    threads = []
    N_THREADS = 5
    PER_THREAD = 50

    def worker(idx: int):
        for i in range(PER_THREAD):
            rec = db.new()
            rec["name"] = f"user-{idx}-{i}"
            rec.save()

    for t in range(N_THREADS):
        th = threading.Thread(target=worker, args=(t,))
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

    st = db.stats()
    assert st["live"] == N_THREADS * PER_THREAD
    db.close()
