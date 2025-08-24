import os
import re
from embedded_jsonl_db_engine import Database

def make_schema():
    return {
        "id": {"type": "str", "mandatory": True, "index": True},
        "name": {"type": "str", "mandatory": True},
        "createdAt": {"type": "datetime", "mandatory": True},
    }

def test_daily_backup_retention(tmp_path, monkeypatch):
    db_path = tmp_path / "ret.jsonl"
    maintenance = {"backup": {"rolling_keep": 3, "daily_dir": "daily", "daily_keep": 2}}
    db = Database(str(db_path), schema=make_schema(), maintenance=maintenance)

    # Insert one record to have some content
    r = db.new()
    r["name"] = "A"
    r.save()

    base = os.path.basename(str(db_path))
    backup_dir = tmp_path / "embedded_jsonl_db_backup" / "daily"
    os.makedirs(backup_dir, exist_ok=True)

    # Create three daily backups with different dates
    def set_date(datestr: str):
        monkeypatch.setattr("embedded_jsonl_db_engine.database.now_iso", lambda: f"{datestr}T00:00:00Z")

    set_date("2025-01-01")
    db.backup_now("daily")
    set_date("2025-01-02")
    db.backup_now("daily")
    set_date("2025-01-03")
    db.backup_now("daily")

    files = sorted(os.listdir(backup_dir))
    pat = re.compile(re.escape(base) + r"\.(\d{4}-\d{2}-\d{2})\.jsonl\.gz\Z")
    own = [f for f in files if pat.match(f)]
    # Only last 2 should remain: 2025-01-02 and 2025-01-03
    assert len(own) == 2
    assert own[0].endswith("2025-01-02.jsonl.gz")
    assert own[1].endswith("2025-01-03.jsonl.gz")
