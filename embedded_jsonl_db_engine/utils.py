import json
import os
import time
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"

def now_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).strftime(ISO_FMT)

def iso_to_epoch_ms(s: str) -> int:
    dt = datetime.strptime(s, ISO_FMT).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def epoch_ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime(ISO_FMT)

def canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def sha256_hex(data: bytes) -> str:
    return sha256(data).hexdigest()

# Упрощённый ULID-подобный id
_ALPH = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
def new_ulid() -> str:
    ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")  # 80 бит
    return f"{ms:012x}{rand:020x}"
