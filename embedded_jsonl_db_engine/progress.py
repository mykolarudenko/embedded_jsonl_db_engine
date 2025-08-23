from typing import Callable, Dict, Optional

class Progress:
    """
    Лёгкий диспетчер прогресса. Все события проходят через on(event: dict).
    event = {"phase": str, "pct": int, "msg": str, "bytes_done": int, "bytes_total": int, ...}
    """
    def __init__(self, cb: Optional[Callable[[Dict], None]] = None) -> None:
        self._cb = cb

    def emit(self, phase: str, pct: int, /, **kw) -> None:
        if self._cb:
            evt = {"phase": phase, "pct": pct}
            evt.update(kw)
            self._cb(evt)
