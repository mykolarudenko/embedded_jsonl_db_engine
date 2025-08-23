from __future__ import annotations
from typing import Any, Dict

SIMPLE_OPS = {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}

def is_simple_query(q: Dict[str, Any], max_terms: int = 3) -> bool:
    """
    Проверяет, что запрос состоит из <= max_terms простых скалярных предикатов без $or/$in/$regex/etc.
    """
    terms = 0
    def visit(obj: Any) -> bool:
        nonlocal terms
        if terms > max_terms:
            return False
        if isinstance(obj, dict):
            if any(k in obj for k in ("$or", "$in", "$nin", "$regex", "$contains")):
                return False
            for k, v in obj.items():
                if isinstance(v, dict):
                    if any(op in v for op in SIMPLE_OPS):
                        terms += 1
                    else:
                        if not visit(v):
                            return False
                else:
                    terms += 1
        return True
    ok = visit(q)
    return ok and terms <= max_terms
