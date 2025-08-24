from __future__ import annotations
from typing import Any, Dict

SIMPLE_OPS = {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}

def is_simple_query(q: Dict[str, Any], max_terms: int = 3) -> bool:
    """
    Check that the query contains <= max_terms simple scalar predicates without $or/$regex/$contains/etc.
    """
    terms = 0
    def visit(obj: Any) -> bool:
        nonlocal terms
        if terms > max_terms:
            return False
        if isinstance(obj, dict):
            if any(k in obj for k in ("$or", "$regex", "$contains")):
                return False
            for k, v in obj.items():
                if isinstance(v, dict):
                    # allow simple ops + $in/$nin on scalars
                    if any(op in v for op in (SIMPLE_OPS | {"$in", "$nin"})):
                        terms += 1
                    else:
                        if not visit(v):
                            return False
                else:
                    terms += 1
        return True
    ok = visit(q)
    return ok and terms <= max_terms
