import re
from typing import Pattern

_STR = r'"(?:(?:[^"\\]|\\.)*)"'
_INT = r'-?\d+'
_FLOAT = r'-?(?:\d+\.\d+|\d+)(?:[eE][+-]?\d+)?'
_BOOL = r'(?:true|false)'

def _val_pattern(tp: str) -> str:
    if tp == "str" or tp == "datetime":
        return _STR
    if tp == "int":
        return _INT
    if tp == "float":
        return _FLOAT
    if tp == "bool":
        return _BOOL
    raise ValueError(f"Unsupported scalar type for fast regex: {tp}")

def compile_path_pattern(path: str, tp: str) -> Pattern[str]:
    parts = [p for p in path.split("/") if p]
    segs = []
    for i, key in enumerate(parts):
        if i < len(parts) - 1:
            segs.append(rf'"{re.escape(key)}"\s*:\s*\{{\s*')
        else:
            segs.append(rf'"{re.escape(key)}"\s*:\s*({_val_pattern(tp)})')
    pat = "".join(segs)
    return re.compile(pat, re.DOTALL)

def extract_first(pattern: Pattern[str], data_line: str) -> str | None:
    m = pattern.search(data_line)
    if not m:
        return None
    return m.group(1)
