import re
from typing import Pattern

# Упрощённые паттерны для извлечения значений по JSON-пути.
# Важно: это быстрый эвристический путь, он не покрывает все углы JSON.
# Где и как используется (eng):
# - In Database._build_indexes_on_open() fast path compiles one regex per scalar index path
#   and extracts values directly from JSON lines to populate secondary indexes, avoiding json.loads.
# - In Database.find(), when is_simple_query(query) is True, we compile regex patterns for the
#   referenced scalar paths and extract values to pre-check predicates. This reduces the number
#   of full JSON parses to only records that already matched fast checks or require projection.
# - Cost model:
#   • compile_path_pattern(): O(len(path)) compile once per distinct path per process.
#   • extract_first(): O(len(line)) search with DOTALL regex, typically dominated by JSON line length.
#   • parse of matched scalar is trivial (int/float/bool via cast; str via json.loads on the token).
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
    """
    Компилирует regex для поиска значения на пути "a/b/c" в одной JSON-строке.
    Ключи экранируются через re.escape. Между сегментами допускаются пробелы/переносы.
    """
    parts = [p for p in path.split("/") if p]
    segs: list[str] = []
    for i, key in enumerate(parts):
        k = re.escape(key)
        if i < len(parts) - 1:
            # "key"\s*:\s*{  (вложенный объект)
            segs.append(rf'"{k}"\s*:\s*\{{\s*')
        else:
            # "key"\s*:\s*(<VAL>)
            segs.append(rf'"{k}"\s*:\s*({_val_pattern(tp)})')
    pat = "".join(segs)
    # DOTALL — чтобы . покрывало переводы строк; IGNORECASE не нужен
    return re.compile(pat, re.DOTALL)

def extract_first(pattern: Pattern[str], data_line: str) -> str | None:
    """
    Возвращает строковое представление первого найденного значения (как в исходной JSON-строке),
    либо None, если не найдено.
    """
    m = pattern.search(data_line)
    if not m:
        return None
    return m.group(1)
