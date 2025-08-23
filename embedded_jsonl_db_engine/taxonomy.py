from __future__ import annotations
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .database import Database  # only for type checking to avoid circular import

class TaxonomyAPI:
    """
    Управление таксономиями, хранящимися в шапке файла.
    Все изменения требуют немедленной перепаковки файла (бэкап -> миграция -> replace).
    Реальную миграцию выполняет Database.
    """
    def __init__(self, db: "Database", name: str) -> None:
        self._db = db
        self._name = name

    def list(self) -> List[Dict[str, Any]]:
        items = self._db._taxonomies.get(self._name, {}).get("list", [])
        return list(items)

    def get(self, key: str) -> Dict[str, Any] | None:
        for item in self._db._taxonomies.get(self._name, {}).get("list", []):
            if item.get("key") == key:
                return dict(item)
        return None

    def stats(self) -> List[Dict[str, Any]]:
        # Используем обратный индекс из db._index
        result = []
        for (taxo, key), ids in self._db._index.reverse.items():
            if taxo == self._name:
                result.append({"key": key, "count": len(ids)})
        result.sort(key=lambda x: (-x["count"], x["key"]))
        return result

    def upsert(self, key: str, **attrs: Any) -> None:
        self._db._taxonomy_header_update(self._name, op="upsert", key=key, attrs=attrs)

    def rename(self, old_key: str, new_key: str, collision: str = "merge") -> None:
        self._db._taxonomy_migrate(self._name, action="rename", old_key=old_key, new_key=new_key,
                                   collision=collision)

    def merge(self, source_keys: List[str], target_key: str) -> None:
        self._db._taxonomy_migrate(self._name, action="merge", source_keys=source_keys,
                                   target_key=target_key)

    def delete(self, key: str, strategy: str = "detach") -> None:
        self._db._taxonomy_migrate(self._name, action="delete", key=key, strategy=strategy)

    def set_attrs(self, key: str, **attrs: Any) -> None:
        self._db._taxonomy_header_update(self._name, op="set_attrs", key=key, attrs=attrs)
