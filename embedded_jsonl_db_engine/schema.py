from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Tuple, Union
from .errors import ValidationError, SchemaError

Json = Union[dict, list, str, int, float, bool, None]

SCALAR_TYPES = {"str", "int", "float", "bool", "datetime"}
COMPLEX_TYPES = {"object", "list", "blob"}

@dataclass(frozen=True)
class FieldSpec:
    type: str
    mandatory: bool = False
    default: Any = None
    index: bool = False
    index_membership: bool = False
    taxonomy: str | None = None
    taxonomy_mode: str | None = None  # "single" | "multi"
    strict: bool = False

class Schema:
    """
    Holds nested schema (as in file header). Validation and path access.
    """
    def __init__(self, fields: Dict[str, Any]) -> None:
        self._fields = fields
        self._flat: Dict[Tuple[str, ...], FieldSpec] = {}
        self._flatten(fields, ())

    def _flatten(self, node: Dict[str, Any], path: Tuple[str, ...]) -> None:
        for key, spec in node.items():
            if not isinstance(key, str):
                raise SchemaError(f"Invalid key type at {path}: {key!r}")
            if not isinstance(spec, dict) or "type" not in spec:
                raise SchemaError(f"Field '{'/'.join(path+(key,))}' must be a dict with 'type'")
            t = spec["type"]
            if t in SCALAR_TYPES or t == "blob":
                self._flat[path + (key,)] = FieldSpec(
                    type=t,
                    mandatory=bool(spec.get("mandatory", False)),
                    default=spec.get("default", None),
                    index=bool(spec.get("index", False)),
                    index_membership=bool(spec.get("index_membership", False)),
                    taxonomy=spec.get("taxonomy"),
                    taxonomy_mode=spec.get("taxonomy_mode"),
                    strict=bool(spec.get("strict", False)),
                )
            elif t == "object":
                sub = spec.get("fields")
                if not isinstance(sub, dict):
                    raise SchemaError(f"object '{'/'.join(path+(key,))}' must have 'fields'")
                self._flat[path + (key,)] = FieldSpec(
                    type="object",
                    mandatory=bool(spec.get("mandatory", False)),
                    default=spec.get("default", None),
                )
                self._flatten(sub, path + (key,))
            elif t == "list":
                items = spec.get("items")
                if not isinstance(items, dict) or "type" not in items:
                    raise SchemaError(f"list '{'/'.join(path+(key,))}' must have 'items'")
                self._flat[path + (key,)] = FieldSpec(
                    type="list",
                    mandatory=bool(spec.get("mandatory", False)),
                    default=spec.get("default", []),
                    index_membership=bool(spec.get("index_membership", False)),
                    taxonomy=spec.get("taxonomy"),
                    taxonomy_mode=spec.get("taxonomy_mode"),
                    strict=bool(spec.get("strict", False)),
                )
            else:
                raise SchemaError(f"Unsupported type '{t}' for field '{'/'.join(path+(key,))}'")

    def apply_defaults(self, record: Dict[str, Any]) -> None:
        # Materialize defaults into record in-place
        def walk(spec: Dict[str, Any], obj: Dict[str, Any]) -> None:
            for k, fspec in spec.items():
                t = fspec["type"]
                if t in SCALAR_TYPES or t == "blob":
                    if k not in obj and "default" in fspec:
                        obj[k] = fspec.get("default")
                elif t == "object":
                    sub = fspec.get("fields", {})
                    if k not in obj:
                        obj[k] = {}
                    if not isinstance(obj[k], dict):
                        raise ValidationError(f"Field '{k}' must be object")
                    walk(sub, obj[k])
                elif t == "list":
                    if k not in obj:
                        obj[k] = fspec.get("default", [])
                else:
                    raise SchemaError(f"Unsupported type '{t}'")
        walk(self._fields, record)

    def validate(self, record: Dict[str, Any]) -> None:
        # Full type/presence validation. Raises ValidationError on mismatch.
        def walk(spec: Dict[str, Any], obj: Dict[str, Any], path: Tuple[str, ...]) -> None:
            for k, fspec in spec.items():
                t = fspec["type"]
                p = path + (k,)
                if t in SCALAR_TYPES or t == "blob":
                    if fspec.get("mandatory") and k not in obj:
                        raise ValidationError(f"Mandatory field '{'/'.join(p)}' is missing")
                    if k in obj:
                        v = obj[k]
                        self._validate_scalar(v, t, p)
                elif t == "object":
                    sub = fspec.get("fields", {})
                    if fspec.get("mandatory") and k not in obj:
                        raise ValidationError(f"Mandatory object '{'/'.join(p)}' is missing")
                    if k in obj:
                        if not isinstance(obj[k], dict):
                            raise ValidationError(f"Field '{'/'.join(p)}' must be object")
                        walk(sub, obj[k], p)
                elif t == "list":
                    if fspec.get("mandatory") and k not in obj:
                        raise ValidationError(f"Mandatory list '{'/'.join(p)}' is missing")
                    if k in obj:
                        if not isinstance(obj[k], list):
                            raise ValidationError(f"Field '{'/'.join(p)}' must be list")
                else:
                    raise SchemaError(f"Unsupported type '{t}' at '{'/'.join(p)}'")
        walk(self._fields, record, ())

    @staticmethod
    def _validate_scalar(v: Any, t: str, p: Tuple[str, ...]) -> None:
        if t == "str":
            if not isinstance(v, str):
                raise ValidationError(f"Field '{'/'.join(p)}' expects str, got {type(v).__name__}")
        elif t == "int":
            if not isinstance(v, int):
                raise ValidationError(f"Field '{'/'.join(p)}' expects int, got {type(v).__name__}")
        elif t == "float":
            if not isinstance(v, (int, float)):
                raise ValidationError(f"Field '{'/'.join(p)}' expects float, got {type(v).__name__}")
        elif t == "bool":
            if not isinstance(v, bool):
                raise ValidationError(f"Field '{'/'.join(p)}' expects bool, got {type(v).__name__}")
        elif t == "datetime":
            if not isinstance(v, str):
                raise ValidationError(f"Field '{'/'.join(p)}' expects ISO datetime string")
        elif t == "blob":
            if not (isinstance(v, dict) and "$blob" in v and "size" in v and "mime" in v):
                raise ValidationError(f"Field '{'/'.join(p)}' expects blob-ref dict")
        else:
            raise SchemaError(f"Unknown scalar type '{t}'")
