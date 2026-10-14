"""Feature view definitions and value validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class FeatureType(StrEnum):
    INT = "int"
    FLOAT = "float"
    STR = "str"
    BOOL = "bool"


@dataclass(frozen=True)
class FeatureView:
    """A named group of features about one entity type, with an optional freshness limit.

    ``ttl_seconds`` bounds how old a feature value may be when it is used. A value older than
    that is treated as missing, which stops a stale signal from silently feeding a model.
    """

    name: str
    entity: str
    features: dict[str, FeatureType] = field(default_factory=dict)
    ttl_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.name.replace("_", "").isalnum():
            raise ValueError("view name must be alphanumeric with underscores")
        if not self.features:
            raise ValueError("a view needs at least one feature")
        if self.ttl_seconds is not None and self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "entity": self.entity,
            "features": {k: v.value for k, v in self.features.items()},
            "ttl_seconds": self.ttl_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FeatureView:
        return cls(
            name=data["name"],
            entity=data["entity"],
            features={k: FeatureType(v) for k, v in data["features"].items()},
            ttl_seconds=data.get("ttl_seconds"),
        )

    def validate_values(self, values: dict[str, Any]) -> dict[str, Any]:
        """Check that every declared feature is present and has the declared type."""
        missing = set(self.features) - set(values)
        extra = set(values) - set(self.features)
        if missing or extra:
            raise ValueError(
                f"{self.name}: missing {sorted(missing)} unexpected {sorted(extra)} features"
            )
        return {
            name: _coerce(self.name, name, self.features[name], v) for name, v in values.items()
        }


def _coerce(view: str, name: str, kind: FeatureType, value: Any) -> Any:
    if value is None:
        return None
    ok = (
        (kind is FeatureType.BOOL and isinstance(value, bool))
        or (kind is FeatureType.INT and isinstance(value, int) and not isinstance(value, bool))
        or (
            kind is FeatureType.FLOAT
            and isinstance(value, int | float)
            and not isinstance(value, bool)
        )
        or (kind is FeatureType.STR and isinstance(value, str))
    )
    if not ok:
        raise ValueError(f"{view}.{name}: expected {kind.value}, got {type(value).__name__}")
    return float(value) if kind is FeatureType.FLOAT else value


def to_epoch(ts: datetime | str | float | int) -> float:
    """Normalise timestamps to UTC epoch seconds. Naive datetimes are assumed to be UTC."""
    if isinstance(ts, int | float):
        return float(ts)
    parsed = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()
