"""The feature store: registry, offline history, online serving and the joins between them."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

from featurestore.schema import FeatureView, to_epoch

Timestamp = datetime | str | float | int

# SQLite's default SQLITE_MAX_VARIABLE_NUMBER is comfortably above this, and keeping each
# query to a few hundred rows avoids ever bumping into it on an unusual build.
_BULK_CHUNK_SIZE = 500

_DDL = """
CREATE TABLE IF NOT EXISTS views (name TEXT PRIMARY KEY, definition TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS offline (
    view TEXT NOT NULL, entity_id TEXT NOT NULL, event_ts REAL NOT NULL, payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS offline_lookup ON offline (view, entity_id, event_ts);
CREATE TABLE IF NOT EXISTS online (
    view TEXT NOT NULL, entity_id TEXT NOT NULL, event_ts REAL NOT NULL, payload TEXT NOT NULL,
    PRIMARY KEY (view, entity_id)
);
"""


class FeatureStore:
    def __init__(self, path: str = ":memory:") -> None:
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.executescript(_DDL)

    # ------------------------------------------------------------------ registry
    def register(self, view: FeatureView) -> None:
        """Create a view, or update it if the definition is compatible with the stored one."""
        existing = self.view(view.name, missing_ok=True)
        if existing is not None and existing.features != view.features:
            raise ValueError(f"view {view.name!r} already exists with different features")
        self._db.execute(
            "INSERT OR REPLACE INTO views (name, definition) VALUES (?, ?)",
            (view.name, json.dumps(view.to_dict())),
        )
        self._db.commit()

    def view(self, name: str, missing_ok: bool = False) -> FeatureView | None:
        row = self._db.execute("SELECT definition FROM views WHERE name = ?", (name,)).fetchone()
        if row is None:
            if missing_ok:
                return None
            raise KeyError(f"unknown feature view {name!r}")
        return FeatureView.from_dict(json.loads(row[0]))

    def views(self) -> list[FeatureView]:
        rows = self._db.execute("SELECT definition FROM views ORDER BY name").fetchall()
        return [FeatureView.from_dict(json.loads(r[0])) for r in rows]

    # ------------------------------------------------------------------ ingestion
    def ingest(self, view_name: str, rows: Iterable[dict[str, Any]]) -> int:
        """Append history. Each row needs ``entity_id``, ``event_ts`` and every feature value."""
        view = self.view(view_name)
        assert view is not None
        prepared: list[tuple[str, str, float, str]] = []
        for row in rows:
            values = {k: v for k, v in row.items() if k not in ("entity_id", "event_ts")}
            payload = view.validate_values(values)
            prepared.append(
                (view_name, str(row["entity_id"]), to_epoch(row["event_ts"]), json.dumps(payload))
            )
        self._db.executemany(
            "INSERT INTO offline (view, entity_id, event_ts, payload) VALUES (?, ?, ?, ?)", prepared
        )
        self._db.commit()
        return len(prepared)

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _split_refs(refs: Sequence[str]) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for ref in refs:
            view, sep, feature = ref.partition(":")
            if not sep or not feature:
                raise ValueError(f"feature reference {ref!r} must look like 'view:feature'")
            grouped.setdefault(view, []).append(feature)
        return grouped

    def _validated(self, refs: Sequence[str]) -> dict[str, FeatureView]:
        views: dict[str, FeatureView] = {}
        for view_name, features in self._split_refs(refs).items():
            view = self.view(view_name)
            assert view is not None
            unknown = [f for f in features if f not in view.features]
            if unknown:
                raise ValueError(f"{view_name} has no feature(s) {unknown}")
            views[view_name] = view
        return views

    @staticmethod
    def _fresh(view: FeatureView, feature_ts: float, as_of: float) -> bool:
        return view.ttl_seconds is None or as_of - feature_ts <= view.ttl_seconds

    # ------------------------------------------------------------------ training
    def get_historical_features(
        self, entity_rows: Sequence[dict[str, Any]], refs: Sequence[str]
    ) -> list[dict[str, Any]]:
        """Point-in-time-correct join for building training sets.

        For every ``{"entity_id", "event_ts"}`` row, each feature is the most recent value known
        at or before ``event_ts``. Values from the future are never used, and values older than the
        view's TTL come back as ``None``. This is what prevents label leakage.

        The join runs as a bulk as-of join: each referenced view costs one query per
        ``_BULK_CHUNK_SIZE`` rows of ``entity_rows``, rather than one query per row.
        """
        views = self._validated(refs)
        wanted = self._split_refs(refs)
        requested = [(str(row["entity_id"]), to_epoch(row["event_ts"])) for row in entity_rows]
        out: list[dict[str, Any]] = [
            {"entity_id": entity_id, "event_ts": as_of} for entity_id, as_of in requested
        ]
        for view_name, features in wanted.items():
            for start in range(0, len(requested), _BULK_CHUNK_SIZE):
                chunk = requested[start : start + _BULK_CHUNK_SIZE]
                for offset, (event_ts, payload) in self._as_of_join(view_name, chunk):
                    idx = start + offset
                    as_of = requested[idx][1]
                    usable = event_ts is not None and self._fresh(views[view_name], event_ts, as_of)
                    values = json.loads(payload) if usable and payload is not None else {}
                    for feature in features:
                        out[idx][f"{view_name}:{feature}"] = values.get(feature)
        return out

    def _as_of_join(
        self, view_name: str, chunk: Sequence[tuple[str, float]]
    ) -> list[tuple[int, tuple[float | None, str | None]]]:
        """For each ``(entity_id, as_of)`` in ``chunk``, the latest offline row at or before it.

        One query per call, built as a ``VALUES`` table joined against ``offline`` through a
        correlated subquery that the ``offline_lookup`` index turns into an indexed lookup per
        requested row, so a whole batch costs one round trip instead of one per row.
        """
        if not chunk:
            return []
        values_sql = ", ".join("(?, ?, ?)" for _ in chunk)
        params: list[Any] = []
        for offset, (entity_id, as_of) in enumerate(chunk):
            params.extend((offset, entity_id, as_of))
        params.append(view_name)
        rows = self._db.execute(
            f"WITH requested(idx, entity_id, as_of) AS (VALUES {values_sql}) "
            "SELECT requested.idx, offline.event_ts, offline.payload FROM requested "
            "LEFT JOIN offline ON offline.rowid = ("
            "  SELECT o.rowid FROM offline o WHERE o.view = ? AND o.entity_id = requested.entity_id"
            "  AND o.event_ts <= requested.as_of ORDER BY o.event_ts DESC LIMIT 1"
            ")",
            params,
        ).fetchall()
        return [(idx, (event_ts, payload)) for idx, event_ts, payload in rows]

    # ------------------------------------------------------------------ online
    def materialize(self, view_name: str, as_of: Timestamp | None = None) -> int:
        """Copy the latest value per entity (known by ``as_of``) into the online store."""
        self.view(view_name)
        cutoff = to_epoch(as_of) if as_of is not None else time.time()
        rows = self._db.execute(
            "SELECT entity_id, MAX(event_ts), payload FROM offline "
            "WHERE view = ? AND event_ts <= ? GROUP BY entity_id",
            (view_name, cutoff),
        ).fetchall()
        for entity_id, event_ts, payload in rows:
            self._db.execute(
                "INSERT INTO online (view, entity_id, event_ts, payload) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(view, entity_id) DO UPDATE SET event_ts = excluded.event_ts, "
                "payload = excluded.payload WHERE excluded.event_ts >= online.event_ts",
                (view_name, entity_id, event_ts, payload),
            )
        self._db.commit()
        return len(rows)

    def get_online_features(
        self, entity_ids: Sequence[str], refs: Sequence[str], now: Timestamp | None = None
    ) -> dict[str, dict[str, Any]]:
        """Low-latency lookup of the latest materialised values. Stale or missing ones are None."""
        views = self._validated(refs)
        wanted = self._split_refs(refs)
        clock = to_epoch(now) if now is not None else time.time()
        result: dict[str, dict[str, Any]] = {}
        for entity_id in entity_ids:
            values: dict[str, Any] = {}
            for view_name, features in wanted.items():
                row = self._db.execute(
                    "SELECT event_ts, payload FROM online WHERE view = ? AND entity_id = ?",
                    (view_name, str(entity_id)),
                ).fetchone()
                usable = row is not None and self._fresh(views[view_name], row[0], clock)
                payload = json.loads(row[1]) if usable else {}
                for feature in features:
                    values[f"{view_name}:{feature}"] = payload.get(feature)
            result[str(entity_id)] = values
        return result
