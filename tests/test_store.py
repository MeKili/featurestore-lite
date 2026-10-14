from pathlib import Path

import pytest

from featurestore import FeatureStore, FeatureType, FeatureView

REFS = ["stats:orders", "stats:basket"]


def make_store(ttl: float | None = None) -> FeatureStore:
    store = FeatureStore()
    store.register(
        FeatureView(
            "stats",
            "customer",
            {"orders": FeatureType.INT, "basket": FeatureType.FLOAT},
            ttl_seconds=ttl,
        )
    )
    store.ingest(
        "stats",
        [
            {"entity_id": "c1", "event_ts": 100, "orders": 1, "basket": 10.0},
            {"entity_id": "c1", "event_ts": 200, "orders": 2, "basket": 20.0},
            {"entity_id": "c1", "event_ts": 300, "orders": 3, "basket": 30.0},
            {"entity_id": "c2", "event_ts": 150, "orders": 7, "basket": 70.0},
        ],
    )
    return store


def test_point_in_time_join_uses_latest_value_at_or_before_the_timestamp() -> None:
    store = make_store()
    rows = store.get_historical_features(
        [
            {"entity_id": "c1", "event_ts": 250},
            {"entity_id": "c1", "event_ts": 200},
            {"entity_id": "c1", "event_ts": 99},
            {"entity_id": "c2", "event_ts": 1000},
        ],
        REFS,
    )
    assert [r["stats:orders"] for r in rows] == [2, 2, None, 7]


def test_point_in_time_join_never_leaks_future_values() -> None:
    store = make_store()
    for as_of in range(0, 400, 7):
        row = store.get_historical_features([{"entity_id": "c1", "event_ts": as_of}], REFS)[0]
        known = [o for ts, o in ((100, 1), (200, 2), (300, 3)) if ts <= as_of]
        assert row["stats:orders"] == (known[-1] if known else None)


def test_ttl_turns_stale_values_into_none() -> None:
    store = make_store(ttl=50)
    rows = store.get_historical_features(
        [{"entity_id": "c1", "event_ts": 240}, {"entity_id": "c1", "event_ts": 260}], REFS
    )
    assert rows[0]["stats:orders"] == 2
    assert rows[1]["stats:orders"] is None


def test_iso_timestamps_and_naive_datetimes_agree() -> None:
    store = FeatureStore()
    store.register(FeatureView("v", "e", {"x": FeatureType.INT}))
    store.ingest("v", [{"entity_id": "a", "event_ts": "2026-01-01T00:00:00+00:00", "x": 5}])
    row = store.get_historical_features([{"entity_id": "a", "event_ts": "2026-01-02"}], ["v:x"])[0]
    assert row["v:x"] == 5


def test_online_store_matches_the_training_join_at_the_same_time() -> None:
    store = make_store()
    store.materialize("stats", as_of=250)
    online = store.get_online_features(["c1", "c2"], REFS, now=250)
    offline = store.get_historical_features(
        [{"entity_id": "c1", "event_ts": 250}, {"entity_id": "c2", "event_ts": 250}], REFS
    )
    assert online["c1"] == {k: offline[0][k] for k in REFS}
    assert online["c2"] == {k: offline[1][k] for k in REFS}


def test_materialize_is_idempotent_and_never_moves_backwards() -> None:
    store = make_store()
    assert store.materialize("stats", as_of=300) == 2
    store.materialize("stats", as_of=300)
    store.materialize("stats", as_of=150)
    assert store.get_online_features(["c1"], REFS, now=300)["c1"]["stats:orders"] == 3


def test_online_ttl_and_missing_entities() -> None:
    store = make_store(ttl=60)
    store.materialize("stats", as_of=300)
    fresh = store.get_online_features(["c1", "ghost"], REFS, now=330)
    assert fresh["c1"]["stats:orders"] == 3
    assert fresh["ghost"]["stats:orders"] is None
    stale = store.get_online_features(["c1"], REFS, now=500)
    assert stale["c1"]["stats:orders"] is None


def test_ingest_validates_types_and_columns() -> None:
    store = make_store()
    with pytest.raises(ValueError, match="expected int"):
        store.ingest("stats", [{"entity_id": "x", "event_ts": 1, "orders": "many", "basket": 1.0}])
    with pytest.raises(ValueError, match="missing"):
        store.ingest("stats", [{"entity_id": "x", "event_ts": 1, "orders": 1}])
    with pytest.raises(ValueError, match="expected int"):
        store.ingest("stats", [{"entity_id": "x", "event_ts": 1, "orders": True, "basket": 1.0}])


def test_registry_rules() -> None:
    store = make_store()
    with pytest.raises(ValueError, match="different features"):
        store.register(FeatureView("stats", "customer", {"other": FeatureType.STR}))
    existing = store.view("stats")
    assert existing is not None
    store.register(FeatureView("stats", "customer", existing.features, 10.0))
    with pytest.raises(KeyError):
        store.view("nope")
    with pytest.raises(ValueError):
        FeatureView("bad name!", "e", {"x": FeatureType.INT})
    with pytest.raises(ValueError):
        FeatureView("v", "e", {})
    with pytest.raises(ValueError):
        FeatureView("v", "e", {"x": FeatureType.INT}, ttl_seconds=0)


def test_reference_validation() -> None:
    store = make_store()
    with pytest.raises(ValueError, match="view:feature"):
        store.get_online_features(["c1"], ["orders"])
    with pytest.raises(ValueError, match="no feature"):
        store.get_historical_features([{"entity_id": "c1", "event_ts": 1}], ["stats:nope"])
    with pytest.raises(KeyError):
        store.get_online_features(["c1"], ["ghost:x"])


def test_persistence_across_connections(tmp_path: Path) -> None:
    path = str(tmp_path / "fs.db")
    first = FeatureStore(path)
    first.register(FeatureView("v", "e", {"x": FeatureType.INT}))
    first.ingest("v", [{"entity_id": "a", "event_ts": 1, "x": 9}])
    second = FeatureStore(path)
    assert (
        second.get_historical_features([{"entity_id": "a", "event_ts": 5}], ["v:x"])[0]["v:x"] == 9
    )
