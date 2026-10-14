"""``featurestore demo``: show why point-in-time joins matter, using a tiny in-memory store."""

from __future__ import annotations

from collections.abc import Sequence

from featurestore.schema import FeatureType, FeatureView
from featurestore.store import FeatureStore


def main(argv: Sequence[str] | None = None) -> int:
    store = FeatureStore()
    store.register(
        FeatureView(
            "customer_stats",
            "customer",
            {"orders_30d": FeatureType.INT, "avg_basket": FeatureType.FLOAT},
            ttl_seconds=45 * 86400,
        )
    )
    store.ingest(
        "customer_stats",
        [
            {"entity_id": "c1", "event_ts": "2026-01-01", "orders_30d": 1, "avg_basket": 20.0},
            {"entity_id": "c1", "event_ts": "2026-02-01", "orders_30d": 4, "avg_basket": 35.0},
            {"entity_id": "c1", "event_ts": "2026-03-01", "orders_30d": 9, "avg_basket": 60.0},
        ],
    )
    refs = ["customer_stats:orders_30d", "customer_stats:avg_basket"]
    labels = [{"entity_id": "c1", "event_ts": "2026-02-15"}]
    print("training row as of 2026-02-15 (point in time, no leakage):")
    print(store.get_historical_features(labels, refs)[0])

    store.materialize("customer_stats", "2026-03-02")
    print("\nonline lookup on 2026-03-02:")
    print(store.get_online_features(["c1"], refs, now="2026-03-02")["c1"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
