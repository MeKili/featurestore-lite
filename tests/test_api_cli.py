import pytest
from fastapi.testclient import TestClient

from featurestore.api import create_app
from featurestore.cli import main
from featurestore.store import FeatureStore


def test_api_end_to_end() -> None:
    client = TestClient(create_app(FeatureStore()))
    assert client.get("/health").json()["status"] == "ok"

    created = client.put(
        "/views",
        json={
            "name": "stats",
            "entity": "customer",
            "features": {"orders": "int"},
            "ttl_seconds": 100,
        },
    )
    assert created.status_code == 200
    assert client.get("/views").json()[0]["name"] == "stats"

    rows = [
        {"entity_id": "c1", "event_ts": 100, "orders": 1},
        {"entity_id": "c1", "event_ts": 200, "orders": 5},
    ]
    assert client.post("/views/stats/ingest", json={"rows": rows}).json() == {"ingested": 2}

    historical = client.post(
        "/features/historical",
        json={"entity_rows": [{"entity_id": "c1", "event_ts": 150}], "features": ["stats:orders"]},
    )
    assert historical.json()[0]["stats:orders"] == 1

    mat = client.post("/views/stats/materialize", json={"as_of": "1970-01-01T00:03:20+00:00"})
    assert mat.json() == {"entities": 1}
    online = client.post(
        "/features/online",
        json={
            "entity_ids": ["c1"],
            "features": ["stats:orders"],
            "now": "1970-01-01T00:03:30+00:00",
        },
    )
    assert online.json()["c1"]["stats:orders"] == 5


def test_api_errors() -> None:
    client = TestClient(create_app(FeatureStore()))
    assert (
        client.post(
            "/views/ghost/ingest", json={"rows": [{"entity_id": "a", "event_ts": 1}]}
        ).status_code
        == 404
    )
    client.put("/views", json={"name": "v", "entity": "e", "features": {"x": "int"}})
    bad = client.post(
        "/views/v/ingest", json={"rows": [{"entity_id": "a", "event_ts": 1, "x": "no"}]}
    )
    assert bad.status_code == 422
    assert (
        client.post("/features/online", json={"entity_ids": ["a"], "features": ["x"]}).status_code
        == 422
    )


def test_cli_demo_shows_point_in_time_row(capsys: pytest.CaptureFixture[str]) -> None:
    assert main() == 0
    out = capsys.readouterr().out
    assert "'customer_stats:orders_30d': 4" in out
    assert "'customer_stats:orders_30d': 9" in out
