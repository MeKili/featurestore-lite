"""FastAPI layer: register views, ingest history, materialise and serve online features."""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from featurestore import __version__
from featurestore.schema import FeatureType, FeatureView
from featurestore.store import FeatureStore


class ViewIn(BaseModel):
    name: str
    entity: str
    features: dict[str, FeatureType]
    ttl_seconds: float | None = None


class IngestIn(BaseModel):
    rows: list[dict[str, Any]] = Field(min_length=1)


class MaterializeIn(BaseModel):
    as_of: str | None = None


class OnlineIn(BaseModel):
    entity_ids: list[str] = Field(min_length=1)
    features: list[str] = Field(min_length=1)
    now: str | None = None


class HistoricalIn(BaseModel):
    entity_rows: list[dict[str, Any]] = Field(min_length=1)
    features: list[str] = Field(min_length=1)


def create_app(store: FeatureStore | None = None) -> FastAPI:
    fs = store or FeatureStore(os.environ.get("FEATURESTORE_DB", ":memory:"))
    app = FastAPI(title="featurestore-lite", version=__version__)

    def guard(call: Any) -> Any:
        try:
            return call()
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/views")
    def list_views() -> list[dict[str, Any]]:
        return [v.to_dict() for v in fs.views()]

    @app.put("/views")
    def register(body: ViewIn) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            view = FeatureView(body.name, body.entity, body.features, body.ttl_seconds)
            fs.register(view)
            return view.to_dict()

        return dict(guard(run))

    @app.post("/views/{name}/ingest")
    def ingest(name: str, body: IngestIn) -> dict[str, int]:
        return {"ingested": int(guard(lambda: fs.ingest(name, body.rows)))}

    @app.post("/views/{name}/materialize")
    def materialize(name: str, body: MaterializeIn) -> dict[str, int]:
        return {"entities": int(guard(lambda: fs.materialize(name, body.as_of)))}

    @app.post("/features/online")
    def online(body: OnlineIn) -> dict[str, dict[str, Any]]:
        result = guard(lambda: fs.get_online_features(body.entity_ids, body.features, body.now))
        return dict(result)

    @app.post("/features/historical")
    def historical(body: HistoricalIn) -> list[dict[str, Any]]:
        result = guard(lambda: fs.get_historical_features(body.entity_rows, body.features))
        return list(result)

    return app


app = create_app()
