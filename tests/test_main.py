import json
from pathlib import Path

import pandas as pd

from app import main
from app.schemas import RecommendationResponse


def test_build_compact_json():
    df_ads = pd.DataFrame([{"campaign_id": 1, "impressions": 10}])
    df_play = pd.DataFrame([{"review_id": "a", "rating": 5}])
    result = main.build_compact_json(df_ads, df_play)
    assert result == {
        "google_ads": [{"campaign_id": 1, "impressions": 10}],
        "google_play": [{"review_id": "a", "rating": 5}],
    }


def test_store_results(tmp_path: Path):
    path = tmp_path / "out.json"
    main.store_results({"foo": "bar"}, path=path)
    assert json.loads(path.read_text()) == {"foo": "bar"}


def test_pipeline(monkeypatch):
    def fake_ads():
        return pd.DataFrame([{"a": 1}])

    def fake_play():
        return pd.DataFrame([{"b": 2}])

    called = {}

    def fake_get_reco(_payload: dict):
        called["payload"] = _payload
        return RecommendationResponse(google_ads=["x"], google_play=["y"])

    def fake_store(reco: dict, path=None):
        called["result"] = reco
        called["path"] = path

    monkeypatch.setattr(main, "export_ads", fake_ads)
    monkeypatch.setattr(main, "export_play", fake_play)
    monkeypatch.setattr(main, "get_recommendations", fake_get_reco)
    monkeypatch.setattr(main, "store_results", fake_store)

    main.pipeline()

    assert called["payload"] == {
        "google_ads": [{"a": 1}],
        "google_play": [{"b": 2}],
    }
    assert called["result"] == {"google_ads": ["x"], "google_play": ["y"]}
