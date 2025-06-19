"""Command-line pipeline runner."""

from __future__ import annotations

import pandas as pd

from .openai_client import get_recommendations


def export_ads() -> pd.DataFrame:
    return pd.DataFrame()


def export_play() -> pd.DataFrame:
    return pd.DataFrame()


def build_compact_json(df_ads: pd.DataFrame, df_play: pd.DataFrame) -> dict:
    return {}


def store_results(reco: dict) -> None:
    print(reco)


def pipeline() -> None:
    df_ads = export_ads()
    df_play = export_play()
    compact = build_compact_json(df_ads, df_play)
    recommendations = get_recommendations(compact)
    store_results(recommendations.dict())


def main() -> None:
    pipeline()


if __name__ == "__main__":
    main()
