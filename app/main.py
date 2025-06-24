"""Command-line pipeline runner."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from .openai_client import get_recommendations

mod_path = Path(__file__).resolve().parent

TOKEN_FILE = Path.home() / ".config/gads-play-optimizer/credentials.json"
ADS_CONFIG_FILE = mod_path.parent / "config/google-ads.yaml"


def export_ads() -> pd.DataFrame:
    """Export campaign metrics from Google Ads.

    Returns a ``DataFrame`` with ``campaign_id``, ``campaign_name``, ``impressions``,
    ``clicks``, ``cost_micros`` and ``conversions`` for the last seven days. The
    Google Ads customer ID must be provided via the ``GOOGLE_ADS_CUSTOMER_ID``
    environment variable.
    """

    customer_id = os.environ["GOOGLE_ADS_CUSTOMER_ID"].replace("-", "")
    client = GoogleAdsClient.load_from_storage(str(ADS_CONFIG_FILE))
    ga_service = client.get_service("GoogleAdsService")
    query = (
        "SELECT campaign.id, campaign.name, metrics.impressions, metrics.clicks, "
        "metrics.cost_micros, metrics.conversions FROM campaign WHERE segments.date "
        "DURING LAST_7_DAYS"
    )
    response = ga_service.search(customer_id=customer_id, query=query)
    rows: list[dict[str, Any]] = []
    for row in response:
        rows.append(
            {
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "impressions": row.metrics.impressions,
                "clicks": row.metrics.clicks,
                "cost_micros": row.metrics.cost_micros,
                "conversions": row.metrics.conversions,
            }
        )
    return pd.DataFrame(rows)


def export_play() -> pd.DataFrame:
    """Export review information from Google Play.

    The Google Play package name must be supplied via the
    ``GOOGLE_PLAY_PACKAGE_NAME`` environment variable. The returned
    ``DataFrame`` contains ``review_id``, ``rating``, ``text`` and ``last_updated``
    to give OpenAI more context while keeping the payload small.
    """

    package_name = os.environ["GOOGLE_PLAY_PACKAGE_NAME"]
    creds = Credentials.from_authorized_user_file(str(TOKEN_FILE))
    service = build("androidpublisher", "v3", credentials=creds)
    result = service.reviews().list(packageName=package_name).execute()
    rows: list[dict[str, Any]] = []
    for review in result.get("reviews", []):
        user_comment = review["comments"][0]["userComment"]
        rating = user_comment.get("starRating")
        rows.append(
            {
                "review_id": review["reviewId"],
                "rating": rating,
                "text": user_comment.get("text"),
                "last_updated": user_comment.get("lastModified", {}).get("seconds"),
            }
        )
    return pd.DataFrame(rows)


def build_compact_json(df_ads: pd.DataFrame, df_play: pd.DataFrame) -> dict:
    """Convert exported data frames into a compact JSON payload."""

    return {
        "google_ads": df_ads.to_dict(orient="records"),
        "google_play": df_play.to_dict(orient="records"),
    }


def store_results(reco: dict, path: Path | None = None) -> None:
    """Store generated recommendations on disk."""

    dest = path or Path("recommendations.json")
    dest.write_text(json.dumps(reco, indent=2))
    print(f"Saved recommendations to {dest}")


def pipeline() -> None:
    """Run the data export and recommendation pipeline."""

    df_ads = export_ads()
    df_play = export_play()
    compact = build_compact_json(df_ads, df_play)
    recommendations = get_recommendations(compact)
    store_results(recommendations.dict())


def main() -> None:
    """Entry point for ``python -m app.main``."""

    pipeline()


if __name__ == "__main__":
    main()
