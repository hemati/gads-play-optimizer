# -*- coding: utf-8 -*-
"""Ads‑only pipeline with 14‑day blocks (last 60 days).

Erstellt **zeitliche Verlaufs‑Serien pro Asset**, sodass GPT Trends klar
ablesen kann (Block 0 = ältester, Block 3 = jüngster):

* `google_ads_campaigns_blocks` – unverändert flache KPI‑Records.
* `google_ads_assets_time_series` – je Asset eine Liste von 14‑Tage‑Blöcken in
  chronologischer Reihenfolge.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import date, timedelta
from typing import List, Dict, Any

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient

from .openai_client import get_recommendations

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
mod_path = Path(__file__).resolve().parent
ADS_CONFIG_FILE = mod_path.parent / "config/google-ads.yaml"
CUSTOMER_ID = os.environ["GOOGLE_ADS_CUSTOMER_ID"].replace("-", "")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _ga_client() -> GoogleAdsClient:
    return GoogleAdsClient.load_from_storage(str(ADS_CONFIG_FILE))


def _date_blocks(block_len: int = 14, total_days: int = 60) -> List[tuple[date, date]]:
    """Return (start, end) tuples, oldest → newest."""
    end = pd.Timestamp.utcnow().normalize().date()
    blocks = [
        (
            end - timedelta(days=total_days - i * block_len),
            end - timedelta(days=total_days - (i + 1) * block_len - 1),
        )
        for i in range(total_days // block_len)
    ]
    return blocks  # oldest first


def _active_campaign_ids() -> set[int]:
    client = _ga_client()
    ga_service = client.get_service("GoogleAdsService")
    query = "SELECT campaign.id FROM campaign WHERE campaign.status = 'ENABLED'"
    return {row.campaign.id for row in ga_service.search(customer_id=CUSTOMER_ID, query=query)}

# -----------------------------------------------------------------------------
# 1. Campaign KPIs per block
# -----------------------------------------------------------------------------

def export_ads_blocks(blocks: List[tuple[date, date]], campaign_ids: set[int]) -> pd.DataFrame:
    client = _ga_client()
    ga_service = client.get_service("GoogleAdsService")
    rows: List[Dict[str, Any]] = []
    id_list = ",".join(str(i) for i in campaign_ids)

    for idx, (start, end) in enumerate(blocks):
        query = f"""
            SELECT
              campaign.id,
              campaign.name,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.conversions,
              metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{start}' AND '{end}'
              AND campaign.id IN ({id_list})
        """
        for r in ga_service.search(customer_id=CUSTOMER_ID, query=query):
            rows.append({
                "campaign_id": r.campaign.id,
                "campaign_name": r.campaign.name,
                "impressions": r.metrics.impressions,
                "clicks": r.metrics.clicks,
                "cost_micros": r.metrics.cost_micros,
                "conversions": r.metrics.conversions,
                "conv_value": r.metrics.conversions_value,
                "block_index": idx,  # 0 = oldest
                "block_start": str(start),
                "block_end": str(end),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    u = 1_000_000
    df["cost"] = df.cost_micros / u
    df["cpa"] = df.cost / df.conversions.replace({0: pd.NA})
    df["roas"] = df.conv_value / df.cost.replace({0: pd.NA})
    return df

# -----------------------------------------------------------------------------
# 2. Asset KPIs per block (include disabled / removed)
# -----------------------------------------------------------------------------

def export_asset_blocks(blocks: List[tuple[date, date]], campaign_ids: set[int]) -> pd.DataFrame:
    client = _ga_client()
    ga_service = client.get_service("GoogleAdsService")
    id_list = ",".join(str(i) for i in campaign_ids)
    rows: List[Dict[str, Any]] = []

    for idx, (start, end) in enumerate(blocks):
        query = f"""
            SELECT
              campaign.id,
              campaign.name,
              ad_group.id,
              ad_group.name,
              asset.id,
              asset.name,
              asset.type,
              ad_group_ad_asset_view.field_type,
--               ad_group_ad_asset_view.performance_label,
              ad_group_ad.status,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros,
              metrics.all_conversions
            FROM ad_group_ad_asset_view
            WHERE segments.date BETWEEN '{start}' AND '{end}'
              AND campaign.id IN ({id_list})
        """
        for batch in ga_service.search_stream(customer_id=CUSTOMER_ID, query=query):
            for r in batch.results:
                rows.append({
                    "campaign_id": int(r.campaign.id),
                    "campaign_name": r.campaign.name,
                    "asset_id": r.asset.id,
                    "asset_name": r.asset.name,
                    "asset_type": r.asset.type.name,
                    "field_type": r.ad_group_ad_asset_view.field_type.name,
                    # "label": r.ad_group_ad_asset_view.performance_label.name,
                    "ad_status": r.ad_group_ad.status.name,
                    "impr": r.metrics.impressions,
                    "clicks": r.metrics.clicks,
                    "cost_micros": r.metrics.cost_micros,
                    "installs": r.metrics.all_conversions,
                    "block_index": idx,
                    "block_start": str(start),
                    "block_end": str(end),
                })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    u = 1_000_000
    df["cost"] = df.cost_micros / u
    df["ctr"] = df.clicks / df.impr.replace({0: pd.NA})
    df["cpi"] = df.cost / df.installs.replace({0: pd.NA})
    return df

# -----------------------------------------------------------------------------
# 3. Convert asset blocks -> time‑series structure
# -----------------------------------------------------------------------------

def _asset_time_series(df_assets: pd.DataFrame) -> List[Dict[str, Any]]:
    series: List[Dict[str, Any]] = []
    if df_assets.empty:
        return series

    metrics_cols = [
        "block_index",
        "block_start",
        "block_end",
        "impr",
        "clicks",
        "cost",
        "ctr",
        "cpi",
        "installs",
        # "label",
        "ad_status",
    ]

    for (aid, aname, atype, ftype), grp in df_assets.groupby(
        ["asset_id", "asset_name", "asset_type", "field_type"], sort=False
    ):
        grp_sorted = grp.sort_values("block_index")
        ts = grp_sorted[metrics_cols].to_dict("records")
        series.append({
            "asset_id": int(aid),
            "asset_name": aname,
            "asset_type": atype,
            "field_type": ftype,
            "time_series": ts,
        })
    return series

# -----------------------------------------------------------------------------
# 4. Build payload & store
# -----------------------------------------------------------------------------

def build_payload(blocks: List[tuple[date, date]], df_camp: pd.DataFrame, df_assets: pd.DataFrame) -> dict:
    return {
        "meta": {
            "block_length_days": 14,
            "blocks": [
                {"index": i, "start": str(s), "end": str(e)}
                for i, (s, e) in enumerate(blocks)
            ],
        },
        "google_ads_campaigns_blocks": df_camp.to_dict("records"),
        "google_ads_assets_time_series": _asset_time_series(df_assets),
    }


def store_results(reco: dict, path: Path | None = None) -> None:
    dest = path or Path("recommendations.json")
    dest.write_text(json.dumps(reco, indent=2))
    print(f"Saved recommendations to {dest}")

# -----------------------------------------------------------------------------
# 5. Pipeline orchestration
# -----------------------------------------------------------------------------

def pipeline() -> None:
    campaign_ids = _active_campaign_ids()
    if not campaign_ids:
        print("No active campaigns found – exiting.")
        return

    blocks = _date_blocks(total_days=14)
    df_camp = export_ads_blocks(blocks, campaign_ids)
    df_assets = export_asset_blocks(blocks, campaign_ids)

    payload = build_payload(blocks, df_camp, df_assets)
    recos = get_recommendations(payload)
    store_results(recos.dict())

# -----------------------------------------------------------------------------
# 6. CLI entry point
# -----------------------------------------------------------------------------

def main() -> None:
    pipeline()


if __name__ == "__main__":
    main()
