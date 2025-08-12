# -*- coding: utf-8 -*-
"""
Ads‑only pipeline with 14‑day blocks (last 60 days).

Erstellt **zeitliche Verlaufs‑Serien pro Asset**, sodass GPT Trends klar
ablesen kann (Block 0 = ältester, Block n = jüngster). Zusätzlich:
- Konto‑Meta (Zeitzone, Währung) für korrekte Datumsfenster & Formatierung.
- Asset‑Details inkl. Typ (text/image/video) und Preview (Text, Image‑URL, YouTube‑ID).
- Kampagnen‑Benchmarks (CTR, CPI) im jüngsten Block.
- Post‑Processing der LLM‑Antwort: von Pipe‑Strings -> strukturierte Empfehlungen
  mit sauberem Locator (campaign/ad_group/asset) und `kind`.

Benötigt:
- google-ads (GoogleAdsClient)
- pandas
- Python 3.9+ (für zoneinfo)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

from zoneinfo import ZoneInfo

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


def _safe_get(obj: Any, path: str, default: Any = None) -> Any:
    """Robustes getattr über Pfad 'a.b.c'. Gibt default zurück, wenn irgendwas fehlt."""
    cur = obj
    try:
        for part in path.split("."):
            if cur is None:
                return default
            cur = getattr(cur, part)
        return cur if cur is not None else default
    except Exception:
        return default


def _account_meta() -> dict:
    """Name, Zeitzone und Währung des Kontos (für Meta & korrekte Datumsblöcke)."""
    client = _ga_client()
    ga_service = client.get_service("GoogleAdsService")
    query = (
        "SELECT customer.descriptive_name, customer.time_zone, customer.currency_code "
        "FROM customer"
    )
    row = next(iter(ga_service.search(customer_id=CUSTOMER_ID, query=query)))
    return {
        "account_id": CUSTOMER_ID,
        "account_name": row.customer.descriptive_name,
        "time_zone": row.customer.time_zone,
        "currency_code": row.customer.currency_code,
    }


def _date_blocks(
    block_len: int = 14,
    total_days: int = 60,
    tz_str: str = "UTC",
) -> List[tuple[date, date]]:
    """
    Liefert (start, end) inklusiv, älteste → neueste.
    Anker ist 'gestern' in Konto‑TZ (vermeidet halbfertigen heutigen Tag).
    """
    try:
        tz = ZoneInfo(tz_str)
    except Exception:
        tz = timezone.utc

    now_local = datetime.now(timezone.utc).astimezone(tz)
    anchor = now_local.date() - timedelta(days=1)  # gestern in Konto‑TZ
    n_blocks = max(1, total_days // block_len)

    blocks: List[Tuple[date, date]] = []
    end = anchor
    for _ in range(n_blocks):
        start = end - timedelta(days=block_len - 1)
        blocks.append((start, end))
        end = start - timedelta(days=1)

    blocks.reverse()  # älteste zuerst
    return blocks


def _active_campaign_ids() -> set[int]:
    client = _ga_client()
    ga_service = client.get_service("GoogleAdsService")
    query = "SELECT campaign.id FROM campaign WHERE campaign.status = 'ENABLED'"
    return {int(row.campaign.id) for row in ga_service.search(customer_id=CUSTOMER_ID, query=query)}

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
                "campaign_id": int(r.campaign.id),
                "campaign_name": r.campaign.name,
                "impressions": int(r.metrics.impressions or 0),
                "clicks": int(r.metrics.clicks or 0),
                "cost_micros": int(r.metrics.cost_micros or 0),
                "conversions": float(r.metrics.conversions or 0.0),
                "conv_value": float(r.metrics.conversions_value or 0.0),
                "block_index": idx,  # 0 = ältester
                "block_start": str(start),
                "block_end": str(end),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    u = 1_000_000
    df["cost"] = df.cost_micros / u
    df["ctr"] = df.clicks / df.impressions.replace({0: pd.NA})
    # CPA/ROAS auf Kampagnenebene (nicht zwingend identisch zu App‑Installs)
    df["cpa"] = df.cost / df.conversions.replace({0: pd.NA})
    df["roas"] = df.conv_value / df.cost.replace({0: pd.NA})
    return df

# -----------------------------------------------------------------------------
# 2. Asset KPIs per block (inkl. disabled/removed + Content‑Felder)
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
              ad_group_ad.resource_name,
              asset.resource_name,
              asset.id,
              asset.name,
              asset.type,
              ad_group_ad_asset_view.field_type,
              ad_group_ad_asset_view.performance_label,
              ad_group_ad.status,
              asset.text_asset.text,
              asset.image_asset.full_size.url,
              asset.image_asset.full_size.width_pixels,
              asset.image_asset.full_size.height_pixels,
              asset.youtube_video_asset.youtube_video_id,
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
                    "ad_group_id": int(r.ad_group.id),
                    "ad_group_name": r.ad_group.name,
                    "ad_group_ad_resource": r.ad_group_ad.resource_name,
                    "asset_resource": r.asset.resource_name,
                    "asset_id": int(r.asset.id),
                    "asset_name": r.asset.name,
                    "asset_type": r.asset.type.name,  # TEXT_ASSET / IMAGE / YOUTUBE_VIDEO ...
                    "field_type": r.ad_group_ad_asset_view.field_type.name,  # HEADLINE / DESCRIPTION / MARKETING_IMAGE ...
                    "performance_label": (
                        r.ad_group_ad_asset_view.performance_label.name
                        if r.ad_group_ad_asset_view.performance_label else None
                    ),
                    "ad_status": r.ad_group_ad.status.name,
                    "text": _safe_get(r, "asset.text_asset.text"),
                    "image_url": _safe_get(r, "asset.image_asset.full_size.url"),
                    "image_w": _safe_get(r, "asset.image_asset.full_size.width_pixels"),
                    "image_h": _safe_get(r, "asset.image_asset.full_size.height_pixels"),
                    "youtube_id": _safe_get(r, "asset.youtube_video_asset.youtube_video_id"),
                    "impr": int(_safe_get(r, "metrics.impressions", 0)),
                    "clicks": int(_safe_get(r, "metrics.clicks", 0)),
                    "cost_micros": int(_safe_get(r, "metrics.cost_micros", 0)),
                    "installs": float(_safe_get(r, "metrics.all_conversions", 0.0)),
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

def _kind(asset_type: str) -> str:
    at = (asset_type or "").upper()
    if at in ("TEXT_ASSET", "TEXT"):
        return "text"
    if at in ("IMAGE", "IMAGE_ASSET"):
        return "image"
    if at in ("YOUTUBE_VIDEO", "VIDEO"):
        return "video"
    return at.lower() or "unknown"


def _last_not_null(series: pd.Series) -> Optional[Any]:
    """Letzter nicht‑Null Wert (für Preview etc.)."""
    non_null = series.dropna()
    return non_null.iloc[-1] if not non_null.empty else None


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
        "ad_status",
        "performance_label",
    ]

    # Gruppierung: pro (Campaign, AdGroup, Asset, FieldType) eine Serie
    group_cols = [
        "campaign_id", "campaign_name",
        "ad_group_id", "ad_group_name",
        "asset_id", "asset_name", "asset_type", "field_type",
        "asset_resource", "ad_group_ad_resource",
    ]

    for keys, grp in df_assets.groupby(group_cols, sort=False):
        grp_sorted = grp.sort_values("block_index")

        # Preview & Basics
        atype = keys[6]  # asset_type
        kind = _kind(atype)

        preview: Dict[str, Any] = {}
        if kind == "text":
            preview["text"] = _last_not_null(grp_sorted["text"])
        elif kind == "image":
            preview["image_url"] = _last_not_null(grp_sorted["image_url"])
            preview["width"] = _last_not_null(grp_sorted["image_w"])
            preview["height"] = _last_not_null(grp_sorted["image_h"])
        elif kind == "video":
            preview["youtube_id"] = _last_not_null(grp_sorted["youtube_id"])

        ts = grp_sorted[metrics_cols].to_dict("records")

        item = {
            "campaign_id": int(keys[0]),
            "campaign_name": keys[1],
            "ad_group_id": int(keys[2]),
            "ad_group_name": keys[3],
            "asset_id": int(keys[4]),
            "asset_name": keys[5],
            "asset_type": atype,
            "field_type": keys[7],
            "asset_resource": keys[8],
            "ad_group_ad_resource": keys[9],
            "kind": kind,
            "preview": preview,
            "time_series": ts,
        }
        series.append(item)

    return series

# -----------------------------------------------------------------------------
# 4. Benchmarks & Payload
# -----------------------------------------------------------------------------

def _campaign_benchmarks(df_camp: pd.DataFrame, df_assets: pd.DataFrame) -> Dict[int, dict]:
    """Jüngster Block: CTR aus Kampagnen‑Report, CPI aus Asset‑Installs."""
    out: Dict[int, dict] = {}
    if df_camp.empty:
        return out

    last_idx = int(df_camp["block_index"].max())

    # CTR/Cost/Clicks/Impr aus Campaign‑DF
    last_c = df_camp.loc[df_camp.block_index == last_idx]

    # CPI (Cost / Installs) aus Asset‑DF (gleicher Block)
    if not df_assets.empty and "block_index" in df_assets:
        last_a = df_assets.loc[df_assets.block_index == last_idx]
    else:
        last_a = pd.DataFrame(columns=["campaign_id", "cost", "installs"])

    # Aggregation
    ctr_by_camp = (
        last_c.groupby("campaign_id")
        .agg(impressions=("impressions", "sum"), clicks=("clicks", "sum"), cost=("cost", "sum"))
        .reset_index()
    )
    inst_by_camp = (
        last_a.groupby("campaign_id")
        .agg(installs=("installs", "sum"))
        .reset_index()
        if not last_a.empty else pd.DataFrame({"campaign_id": [], "installs": []})
    )

    merged = pd.merge(ctr_by_camp, inst_by_camp, on="campaign_id", how="left")
    merged["installs"] = merged["installs"].fillna(0.0)

    for _, r in merged.iterrows():
        ctr = (r["clicks"] / r["impressions"]) if r["impressions"] else None
        cpi = (r["cost"] / r["installs"]) if r["installs"] else None
        out[int(r["campaign_id"])] = {
            "ctr": float(ctr) if ctr is not None else None,
            "cpi": float(cpi) if cpi is not None else None,
        }
    return out


def build_payload(
    blocks: List[tuple[date, date]],
    df_camp: pd.DataFrame,
    df_assets: pd.DataFrame,
    account: dict,
) -> dict:
    block_meta = [
        {"index": i, "start": str(s), "end": str(e)}
        for i, (s, e) in enumerate(blocks)
    ]
    payload = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "account": {
                "id": account.get("account_id"),
                "name": account.get("account_name"),
                "time_zone": account.get("time_zone"),
                "currency": account.get("currency_code"),
            },
            "block_length_days": (blocks[0][1] - blocks[0][0]).days + 1 if blocks else 14,
            "blocks": block_meta,
            "benchmarks": {
                "campaign": _campaign_benchmarks(df_camp, df_assets)
            },
        },
        "google_ads_campaigns_blocks": df_camp.to_dict("records"),
        "google_ads_assets_time_series": _asset_time_series(df_assets),
    }
    return payload

# -----------------------------------------------------------------------------
# 5. LLM-Output anreichern (Pipe‑Strings -> strukturierte Empfehlungen)
# -----------------------------------------------------------------------------

def _index_assets_by_id(series_list: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """asset_id -> Liste von Vorkommen (falls Asset in mehreren AdGroups/Camps hängt)."""
    idx: Dict[int, List[Dict[str, Any]]] = {}
    for s in series_list:
        idx.setdefault(int(s["asset_id"]), []).append(s)
    return idx


def _pick_asset_occurrence(
    candidates: List[Dict[str, Any]],
    campaign_name_hint: Optional[str],
) -> Dict[str, Any]:
    """Wählt das passendste Vorkommen per Kampagnen‑Namenshinweis, sonst erstes."""
    if not candidates:
        return {}
    if campaign_name_hint:
        for c in candidates:
            if c.get("campaign_name") == campaign_name_hint:
                return c
    return candidates[0]


def _parse_pipe_line(line: str) -> Dict[str, Any]:
    """
    Erwartetes Format (Beispiel):
    226330626500|BibleGPT - BR - PT|ACTION=scale|WHY=CPI 0.10 € << campaign avg 1.71 and CTR 4.55%|SUGGEST=Pin...
    """
    parts = [p.strip() for p in (line or "").split("|")]
    parsed: Dict[str, Any] = {"raw": line, "id": None, "campaign_name": None, "action": None, "why": None, "suggest": None}

    if parts:
        # id versuchen
        try:
            parsed["id"] = int(parts[0])
        except Exception:
            parsed["id"] = None
    if len(parts) > 1:
        parsed["campaign_name"] = parts[1] or None
    # Restliche Schlüssel extrahieren
    for p in parts[2:]:
        if p.startswith("ACTION="):
            parsed["action"] = p.replace("ACTION=", "").strip()
        elif p.startswith("WHY="):
            parsed["why"] = p.replace("WHY=", "").strip()
        elif p.startswith("SUGGEST="):
            parsed["suggest"] = p.replace("SUGGEST=", "").strip()
    return parsed


def _last_block_index(meta_blocks: List[Dict[str, Any]]) -> int:
    return int(meta_blocks[-1]["index"]) if meta_blocks else 0


def _metrics_from_asset_series(asset_series: Dict[str, Any], block_index: int) -> Dict[str, Any]:
    """Zieht Metriken des gewünschten Blocks (oder letzten vorhandenen)."""
    ts = asset_series.get("time_series", []) or []
    chosen = None
    for r in ts:
        if int(r.get("block_index", -1)) == block_index:
            chosen = r
            break
    if chosen is None and ts:
        chosen = ts[-1]
    if not chosen:
        return {}
    return {
        "block_index": int(chosen.get("block_index")),
        "impr": int(chosen.get("impr") or 0),
        "clicks": int(chosen.get("clicks") or 0),
        "installs": float(chosen.get("installs") or 0.0),
        "cost": float(chosen.get("cost") or 0.0),
        "ctr": float(chosen.get("ctr")) if chosen.get("ctr") is not None else None,
        "cpi": float(chosen.get("cpi")) if chosen.get("cpi") is not None else None,
    }


def _priority_from_action(action: Optional[str]) -> int:
    a = (action or "").lower()
    if a == "scale":
        return 5
    if a == "pause":
        return 4
    if a in ("create_variation", "replace"):
        return 3
    return 2


def enrich_recommendations(raw_recos: dict, payload: dict) -> dict:
    """
    Nimmt die (evtl. string‑basierten) LLM‑Empfehlungen und baut eine saubere,
    eindeutig zuordenbare Struktur inkl. `kind`/Preview & Metriken.
    """
    series_list = payload.get("google_ads_assets_time_series", []) or []
    assets_by_id = _index_assets_by_id(series_list)

    # Kampagnen‑Map (Name/ID)
    camp_blocks = payload.get("google_ads_campaigns_blocks", []) or []
    camp_name_by_id: Dict[int, str] = {}
    for r in camp_blocks:
        camp_name_by_id[int(r["campaign_id"])] = r.get("campaign_name")

    # Benchmarks
    benchmarks = (payload.get("meta", {}).get("benchmarks", {}) or {}).get("campaign", {}) or {}
    last_idx = _last_block_index(payload.get("meta", {}).get("blocks", []) or [])

    # Input normalisieren
    if hasattr(raw_recos, "dict"):
        raw_recos = raw_recos.dict()
    raw_ads = raw_recos.get("google_ads", []) or []
    raw_play = raw_recos.get("google_play", []) or []

    structured: List[Dict[str, Any]] = []

    for line in raw_ads:
        parsed = _parse_pipe_line(line)
        rid = parsed.get("id")
        cname_hint = parsed.get("campaign_name")
        action = parsed.get("action")
        why = parsed.get("why")
        suggest = parsed.get("suggest")

        entity: Dict[str, Any] = {}
        level = "asset"
        metrics: Dict[str, Any] = {}

        # Primär: Asset‑ID matchen
        asset_occ = _pick_asset_occurrence(assets_by_id.get(rid, []), cname_hint)
        if asset_occ:
            camp_id = int(asset_occ["campaign_id"])
            entity = {
                "customer_id": CUSTOMER_ID,
                "campaign_id": camp_id,
                "campaign_name": asset_occ.get("campaign_name"),
                "ad_group_id": int(asset_occ["ad_group_id"]),
                "ad_group_name": asset_occ.get("ad_group_name"),
                "asset_id": int(asset_occ["asset_id"]),
                "asset_resource": asset_occ.get("asset_resource"),
                "ad_group_ad_resource": asset_occ.get("ad_group_ad_resource"),
                "kind": asset_occ.get("kind"),
                "field_type": asset_occ.get("field_type"),
                "preview": asset_occ.get("preview"),
            }
            metrics = _metrics_from_asset_series(asset_occ, last_idx)
            metrics["benchmarks"] = benchmarks.get(camp_id)
        else:
            # Fallback: vielleicht ist die ID eine Kampagnen‑ID → Kampagnen‑Level
            level = "campaign"
            camp_name = camp_name_by_id.get(rid) or cname_hint
            entity = {
                "customer_id": CUSTOMER_ID,
                "campaign_id": rid,
                "campaign_name": camp_name,
            }
            # Kampagnen‑Benchmarks direkt anhängen
            metrics["benchmarks"] = benchmarks.get(int(rid)) if rid is not None else None

        structured.append({
            "id": f"rec_{rid}_{last_idx}" if rid is not None else f"rec_{len(structured)}",
            "level": level,
            "entity": entity,
            "metrics": metrics or None,
            "action": {
                "type": action,
                "parameters": None,
                "priority": _priority_from_action(action),
            },
            "rationale_short": why,
            "suggestion": suggest,
            "raw": parsed.get("raw"),
        })

    final_obj = {
        "meta": payload.get("meta", {}),
        "recommendations_structured": structured,
        "llm_raw": raw_recos,            # zur Nachvollziehbarkeit
        "google_play": raw_play or [],
    }
    return final_obj

# -----------------------------------------------------------------------------
# 6. Persistenz
# -----------------------------------------------------------------------------

def store_results(obj: dict, path: Path | None = None) -> None:
    dest = path or Path("recommendations.json")
    dest.write_text(json.dumps(obj, indent=2, ensure_ascii=False))
    print(f"Saved recommendations to {dest.resolve()}")


# -----------------------------------------------------------------------------
# 7. Pipeline orchestration
# -----------------------------------------------------------------------------

def pipeline() -> None:
    # 0) Meta
    account = _account_meta()

    # 1) IDs
    campaign_ids = _active_campaign_ids()
    if not campaign_ids:
        print("No active campaigns found – exiting.")
        return

    # 2) Blöcke (14‑Tage, 60 Tage zurück)
    blocks = _date_blocks(block_len=14, total_days=14, tz_str=account["time_zone"])

    # 3) Exporte
    df_camp = export_ads_blocks(blocks, campaign_ids)
    df_assets = export_asset_blocks(blocks, campaign_ids)

    # 4) Payload bauen
    payload = build_payload(blocks, df_camp, df_assets, account)

    # 5) LLM‑Empfehlungen
    recos = get_recommendations(payload)

    # 6) Anreichern und speichern
    final = enrich_recommendations(recos, payload)
    store_results(final)


# -----------------------------------------------------------------------------
# 8. CLI entry point
# -----------------------------------------------------------------------------

def main() -> None:
    pipeline()


if __name__ == "__main__":
    main()
