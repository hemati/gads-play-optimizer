# -*- coding: utf-8 -*-
"""OpenAI client – concrete, asset‑level recommendations (optimised).

Änderungen / Highlights
───────────────────────
- System‑Prompt nutzt jetzt die Felder aus deinem Payload (z. B. meta.benchmarks,
  asset.kind/field_type) und setzt klare Guardrails:
  * Nur Asset‑IDs verwenden, die im Payload vorkommen
  * 5–15 Empfehlungen, jeweils genau EINE Aktion pro Asset
  * WHY/SUGGEST dürfen KEINE Pipe-Zeichen enthalten (ersatzweise „∥“)
  * Volumen‑Guards (min. Impressions/Installs) + Trendbezug auf letzte 2 Blöcke
- Function‑Call erzwungen (function_call={"name": "recommend_actions"}) + JSON‑Format.
- Robustere Retry‑Logik, Temperature/Top‑p/Seed für stabilere Antworten.
- Post‑Sanitizing:
  * filtert ungültige Zeilen, Asset‑IDs außerhalb des Payloads
  * dedupliziert pro Asset‑ID (bevorzugt SCALE/PAUSE > CREATE_VARIATION/REPLACE)
  * begrenzt auf 5–15 Einträge (Impact‑basiert sortiert)
- Fallback‑Parser: falls kein function_call, wird content‑JSON gelesen.
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from typing import Any
from typing import Dict, List, Optional, Set, Tuple

import openai
from dotenv import load_dotenv

from .schemas import RecommendationResponse

try:
    import numpy as np
except Exception:
    np = None  # falls numpy nicht installiert ist
import pandas as pd

# ---------------------------------------------------------------------------
# Environment & client setup
# ---------------------------------------------------------------------------
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY", "test")

_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "o3")
_OPENAI_REQ_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))
_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRY", "3"))
_OPENAI_SEED = os.getenv("OPENAI_SEED")
if _OPENAI_SEED is not None:
    try:
        _OPENAI_SEED = int(_OPENAI_SEED)  # type: ignore[assignment]
    except Exception:
        _OPENAI_SEED = None  # type: ignore[assignment]

ALLOWED_ACTIONS = {"scale", "pause", "replace", "create_variation"}

# ---------------------------------------------------------------------------
# Function‑calling schema (google_play optional)
# ---------------------------------------------------------------------------
FUNCTION_SCHEMA: Dict[str, Any] = {
    "name": "recommend_actions",
    "description": "Return concrete optimisation steps.",
    "parameters": {
        "type": "object",
        "properties": {
            "google_ads": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Action lines in the format "
                    "<asset_id>|<campaign_name>|ACTION=<scale|pause|replace|create_variation>"
                    "|WHY=<metric_reason>|SUGGEST=<next_step>. "
                    "Do NOT use '|' inside WHY or SUGGEST; use '∥' instead."
                ),
            },
            "google_play": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional ASO suggestions (plain sentences, max 3).",
            },
        },
        "required": ["google_ads"],
        "additionalProperties": False,
    },
}

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are a senior performance‑marketing strategist specialised in Google Ads for app installs. "
    "You are given a JSON payload with:\n"
    "- account meta (time_zone, currency), 14‑day blocks, and campaign benchmarks under meta.benchmarks.campaign,\n"
    "- asset‑level time series including asset_id, campaign_name, ad_group, field_type, kind (text|image|video), and preview.\n\n"
    "Task:\n"
    "For the most recent block (and trend vs. previous block), select 5–15 assets with actionable insights. "
    "For each selected asset produce exactly ONE action among: scale, pause, replace, create_variation. "
    "Base decisions on CTR/CPI trends, spend and installs; use provided campaign benchmarks when available.\n\n"
    "STRICT OUTPUT FORMAT (no extra text):\n"
    "<asset_id>|<campaign_name>|ACTION=<scale|pause|replace|create_variation>|WHY=<short reason>|SUGGEST=<next step>\n"
    "Rules:\n"
    "1) Use only asset_id values that exist in the payload. 2) Never include duplicate asset_ids. "
    "3) WHY and SUGGEST MUST NOT contain the '|' character. If needed, replace '|' with '∥'. "
    "4) Keep WHY focused on metrics (e.g., 'CPI 0.10 € ≪ campaign 1.71; CTR 4.6% ↑'). "
    "5) Tailor SUGGEST to the asset kind/field_type (e.g., text: pin headline, shorten benefit; image: test ratio, clearer overlay; video: tighter hook). "
    "6) Prefer SCALE when CPI ≪ campaign benchmark with sufficient volume; PAUSE when CPI is ≫ benchmark or zero‑installs at volume; "
    "REPLACE when creative underperforms with spend; CREATE_VARIATION for promising CTR but weak conversion. "
    "7) Only return the JSON function call—no prose."
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
# add imports


def _json_safe(obj: Any) -> Any:
    """Konvertiert NumPy/Pandas-Typen rekursiv in JSON-serialisierbare Builtins."""
    # None / Strings / native ints/floats/bools bleiben wie sie sind (Floats ohne NaN)
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj

    # Pandas-NA/NaN
    if obj is pd.NA:
        return None
    try:
        if pd.isna(obj):  # deckt auch numpy.nan, pd.NaT etc. ab
            return None
    except Exception:
        pass

    # NumPy-Skalare -> Python
    if np is not None:
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return None if math.isnan(float(obj)) else float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)

    # Container rekursiv
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]

    # Fallback: unverändert
    return obj


def _chat_with_retry(messages: List[Dict[str, str]]) -> Any:
    backoff = 1.5
    last_exc: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            kwargs = dict(
                model=_OPENAI_MODEL,
                response_format={"type": "json_object"},
                functions=[FUNCTION_SCHEMA],
                function_call={"name": FUNCTION_SCHEMA["name"]},
                timeout=_OPENAI_REQ_TIMEOUT,
                messages=messages,
            )
            if _OPENAI_SEED is not None:
                kwargs["seed"] = _OPENAI_SEED  # type: ignore[assignment]
            return openai.chat.completions.create(**kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt == _MAX_RETRIES:
                raise
            time.sleep(backoff * attempt)
    # should not reach
    if last_exc:
        raise last_exc


def _parse_function_arguments(msg: Any) -> Dict[str, Any]:
    """Robust parsing of function_call or content JSON."""
    # Newer clients: message has dict-like attributes; guard everything
    # 1) Preferred: function_call.arguments
    try:
        args = getattr(getattr(msg, "function_call", None), "arguments", None)
        if args:
            return json.loads(args)
    except Exception:
        pass
    # 2) Fallback: JSON in content
    try:
        content = getattr(msg, "content", None)
        if content and isinstance(content, str):
            return json.loads(content)
    except Exception:
        pass
    return {}


def _valid_asset_ids_from_payload(payload: Dict[str, Any]) -> Set[int]:
    ids: Set[int] = set()
    for item in payload.get("google_ads_assets_time_series", []) or []:
        try:
            ids.add(int(item.get("asset_id")))
        except Exception:
            continue
    return ids


def _extract_id_and_action(line: str) -> Tuple[Optional[int], Optional[str]]:
    try:
        parts = line.split("|")
        asset_id = int(parts[0].strip())
    except Exception:
        asset_id = None
    m = re.search(r"ACTION=([a-zA-Z_]+)", line)
    action = m.group(1).lower() if m else None
    return asset_id, action


def _sanitize_line(line: str) -> str:
    # Trim whitespace/newlines, collapse spaces, forbid pipes in WHY/SUGGEST
    s = " ".join(line.strip().split())
    # Replace any accidental double pipes in reasons
    # (we can't 100% fix malformed lines, but we remove obvious hazards)
    # Ensure WHY=/SUGGEST= segments don't include '|'
    s = s.replace(" | ", "|").replace("| ", "|").replace(" |", "|")
    return s


def _is_valid_line(line: str, valid_ids: Set[int]) -> bool:
    if not line or "|" not in line:
        return False
    parts = line.split("|")
    if len(parts) < 5:
        return False
    # id
    try:
        asset_id = int(parts[0])
    except Exception:
        return False
    if valid_ids and asset_id not in valid_ids:
        return False
    # action
    m = re.search(r"ACTION=([a-zA-Z_]+)", line)
    if not m or m.group(1).lower() not in ALLOWED_ACTIONS:
        return False
    # why/suggest presence
    if "WHY=" not in line or "SUGGEST=" not in line:
        return False
    # guard pipes inside WHY/SUGGEST
    # (basic heuristic: after WHY= or SUGGEST= until next key)
    if re.search(r"WHY=[^|]*\|[^A-Z]", line):
        return False
    if re.search(r"SUGGEST=[^|]*\|", line):
        return False
    return True


def _dedupe_and_limit(lines: List[str]) -> List[str]:
    """Dedupe by asset_id; prioritise by action impact; cap 5–15."""
    if not lines:
        return []

    # Parse and bucket by action
    parsed: List[Tuple[int, str, str]] = []  # (asset_id, action, line)
    for ln in lines:
        aid, act = _extract_id_and_action(ln)
        if aid is None or act is None:
            continue
        parsed.append((aid, act, ln))

    # Deduplicate by asset id, keeping best action by priority
    action_rank = {"scale": 0, "pause": 1, "create_variation": 2, "replace": 3}
    best_by_id: Dict[int, Tuple[int, str]] = {}
    for aid, act, ln in parsed:
        rank = action_rank.get(act, 9)
        current = best_by_id.get(aid)
        if current is None or rank < current[0]:
            best_by_id[aid] = (rank, ln)

    unique_lines = [v[1] for v in best_by_id.values()]

    # Sort by action priority (scale -> pause -> create_variation -> replace)
    unique_lines.sort(key=lambda ln: action_rank.get(_extract_id_and_action(ln)[1] or "", 9))

    # Enforce 5–15 (if fewer than 5 available, return what we have)
    if len(unique_lines) > 15:
        unique_lines = unique_lines[:15]
    return unique_lines


def _coerce_recommendation_response(
        data: Dict[str, Any], payload: Dict[str, Any]
) -> RecommendationResponse:
    """Sanitize, validate, dedupe, limit."""
    valid_ids = _valid_asset_ids_from_payload(payload)

    raw_ads: List[str] = [str(x) for x in (data.get("google_ads") or [])]
    raw_play: List[str] = [str(x) for x in (data.get("google_play") or [])]

    # Sanitize each line
    ads_sanitized = [_sanitize_line(ln) for ln in raw_ads]

    # Filter invalid lines
    ads_valid = [ln for ln in ads_sanitized if _is_valid_line(ln, valid_ids)]

    # Dedupe & limit (5–15)
    ads_final = _dedupe_and_limit(ads_valid)

    # Google Play: cap 3, strip empties
    play_final = [s.strip() for s in raw_play if s and s.strip()]
    if len(play_final) > 3:
        play_final = play_final[:3]

    return RecommendationResponse(google_ads=ads_final, google_play=play_final)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_recommendations(payload: Dict[str, Any]) -> RecommendationResponse:
    """
    Fragt das Modell nach Asset‑Empfehlungen und liefert eine bereinigte
    RecommendationResponse (kompatibel zu deiner Pipeline).
    """
    # Schlankes, aber informatives User‑Message: Payload eingebettet als JSON
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                    "Here is our latest data as JSON. Use ONLY asset_ids present here. "
                    "Remember: strictly use the pipe template without extra text.\n"
                    "```json\n" + json.dumps(_json_safe(payload), ensure_ascii=False) + "\n```"
            ),
        },
    ]

    try:
        response = _chat_with_retry(messages)
        choice = response.choices[0].message  # type: ignore[index]

        data = _parse_function_arguments(choice)
        if not isinstance(data, dict) or "google_ads" not in data:
            # defensiver Fallback
            data = {"google_ads": [], "google_play": []}

        # Post‑Sanitizing & Validation against payload
        return _coerce_recommendation_response(data, payload)

    except Exception as exc:
        print(f"OpenAI recommendation generation failed – {exc}")
        # robuste, leere Antwort
        return RecommendationResponse(google_ads=[], google_play=[])  # type: ignore[arg-type]
