# -*- coding: utf-8 -*-
"""OpenAI client – concrete, asset‑level recommendations.

Changes vs. previous version
────────────────────────────
* **System‑prompt** forces the assistant to output *actionable lines* in a
  deterministic pipe‑separated format:

  ``<asset_id>|<campaign_name>|ACTION=<scale|pause|replace|create_variation>|WHY=<metric_reason>|SUGGEST=<next_step>``

  Example:
  ``175699027231|QuranGPT Install|ACTION=scale|WHY=CTR up 45 % vs. baseline|SUGGEST=duplicate to new lookalike ad‑group``

* **Retry‑logic, model config** unchanged.
* **Schema** keeps `google_ads`/`google_play` lists of strings, preserving
  existing Pydantic validation while delivering far more granular content.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List

import openai
from dotenv import load_dotenv

from .schemas import RecommendationResponse

# ---------------------------------------------------------------------------
# Environment & client setup
# ---------------------------------------------------------------------------
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY", "test")
_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "o3")
_OPENAI_REQ_TIMEOUT = float(os.getenv("OPENAI_TIMEOUT", "30"))
_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRY", "3"))

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
                    "<asset_id>|<campaign_name>|ACTION=<scale|pause|replace|create_variation>" \
                    "|WHY=<metric_reason>|SUGGEST=<next_step>."
                ),
            },
            "google_play": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional ASO suggestions.",
            },
        },
        "required": ["google_ads"],
    },
}

_SYSTEM_PROMPT = (
    "You are a senior performance‑marketing strategist specialised in Google Ads. "
    "Analyse the provided 14‑day block time series per asset. For each asset, "
    "decide on exactly one concrete ACTION among: scale (increase delivery), "
    "pause (temporarily stop), replace (remove and create new creative of same "
    "type), create_variation (duplicate and test variant). Base the decision on "
    "metrics such as CTR, CPI, ROAS trend across blocks. "
    "Output one line per relevant asset following EXACTLY this pipe‑separated "
    "template (no additional punctuation):\n"
    "<asset_id>|<campaign_name>|ACTION=<scale|pause|replace|create_variation>|" \
    "WHY=<short reason>|SUGGEST=<next step>\n"
    "Include 5‑15 lines total. Only include assets with actionable insights. "
    "After asset lines, optionally add up to 3 Google Play suggestions in plain "
    "sentences under google_play."
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _chat_with_retry(messages: List[Dict[str, str]]) -> Any:
    backoff = 1.5
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return openai.chat.completions.create(
                model=_OPENAI_MODEL,
                response_format={"type": "json_object"},
                functions=[FUNCTION_SCHEMA],
                timeout=_OPENAI_REQ_TIMEOUT,
                messages=messages,
            )
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                raise
            time.sleep(backoff * attempt)


def _parse_function_arguments(msg: Any) -> Dict[str, Any]:
    try:
        return json.loads(msg.function_call.arguments)  # type: ignore[attr-defined]
    except Exception:
        return {}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_recommendations(payload: Dict[str, Any]) -> RecommendationResponse:
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": "Here is our latest data as JSON:```json\n" + json.dumps(payload) + "\n```",
        },
    ]

    try:
        response = _chat_with_retry(messages)
        fn_msg = response.choices[0].message
        data = _parse_function_arguments(fn_msg)
        return RecommendationResponse(**data)  # type: ignore[arg-type]
    except Exception as exc:
        print(f"OpenAI recommendation generation failed – {exc}")
        return RecommendationResponse(google_ads=[], google_play=[])  # type: ignore[arg-type]
