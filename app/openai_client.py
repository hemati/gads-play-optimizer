import json
import os

import openai
from dotenv import load_dotenv

from .schemas import RecommendationResponse

load_dotenv()
# Use a placeholder key during tests when OPENAI_API_KEY is undefined
openai.api_key = os.getenv("OPENAI_API_KEY", "test")

FUNCTION_SCHEMA = {
    "name": "recommend_actions",
    "description": "Return ads & play optimisation steps.",
    "parameters": {
        "type": "object",
        "properties": {
            "google_ads": {"type": "array", "items": {"type": "string"}},
            "google_play": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["google_ads", "google_play"],
    },
}


def get_recommendations(payload: dict) -> RecommendationResponse:
    response = openai.chat.completions.create(
        model="gpt-4o",
        response_format={"type": "json_object"},
        functions=[FUNCTION_SCHEMA],
        messages=[
            {"role": "system", "content": "You are a senior ASO & Google Ads coach."},
            {"role": "user", "content": f"Here is our latest data:`{json.dumps(payload)}`"},
        ],
    )
    arguments = response.choices[0].message.function_call.arguments
    data = json.loads(arguments)
    return RecommendationResponse(**data)
