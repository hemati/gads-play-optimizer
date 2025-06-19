from fastapi import FastAPI

from .openai_client import get_recommendations
from .schemas import RecommendationResponse

app = FastAPI(title="GAds Play Optimizer")


@app.get("/recommendations", response_model=RecommendationResponse)
def recommendations():
    dummy_payload = {"ads": {}, "play": {}}
    return get_recommendations(dummy_payload)
