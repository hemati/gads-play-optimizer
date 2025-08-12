from typing import List

from pydantic import BaseModel


class RecommendationResponse(BaseModel):
    google_ads: List[str] = None
    google_play: List[str] = None
