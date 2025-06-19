from typing import List

from pydantic import BaseModel


class RecommendationResponse(BaseModel):
    google_ads: List[str]
    google_play: List[str]
