from pydantic import BaseModel
from typing import List

class RecommendationResponse(BaseModel):
    google_ads: List[str]
    google_play: List[str]
