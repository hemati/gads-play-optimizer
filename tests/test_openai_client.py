from app.openai_client import get_recommendations
from app.schemas import RecommendationResponse

class DummyResponse:
    class Choice:
        class Message:
            function_call = type('F', (), {"arguments": '{"google_ads": [], "google_play": []}'})()
        message = Message()
    choices = [Choice()]


def test_get_recommendations(monkeypatch):
    def fake_create(**kwargs):
        return DummyResponse()
    monkeypatch.setattr("openai.chat.completions.create", fake_create)
    result = get_recommendations({})
    assert isinstance(result, RecommendationResponse)
    assert result.google_ads == []
    assert result.google_play == []
