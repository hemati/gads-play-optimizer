# BaseSettings moved to `pydantic_settings` in pydantic v2
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    openai_api_key: str

    class Config:
        env_file = '.env'

settings = Settings()
