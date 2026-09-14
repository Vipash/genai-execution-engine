from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    APP_ENV: str = "development"
    APP_NAME: str = "GenAI-Execution-Engine"
    DEBUG: bool = True
    PORT: int = 8000

    DATABASE_URL: str
    REDIS_URL: str

    WORKER_CONCURRENCY: int = 5
    JOB_MAX_RETRIES: int = 3

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


settings = Settings()
