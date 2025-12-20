from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    db: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"

    model_config = SettingsConfigDict(
        env_prefix="PG_",
        case_sensitive=False,
    )

settings = Settings()