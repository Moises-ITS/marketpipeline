from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        # Ignore unrelated variables already present in your shell rather than erroring on them.
        extra="ignore",
    )

    redis_url: str = "redis://localhost:6379/0"
    pg_dsn: str = "postgresql://marketdata:marketdata@localhost:5434/marketdata"

    coinbase_ws_url: str = "wss://ws-feed.exchange.coinbase.com"

    symbols: Annotated[list[str], NoDecode] = ["BTC-USD", "ETH-USD", "SOL-USD"]

    log_level: str = "INFO"

    redis_max_connections: int = 50
    pg_max_connections: int = 10

    stream_maxlen: int = 5000

    pg_batch_size: int = 200
    pg_flush_seconds: float = 2.0

    max_history_limit: int = 5000

    # Lower than the tick limit: 1000 one-minute candles is already 16 hours of chart. Wanting
    # more than that means wanting a coarser interval, not a longer page.
    max_candle_limit: int = 1000

    history_force_db: bool = False

    @field_validator("symbols", mode="before")
    @classmethod
    def _split_symbols(cls, v: object) -> object:
        # Env vars are always strings; this turns "BTC-USD,ETH-USD" into a real list.
        # mode="before" means it runs on the raw value, ahead of pydantic's type checking.
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v


# Import this, don't construct your own: reading the environment once keeps every module in
# agreement about which database it is talking to.
settings = Settings()
