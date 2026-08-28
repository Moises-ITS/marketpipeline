"""Configuration, loaded once from the environment and shared by every module.

WHY THIS FILE EXISTS
Connection details could be hardcoded in each script, but then the port-5434 quirk would be
duplicated in five places and one of them would eventually be wrong. Worse, hardcoded values
cannot change between environments - and in build-order step 11 the app moves into Docker,
where the database is reachable at host `timescaledb:5432` instead of `localhost:5434`. With
settings read from the environment, that move is a config change and not a code change.

HOW IT WORKS
pydantic-settings reads each field from an environment variable of the same name (uppercased),
falling back to the `.env` file, then to the default written here. It also *validates*: if
someone sets a field to nonsense, you get a clear error at startup rather than a confusing
crash later. `SYMBOLS=BTC-USD,ETH-USD` becomes a real Python list, not a string you must
remember to split.
"""

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

    # NoDecode is required, and the reason is a genuine trap. For any list-typed field,
    # pydantic-settings assumes the environment variable holds JSON and tries json.loads()
    # on it *before* any validator runs - so `SYMBOLS=BTC-USD,ETH-USD` crashes with
    # "Expecting value: line 1 column 1" long before _split_symbols below is reached.
    # NoDecode switches that JSON step off and hands the raw string to the validator instead.
    symbols: Annotated[list[str], NoDecode] = ["BTC-USD", "ETH-USD", "SOL-USD"]

    log_level: str = "INFO"

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
