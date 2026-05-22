from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class ClientConfig:
    """
    Configuration for MarketDataClient.

    Env vars
    --------
    TRADOWIX_TOKEN          — required: your oauth_session_token
    TRADOWIX_WS_URL         — optional: override WebSocket URL
    RECONNECT_ENABLED       — optional: "true" / "false"  (default true)
    RECONNECT_MAX_ATTEMPTS  — optional: 0 = unlimited      (default 0)
    CSV_DIR                 — optional: output directory    (default "data")
    LOG_LEVEL               — optional: DEBUG/INFO/WARNING  (default INFO)
    """

    token: str = ""

    ping_interval: float = 20.0
    ping_timeout:  float = 10.0

    reconnect_enabled:      bool  = True
    reconnect_initial_delay: float = 1.0
    reconnect_max_delay:     float = 60.0
    reconnect_backoff_factor: float = 2.0
    reconnect_max_attempts:  int   = 0

    request_timeout: float = 15.0

    csv_dir:          str = "data"
    csv_flush_interval: int = 50

    log_level: str = "INFO"

    extra_headers: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "ClientConfig":
        return cls(
            token=os.environ.get("TRADOWIX_TOKEN", ""),
            reconnect_enabled=os.environ.get("RECONNECT_ENABLED", "true").lower() == "true",
            reconnect_max_attempts=int(os.environ.get("RECONNECT_MAX_ATTEMPTS", "0")),
            csv_dir=os.environ.get("CSV_DIR", "data"),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )
