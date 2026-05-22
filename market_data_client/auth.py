from __future__ import annotations

import asyncio
import json
import logging

logger = logging.getLogger(__name__)


class TokenAuthHandler:
    """
    Handles token authentication for TradoWix WebSocket.

    Protocol:
      Server → {"type": "authRequired", ...}
      Client → {"type": "authenticate", "token": "<token>"}
      Server → {"type": "authenticated", "data": {"userId": "...", ...}}
    """

    def __init__(self, token: str, timeout: float = 15.0):
        if not token:
            raise ValueError("Authentication token must not be empty — set TRADOWIX_TOKEN env var")
        self._token = token
        self._timeout = timeout
        self._authenticated = False

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    def invalidate(self) -> None:
        self._authenticated = False

    def build_auth_message(self) -> str:
        return json.dumps({"type": "authenticate", "token": self._token})

    async def authenticate(self, ws) -> None:
        """
        Perform the auth handshake.  Waits for server's 'authenticated' message.
        Raises RuntimeError on failure or timeout.
        """
        self._authenticated = False
        logger.debug("Sending authenticate handshake")
        await ws.send(self.build_auth_message())

        try:
            async with asyncio.timeout(self._timeout):
                async for raw in ws:
                    msg = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                    t   = msg.get("type", "")

                    if t == "authRequired":
                        logger.debug("Server requested auth again — resending token")
                        await ws.send(self.build_auth_message())
                        continue

                    if t in ("authenticated", "ready"):
                        self._authenticated = True
                        user_id = (msg.get("data") or {}).get("userId", "")
                        logger.info("Authenticated (userId=%s)", user_id)
                        return

                    if t in ("authError", "auth_error"):
                        err = (msg.get("data") or {}).get("message") or msg.get("message") or t
                        raise RuntimeError(f"Authentication rejected: {err}")

                    logger.debug("Skipping pre-auth message: type=%s", t)
        except TimeoutError:
            raise RuntimeError(f"Authentication timed out after {self._timeout}s")
