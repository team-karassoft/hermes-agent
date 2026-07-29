"""Host-only signed callback registry for RFC 0001 plugin inline actions."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping

from gateway.plugin_messaging import TopicRoute


_CALLBACK_SIGNING_KEY_BYTES = 32


def load_or_create_callback_signing_key(
    path: str | Path,
) -> bytes:
    """Load the host-owned callback key, creating it privately on first use.

    The explicit path is also the test seam; the gateway supplies a
    profile-local host path rather than accepting plugin-controlled input.
    """
    key_path = Path(path)
    key_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    try:
        fd = os.open(key_path, flags, 0o600)
    except FileExistsError:
        key = key_path.read_bytes()
    else:
        key = secrets.token_bytes(_CALLBACK_SIGNING_KEY_BYTES)
        try:
            if hasattr(os, "fchmod"):
                os.fchmod(fd, 0o600)
            key_file = os.fdopen(fd, "wb")
            fd = -1
            with key_file:
                key_file.write(key)
                key_file.flush()
                os.fsync(key_file.fileno())
        except Exception:
            if fd >= 0:
                os.close(fd)
            key_path.unlink(missing_ok=True)
            raise

    if len(key) != _CALLBACK_SIGNING_KEY_BYTES:
        raise ValueError("invalid callback signing key")
    return key


class CallbackRejected(PermissionError):
    """A callback failed host authenticity, audience, expiry, or replay checks."""


@dataclass(frozen=True)
class TrustedCallback:
    """Transport-normalized callback input built only from trusted host fields."""

    token: str
    route: TopicRoute
    message_id: str
    sender_id: str | None
    event_id: str
    received_at: datetime


@dataclass(frozen=True)
class CallbackClaim:
    """Validated server-side action data; the opaque token is deliberately absent."""

    plugin_id: str
    route: TopicRoute
    message_id: str
    action: str
    payload: Mapping[str, Any]
    sender_id: str | None
    event_id: str
    received_at: datetime


class HostCallbackRegistry:
    """Issues signed opaque handles and atomically enforces one-time ownership."""

    def __init__(
        self,
        *,
        signing_key: bytes,
        database_path: str | Path,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(signing_key, bytes) or len(signing_key) < 8:
            raise ValueError("callback signing key must be at least 8 bytes")
        self._signing_key = signing_key
        self._database_path = Path(database_path)
        self._now = now or (lambda: datetime.now(UTC))
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS plugin_callback_tokens (
                    token_id TEXT PRIMARY KEY,
                    plugin_id TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    thread_id TEXT,
                    message_id TEXT,
                    action TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    consumed_at TEXT
                )
                """
            )

    def _signature(self, token_id: str) -> str:
        digest = hmac.new(
            self._signing_key, f"pc1.{token_id}".encode(), hashlib.sha256
        ).digest()[:16]
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode()

    def _parse(self, token: str) -> str:
        try:
            version, token_id, supplied = token.split(".", 2)
        except (AttributeError, ValueError):
            raise CallbackRejected("tampered callback token") from None
        if version != "pc1" or not token_id or not hmac.compare_digest(
            supplied, self._signature(token_id)
        ):
            raise CallbackRejected("tampered callback token")
        return token_id

    def issue(
        self,
        *,
        plugin_id: str,
        route: TopicRoute,
        action: str,
        payload: Mapping[str, Any],
        expires_at: datetime,
    ) -> str:
        token_id = secrets.token_urlsafe(18)
        payload_json = json.dumps(
            dict(payload), sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO plugin_callback_tokens
                (token_id, plugin_id, platform, chat_id, thread_id, action,
                 payload_json, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    token_id,
                    plugin_id,
                    route.platform,
                    route.chat_id,
                    route.thread_id,
                    action,
                    payload_json,
                    expires_at.astimezone(UTC).isoformat(),
                ),
            )
        return f"pc1.{token_id}.{self._signature(token_id)}"

    def issue_for(
        self,
        *,
        plugin_id: str,
        route: TopicRoute,
        action: str,
        payload: Mapping[str, Any],
        ttl_seconds: int,
    ) -> str:
        return self.issue(
            plugin_id=plugin_id,
            route=route,
            action=action,
            payload=payload,
            expires_at=self._now() + timedelta(seconds=ttl_seconds),
        )

    def bind_message(self, *, token: str, message_id: str) -> None:
        token_id = self._parse(token)
        if not isinstance(message_id, str) or not message_id:
            raise CallbackRejected("callback message binding is missing")
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE plugin_callback_tokens SET message_id=?
                WHERE token_id=? AND message_id IS NULL
                """,
                (message_id, token_id),
            ).rowcount
        if changed != 1:
            raise CallbackRejected("callback message binding failed")

    def validate_and_consume(self, callback: TrustedCallback) -> CallbackClaim:
        token_id = self._parse(callback.token)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM plugin_callback_tokens WHERE token_id=?", (token_id,)
            ).fetchone()
            if row is None:
                raise CallbackRejected("tampered callback token")
            if row["consumed_at"] is not None:
                raise CallbackRejected("replayed callback token")
            if self._now().astimezone(UTC) > datetime.fromisoformat(
                row["expires_at"]
            ):
                raise CallbackRejected("expired callback token")
            stored_route = TopicRoute(
                row["platform"], row["chat_id"], row["thread_id"]
            )
            if callback.route != stored_route:
                raise CallbackRejected("callback route mismatch")
            if not row["message_id"] or callback.message_id != row["message_id"]:
                raise CallbackRejected("callback message mismatch")
            changed = connection.execute(
                """
                UPDATE plugin_callback_tokens SET consumed_at=?
                WHERE token_id=? AND consumed_at IS NULL
                """,
                (self._now().astimezone(UTC).isoformat(), token_id),
            ).rowcount
            if changed != 1:
                raise CallbackRejected("replayed callback token")
            connection.commit()
            return CallbackClaim(
                plugin_id=row["plugin_id"],
                route=stored_route,
                message_id=row["message_id"],
                action=row["action"],
                payload=json.loads(row["payload_json"]),
                sender_id=callback.sender_id,
                event_id=callback.event_id,
                received_at=callback.received_at,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
