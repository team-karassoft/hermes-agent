"""Minimal, inert contract helper used only by the repository test fixture."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from gateway.plugin_messaging import ConsumerDeclaration, TopicRoute

_ROUTE = TopicRoute(platform="telegram", chat_id="-1004411640215", thread_id=None)
_OWNER_ID = "telegram-user:9189955"
_TTL_SECONDS = "28800"


def _strict_active_config(config_path: Path) -> None:
    """Reject every config shape except the bounded active bridge contract."""
    entries: dict[str, str] = {}
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        if not raw_line or ":" not in raw_line:
            raise ValueError("fixture bridge config must contain exact key/value lines")
        key, value = raw_line.split(":", 1)
        if key in entries:
            raise ValueError("fixture bridge config keys must be unique")
        entries[key] = value.strip().strip('"')
    if set(entries) != {
        "active",
        "database_path",
        "telegram_chat_id",
        "owner_id",
        "approval_ttl_seconds",
    }:
        raise ValueError("fixture bridge config keys do not match the contract")
    if (
        entries["active"] != "true"
        or not Path(entries["database_path"]).is_absolute()
        or entries["telegram_chat_id"] != _ROUTE.chat_id
        or entries["owner_id"] != _OWNER_ID
        or entries["approval_ttl_seconds"] != _TTL_SECONDS
    ):
        raise ValueError("fixture bridge config is not the active exact-route contract")


def _claim_exact_owner(event: Any) -> dict[str, str]:
    if (
        event.sender_id == "9189955"
        and event.route == _ROUTE
        and event.thread_id is None
    ):
        return {"action": "claim"}
    return {"action": "reject"}


def register(ctx: Any) -> None:
    """Register one exact consumer after validating the temp-generated config."""
    _strict_active_config(Path(__file__).with_name("config.yaml"))
    ctx.messaging.subscribe(
        subscription_id="gateway-restart",
        routes=[_ROUTE],
        event_types={"message"},
        mode="consumer",
        handler=_claim_exact_owner,
        consumer=ConsumerDeclaration(command_namespace="gateway-restart"),
    )
