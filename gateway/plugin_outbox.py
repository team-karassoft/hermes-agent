"""Host-owned Phase 3 typed plugin text intents backed by delivery_ledger.

This module deliberately validates an exact host-granted route before writing a
normal gateway delivery obligation. Plugins receive no adapter or sender.
"""
from __future__ import annotations

from dataclasses import dataclass

from gateway.delivery_ledger import compute_obligation_id, mark_attempting, mark_delivered, mark_failed, record_obligation
from gateway.plugin_messaging import HostMessagingPermissions, TopicRoute


class OutboundPermissionError(PermissionError):
    """The host has not granted this plugin the requested exact text route."""


@dataclass(frozen=True)
class PluginOutboundIntent:
    idempotency_key: str
    route: TopicRoute
    text: str

    def __post_init__(self) -> None:
        if not isinstance(self.idempotency_key, str) or not self.idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("text is required")


class PluginOutboxService:
    """Validates plugin text intents and records durable gateway obligations."""

    def __init__(self, permissions: HostMessagingPermissions) -> None:
        self._permissions = permissions

    def enqueue(self, *, plugin_id: str, intent: PluginOutboundIntent) -> str:
        obligation_id, _ = self.accept(plugin_id=plugin_id, intent=intent)
        return obligation_id

    def accept(
        self, *, plugin_id: str, intent: PluginOutboundIntent
    ) -> tuple[str, bool]:
        """Persist once and report whether this call accepted a new obligation."""
        if not self._permissions.allows_outbound_text(plugin_id, intent.route):
            raise OutboundPermissionError("plugin has no outbound text grant for route")
        obligation_id = compute_obligation_id(
            f"plugin:{plugin_id}:{intent.route.platform}:{intent.route.chat_id}:{intent.route.thread_id or ''}",
            intent.idempotency_key,
            intent.text,
        )
        inserted = record_obligation(
            obligation_id=obligation_id,
            session_key=f"plugin:{plugin_id}:{intent.route.platform}:{intent.route.chat_id}:{intent.route.thread_id or ''}",
            platform=intent.route.platform,
            chat_id=intent.route.chat_id,
            thread_id=intent.route.thread_id,
            content=intent.text,
            replace_existing=False,
        )
        return obligation_id, inserted

    async def deliver(self, *, adapter, plugin_id: str, intent: PluginOutboundIntent) -> bool:
        """Host-only immediate delivery; callers supply the gateway-selected adapter."""
        obligation_id = self.enqueue(plugin_id=plugin_id, intent=intent)
        return await self.deliver_persisted(
            adapter=adapter,
            obligation_id=obligation_id,
            intent=intent,
        )

    @staticmethod
    async def deliver_persisted(
        *,
        adapter,
        obligation_id: str,
        intent: PluginOutboundIntent,
    ) -> bool:
        """Settle an already-accepted intent through a host-selected adapter."""
        mark_attempting(obligation_id)
        try:
            result = await adapter.send(chat_id=intent.route.chat_id, content=intent.text, reply_to=None, metadata={"thread_id": intent.route.thread_id})
        except Exception as exc:
            mark_failed(obligation_id, type(exc).__name__)
            return False
        if getattr(result, "success", False):
            mark_delivered(obligation_id)
            return True
        mark_failed(obligation_id, str(getattr(result, "error", "") or "send-failed"))
        return False
