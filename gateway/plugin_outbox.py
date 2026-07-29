"""Host-owned typed plugin text and inline-keyboard intents.

This module deliberately validates an exact host-granted route before writing a
normal gateway delivery obligation. Plugins receive no adapter or sender.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from gateway.delivery_ledger import compute_obligation_id, mark_attempting, mark_delivered, mark_failed, record_obligation
from gateway.plugin_messaging import HostMessagingPermissions, InlineKeyboard, TopicRoute


class OutboundPermissionError(PermissionError):
    """The host has not granted this plugin the requested exact text route."""


@dataclass(frozen=True)
class PluginOutboundIntent:
    idempotency_key: str
    route: TopicRoute
    text: str
    keyboard: InlineKeyboard | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.idempotency_key, str) or not self.idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        if not isinstance(self.text, str) or not self.text.strip():
            raise ValueError("text is required")
        if self.keyboard is not None and not isinstance(
            self.keyboard, InlineKeyboard
        ):
            raise ValueError("keyboard must be an InlineKeyboard")


class PluginOutboxService:
    """Validates plugin text intents and records durable gateway obligations."""

    def __init__(
        self, permissions: HostMessagingPermissions, *, callback_registry=None
    ) -> None:
        self._permissions = permissions
        self._callback_registry = callback_registry

    def enqueue(self, *, plugin_id: str, intent: PluginOutboundIntent) -> str:
        obligation_id, _ = self.accept(plugin_id=plugin_id, intent=intent)
        return obligation_id

    def accept(
        self, *, plugin_id: str, intent: PluginOutboundIntent
    ) -> tuple[str, bool]:
        """Persist once and report whether this call accepted a new obligation."""
        if not self._permissions.allows_outbound_text(plugin_id, intent.route):
            raise OutboundPermissionError("plugin has no outbound text grant for route")
        if intent.keyboard is not None:
            if not self._permissions.allows_outbound_keyboard(
                plugin_id, intent.route
            ):
                raise OutboundPermissionError(
                    "plugin has no outbound inline_keyboard grant for route"
                )
            if self._callback_registry is None:
                raise OutboundPermissionError(
                    "host callback validation is unavailable"
                )
        keyboard_fingerprint = (
            json.dumps(
                [
                    [
                        {
                            "label": button.label,
                            "action": button.action,
                            "payload": dict(button.payload),
                        }
                        for button in row
                    ]
                    for row in intent.keyboard.rows
                ],
                sort_keys=True,
                separators=(",", ":"),
            )
            if intent.keyboard is not None
            else ""
        )
        obligation_id = compute_obligation_id(
            f"plugin:{plugin_id}:{intent.route.platform}:{intent.route.chat_id}:{intent.route.thread_id or ''}",
            intent.idempotency_key,
            f"{intent.text}\0{keyboard_fingerprint}",
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
            plugin_id=plugin_id,
            callback_registry=self._callback_registry,
        )

    @staticmethod
    async def deliver_persisted(
        *,
        adapter,
        obligation_id: str,
        intent: PluginOutboundIntent,
        plugin_id: str | None = None,
        callback_registry=None,
    ) -> bool:
        """Settle an already-accepted intent through a host-selected adapter."""
        mark_attempting(obligation_id)
        tokens: list[str] = []
        metadata = {"thread_id": intent.route.thread_id}
        if intent.keyboard is not None:
            if callback_registry is None or not plugin_id:
                mark_failed(obligation_id, "callback-validation-unavailable")
                return False
            rendered = []
            for row in intent.keyboard.rows:
                rendered_row = []
                for button in row:
                    token = callback_registry.issue_for(
                        plugin_id=plugin_id,
                        route=intent.route,
                        action=button.action,
                        payload=button.payload,
                        ttl_seconds=900,
                    )
                    tokens.append(token)
                    rendered_row.append(
                        {"text": button.label, "callback_token": token}
                    )
                rendered.append(rendered_row)
            metadata["inline_keyboard"] = rendered
        try:
            result = await adapter.send(chat_id=intent.route.chat_id, content=intent.text, reply_to=None, metadata=metadata)
        except Exception as exc:
            mark_failed(obligation_id, type(exc).__name__)
            return False
        if getattr(result, "success", False):
            message_id = getattr(result, "message_id", None)
            if tokens and not message_id:
                mark_failed(obligation_id, "missing-message-id")
                return False
            try:
                for token in tokens:
                    callback_registry.bind_message(
                        token=token, message_id=str(message_id)
                    )
            except Exception as exc:
                mark_failed(obligation_id, type(exc).__name__)
                return False
            mark_delivered(obligation_id)
            return True
        mark_failed(obligation_id, str(getattr(result, "error", "") or "send-failed"))
        return False
