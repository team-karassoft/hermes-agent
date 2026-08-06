"""Host-owned typed plugin text and inline-keyboard intents.

This module deliberately validates an exact host-granted route before writing a
normal gateway delivery obligation. Plugins receive no adapter or sender.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Mapping

from gateway.delivery_ledger import compute_obligation_id, mark_attempting, mark_delivered, mark_failed, record_obligation
from gateway.plugin_messaging import Button, HostMessagingPermissions, InlineKeyboard, TopicRoute

_MAX_INTENT_BYTES = 16 * 1024
_MAX_KEYBOARD_ROWS = 12
_MAX_BUTTONS_PER_ROW = 8
_MAX_LABEL_BYTES = 256
_MAX_ACTION_BYTES = 128
_MAX_PAYLOAD_BYTES = 2048
# These are deliberately anchored, value-only formats. Payload key names are
# not security boundaries: only values that encode reserved callbacks or
# credentials are rejected.
_BEARER_CREDENTIAL = re.compile(r"(?i)Bearer\s+\S+\Z")
_JWT_VALUE = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\Z")
_CREDENTIAL_VALUE_PATTERNS = (
    re.compile(r"sk-(?:proj-)?[A-Za-z0-9_-]{20,}\Z"),
    re.compile(r"(?:gh[pousr]_|github_pat_)[A-Za-z0-9_]{20,}\Z"),
    re.compile(r"glpat-[A-Za-z0-9_-]{20,}\Z"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}\Z"),
    re.compile(r"AKIA[0-9A-Z]{16}\Z"),
)


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
        if self.keyboard is not None and not isinstance(self.keyboard, InlineKeyboard):
            raise ValueError("keyboard must be an InlineKeyboard")


class PluginOutboxService:
    """Validates plugin intents and records durable gateway obligations."""

    def __init__(self, permissions: HostMessagingPermissions, *, callback_registry=None) -> None:
        self._permissions = permissions
        self._callback_registry = callback_registry

    def enqueue(self, *, plugin_id: str, intent: PluginOutboundIntent) -> str:
        obligation_id, _ = self.accept(plugin_id=plugin_id, intent=intent)
        return obligation_id

    def accept(self, *, plugin_id: str, intent: PluginOutboundIntent) -> tuple[str, bool]:
        """Persist once and report whether this call accepted a new obligation."""
        _validate_grants(plugin_id, intent, self._permissions, self._callback_registry)
        semantic_intent = _serialize_semantic_intent(plugin_id, intent)
        obligation_id = compute_obligation_id(
            f"plugin:{plugin_id}:{intent.route.platform}:{intent.route.chat_id}:{intent.route.thread_id or ''}",
            intent.idempotency_key,
            f"{intent.text}\0{semantic_intent}",
        )
        inserted = record_obligation(
            obligation_id=obligation_id,
            session_key=f"plugin:{plugin_id}:{intent.route.platform}:{intent.route.chat_id}:{intent.route.thread_id or ''}",
            platform=intent.route.platform,
            chat_id=intent.route.chat_id,
            thread_id=intent.route.thread_id,
            content=intent.text,
            replace_existing=False,
            plugin_intent=semantic_intent,
        )
        return obligation_id, inserted

    @staticmethod
    def reconstruct_persisted(
        row: Mapping[str, Any], *, permissions: HostMessagingPermissions, callback_registry=None
    ) -> tuple[str, PluginOutboundIntent]:
        """Load and revalidate a plugin intent before a recovery delivery.

        The ledger's text is not a fallback for this row: malformed keyboard
        semantics must fail closed rather than silently sending a buttonless
        message after a restart.
        """
        raw = row.get("plugin_intent")
        if not isinstance(raw, str) or not raw:
            raise ValueError("plugin delivery intent is missing")
        try:
            data = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("plugin delivery intent is malformed") from exc
        expected = {"plugin_id", "idempotency_key", "route", "text", "keyboard"}
        if not isinstance(data, dict) or set(data) != expected:
            raise ValueError("plugin delivery intent has invalid fields")
        try:
            plugin_id = data["plugin_id"]
            route_data = data["route"]
            if not isinstance(plugin_id, str) or not isinstance(route_data, dict):
                raise ValueError("invalid identity")
            if set(route_data) != {"platform", "chat_id", "thread_id"}:
                raise ValueError("invalid route")
            route = TopicRoute(route_data["platform"], route_data["chat_id"], route_data["thread_id"])
            intent = PluginOutboundIntent(
                idempotency_key=data["idempotency_key"], route=route, text=data["text"],
                keyboard=_keyboard_from_semantic(data["keyboard"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("plugin delivery intent has invalid semantics") from exc
        if (route.platform != row.get("platform") or route.chat_id != str(row.get("chat_id"))
                or route.thread_id != row.get("thread_id") or intent.text != row.get("content")):
            raise ValueError("plugin delivery intent does not match obligation")
        _validate_grants(plugin_id, intent, permissions, callback_registry)
        return plugin_id, intent

    async def deliver(self, *, adapter, plugin_id: str, intent: PluginOutboundIntent) -> bool:
        obligation_id = self.enqueue(plugin_id=plugin_id, intent=intent)
        return await self.deliver_persisted(
            adapter=adapter, obligation_id=obligation_id, intent=intent, plugin_id=plugin_id,
            callback_registry=self._callback_registry,
        )

    @staticmethod
    async def deliver_persisted(
        *, adapter, obligation_id: str, intent: PluginOutboundIntent, plugin_id: str | None = None,
        callback_registry=None, content_prefix: str = "",
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
                        plugin_id=plugin_id, route=intent.route, action=button.action,
                        payload=button.payload, ttl_seconds=900,
                    )
                    tokens.append(token)
                    rendered_row.append({"text": button.label, "callback_token": token})
                rendered.append(rendered_row)
            metadata["inline_keyboard"] = rendered
        try:
            result = await adapter.send(
                chat_id=intent.route.chat_id, content=content_prefix + intent.text,
                reply_to=None, metadata=metadata,
            )
        except Exception as exc:
            mark_failed(obligation_id, type(exc).__name__)
            return False
        if getattr(result, "success", False):
            message_id = getattr(result, "message_id", None)
            if tokens and not message_id:
                mark_failed(obligation_id, "missing-message-id")
                return False
            try:
                assert callback_registry is not None
                for token in tokens:
                    callback_registry.bind_message(token=token, message_id=str(message_id))
            except Exception as exc:
                mark_failed(obligation_id, type(exc).__name__)
                return False
            mark_delivered(obligation_id)
            return True
        mark_failed(obligation_id, str(getattr(result, "error", "") or "send-failed"))
        return False


def _validate_grants(plugin_id: str, intent: PluginOutboundIntent,
                     permissions: HostMessagingPermissions, callback_registry) -> None:
    if not permissions.allows_outbound_text(plugin_id, intent.route):
        raise OutboundPermissionError("plugin has no outbound text grant for route")
    if intent.keyboard is not None:
        if not permissions.allows_outbound_keyboard(plugin_id, intent.route):
            raise OutboundPermissionError("plugin has no outbound inline_keyboard grant for route")
        if callback_registry is None:
            raise OutboundPermissionError("host callback validation is unavailable")


def _serialize_semantic_intent(plugin_id: str, intent: PluginOutboundIntent) -> str:
    if not isinstance(plugin_id, str) or not plugin_id.strip() or len(plugin_id) > 128:
        raise ValueError("plugin_id is required")
    data = {
        "plugin_id": plugin_id,
        "idempotency_key": intent.idempotency_key,
        "route": {"platform": intent.route.platform, "chat_id": intent.route.chat_id,
                  "thread_id": intent.route.thread_id},
        "text": intent.text,
        "keyboard": _semantic_keyboard(intent.keyboard),
    }
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode("utf-8")) > _MAX_INTENT_BYTES:
        raise ValueError("plugin delivery intent exceeds durable bound")
    return encoded


def _semantic_keyboard(keyboard: InlineKeyboard | None) -> list[list[dict[str, Any]]] | None:
    if keyboard is None:
        return None
    if len(keyboard.rows) > _MAX_KEYBOARD_ROWS:
        raise ValueError("keyboard exceeds durable row bound")
    rows = []
    for row in keyboard.rows:
        if len(row) > _MAX_BUTTONS_PER_ROW:
            raise ValueError("keyboard exceeds durable button bound")
        rendered = []
        for button in row:
            if (len(button.label.encode()) > _MAX_LABEL_BYTES
                    or len(button.action.encode()) > _MAX_ACTION_BYTES):
                raise ValueError("keyboard button exceeds durable bound")
            payload = dict(button.payload)
            _reject_transport_secrets(payload)
            payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
            if len(payload_json.encode()) > _MAX_PAYLOAD_BYTES:
                raise ValueError("keyboard payload exceeds durable bound")
            rendered.append({"label": button.label, "action": button.action, "payload": payload})
        rows.append(rendered)
    return rows


def _reject_transport_secrets(value: Any) -> None:
    if isinstance(value, Mapping):
        for child in value.values():
            _reject_transport_secrets(child)
    elif isinstance(value, list):
        for child in value:
            _reject_transport_secrets(child)
    elif isinstance(value, str) and _is_transport_secret_value(value):
        raise ValueError("keyboard payload must not contain callback tokens or credentials")


def _is_transport_secret_value(value: str) -> bool:
    """Recognize only reserved host callbacks and bounded credential formats."""
    return (
        value.startswith("pc1.")
        or _BEARER_CREDENTIAL.fullmatch(value) is not None
        or _JWT_VALUE.fullmatch(value) is not None
        or any(pattern.fullmatch(value) for pattern in _CREDENTIAL_VALUE_PATTERNS)
    )


def _keyboard_from_semantic(value: Any) -> InlineKeyboard | None:
    if value is None:
        return None
    if not isinstance(value, list) or not value or len(value) > _MAX_KEYBOARD_ROWS:
        raise ValueError("keyboard semantics are invalid")
    rows = []
    for row in value:
        if not isinstance(row, list) or not row or len(row) > _MAX_BUTTONS_PER_ROW:
            raise ValueError("keyboard semantics are invalid")
        buttons = []
        for item in row:
            if not isinstance(item, dict) or set(item) != {"label", "action", "payload"}:
                raise ValueError("keyboard button semantics are invalid")
            _reject_transport_secrets(item["payload"])
            button = Button(item["label"], item["action"], item["payload"])
            _semantic_keyboard(InlineKeyboard(rows=((button,),)))
            buttons.append(button)
        rows.append(tuple(buttons))
    return InlineKeyboard(rows=tuple(rows))
