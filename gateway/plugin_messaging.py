"""Host-owned Phase 1 contracts for plugin inbound message observation.

This module deliberately has no adapter, credential, config-file, outbound, or
normal-dispatch dependencies.  A caller supplies already profile-scoped host
configuration as a mapping; plugin subscriptions are requests and only exact
host grants become eligible for observer fan-out.
"""

from __future__ import annotations

import inspect
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Literal, Mapping


EventKind = Literal["message", "callback"]
SubscriptionMode = Literal["observer", "consumer"]
EventHandler = Callable[["PluginMessageEvent"], Any | Awaitable[Any]]


class SubscriptionError(ValueError):
    """A plugin subscription request does not meet the Phase 1 contract."""


@dataclass(frozen=True)
class AttachmentRef:
    """Opaque attachment reference; transport access remains host-owned."""

    reference: str
    media_type: str | None = None


@dataclass(frozen=True)
class TopicRoute:
    """An exact, platform-neutral inbound route identity."""

    platform: str
    chat_id: str
    thread_id: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.platform, str) or not self.platform.strip():
            raise SubscriptionError("route platform must be a non-empty string")
        if not isinstance(self.chat_id, str) or not self.chat_id.strip():
            raise SubscriptionError("route chat_id must be a non-empty string")
        if self.thread_id is not None and not isinstance(self.thread_id, str):
            raise SubscriptionError("route thread_id must be a string or null")


@dataclass(frozen=True)
class PluginMessageEvent:
    """Immutable plugin-facing envelope derived from trusted host input."""

    event_id: str
    platform: str
    chat_id: str
    thread_id: str | None
    message_id: str | None
    sender_id: str | None
    chat_type: str | None
    kind: EventKind
    text: str | None
    reply_to_message_id: str | None
    attachments: tuple[AttachmentRef, ...]
    received_at: datetime

    @property
    def route(self) -> TopicRoute:
        return TopicRoute(self.platform, self.chat_id, self.thread_id)

    @classmethod
    def from_message_event(cls, event: Any) -> "PluginMessageEvent":
        """Build an envelope using route and sender data only from ``source``.

        ``raw_message``, event metadata, tool values, and plugin/model-provided
        identity fields are intentionally ignored.  ``MessageEvent.source`` is
        created by the host adapter's normalized boundary.
        """
        source = getattr(event, "source", None)
        if source is None:
            raise SubscriptionError("cannot create plugin event without trusted source")
        platform_value = getattr(getattr(source, "platform", None), "value", getattr(source, "platform", None))
        platform = str(platform_value or "").strip().lower()
        chat_id_value = getattr(source, "chat_id", None)
        if not platform or chat_id_value is None or not str(chat_id_value).strip():
            raise SubscriptionError("trusted source must provide platform and chat_id")
        thread_id = getattr(source, "thread_id", None)
        thread = str(thread_id) if thread_id is not None else None
        message_id = getattr(event, "message_id", None)
        message_id = str(message_id) if message_id is not None else None
        update_id = getattr(event, "platform_update_id", None)
        if update_id is not None:
            event_id = f"{platform}:{update_id}"
        elif message_id is not None:
            event_id = f"{platform}:{chat_id_value}:{thread or ''}:{message_id}"
        else:
            raise SubscriptionError("message event requires a trusted update or message id")
        media_urls = getattr(event, "media_urls", ()) or ()
        media_types = getattr(event, "media_types", ()) or ()
        attachments = tuple(
            AttachmentRef(reference=str(value), media_type=(str(media_types[index]) if index < len(media_types) else None))
            for index, value in enumerate(media_urls)
        )
        return cls(
            event_id=event_id,
            platform=platform,
            chat_id=str(chat_id_value),
            thread_id=thread,
            message_id=message_id,
            sender_id=(str(getattr(source, "user_id")) if getattr(source, "user_id", None) is not None else None),
            chat_type=getattr(source, "chat_type", None),
            kind="message",
            text=getattr(event, "text", None),
            reply_to_message_id=(str(getattr(event, "reply_to_message_id")) if getattr(event, "reply_to_message_id", None) is not None else None),
            attachments=attachments,
            received_at=getattr(event, "timestamp", datetime.now()),
        )


@dataclass(frozen=True)
class ConsumerDeclaration:
    """A named Phase 2 consumer claim declaration."""

    command_namespace: str | None = None
    callback_ownership: str | None = None
    priority: int = 0

    def __post_init__(self) -> None:
        declared = [value for value in (self.command_namespace, self.callback_ownership) if value is not None]
        if len(declared) != 1:
            raise SubscriptionError("consumer declaration must declare exactly one namespace")
        if self.command_namespace is not None and not re.fullmatch(r"[a-z][a-z0-9_-]*", self.command_namespace):
            raise SubscriptionError("consumer command namespace must be lowercase letters, digits, '_' or '-'")
        if self.callback_ownership is not None and not re.fullmatch(r"[a-z][a-z0-9_.-]*", self.callback_ownership):
            raise SubscriptionError("consumer callback ownership must be a stable namespace")


@dataclass(frozen=True)
class _Subscription:
    plugin_id: str
    subscription_id: str
    routes: frozenset[TopicRoute]
    event_types: frozenset[EventKind]
    mode: SubscriptionMode
    handler: EventHandler
    consumer: ConsumerDeclaration | None


class HostMessagingPermissions:
    """Deny-by-default inbound grants parsed from profile-scoped host config."""

    def __init__(self, inbound: Mapping[str, frozenset[tuple[TopicRoute, EventKind]]], outbound: Mapping[str, frozenset[TopicRoute]] | None = None) -> None:
        self._inbound = dict(inbound)
        self._outbound = dict(outbound or {})

    @classmethod
    def empty(cls) -> "HostMessagingPermissions":
        return cls({})

    @classmethod
    def from_raw(cls, config: Mapping[str, Any] | None) -> "HostMessagingPermissions":
        root = config.get("plugin_messaging", {}) if isinstance(config, Mapping) else {}
        if not isinstance(root, Mapping):
            return cls.empty()
        grants: dict[str, frozenset[tuple[TopicRoute, EventKind]]] = {}
        outbound_grants: dict[str, frozenset[TopicRoute]] = {}
        for plugin_id, plugin_config in root.items():
            if not isinstance(plugin_id, str) or not isinstance(plugin_config, Mapping):
                continue
            inbound = plugin_config.get("inbound", ())
            if not isinstance(inbound, list):
                inbound = []
            allowed: set[tuple[TopicRoute, EventKind]] = set()
            for item in inbound:
                if not isinstance(item, Mapping):
                    continue
                try:
                    route = TopicRoute(
                        platform=str(item["platform"]).strip().lower(),
                        chat_id=str(item["chat_id"]),
                        thread_id=(str(item["thread_id"]) if item.get("thread_id") is not None else None),
                    )
                except (KeyError, SubscriptionError):
                    continue
                events = item.get("events", ())
                if not isinstance(events, list):
                    continue
                for event_type in events:
                    if event_type in {"message", "callback"}:
                        allowed.add((route, event_type))
            grants[plugin_id] = frozenset(allowed)
            allowed_outbound: set[TopicRoute] = set()
            for item in plugin_config.get("outbound", ()) if isinstance(plugin_config.get("outbound", ()), list) else ():
                if not isinstance(item, Mapping) or "text" not in item.get("types", ()): continue
                try:
                    allowed_outbound.add(TopicRoute(str(item["platform"]).strip().lower(), str(item["chat_id"]), str(item["thread_id"]) if item.get("thread_id") is not None else None))
                except (KeyError, SubscriptionError):
                    continue
            outbound_grants[plugin_id] = frozenset(allowed_outbound)
        return cls(grants, outbound_grants)

    def allows(self, plugin_id: str, route: TopicRoute, event_type: EventKind) -> bool:
        return (route, event_type) in self._inbound.get(plugin_id, frozenset())
    def allows_outbound_text(self, plugin_id: str, route: TopicRoute) -> bool:
        return route in self._outbound.get(plugin_id, frozenset())


class PluginMessagingService:
    """PluginContext facade that binds registrations to manifest identity."""

    def __init__(self, *, plugin_id: str, router: PluginMessageRouter, enqueue_text: Callable[..., str] | None = None) -> None:
        self._plugin_id = plugin_id
        self._router = router
        self._enqueue_text = enqueue_text

    def subscribe(
        self,
        *,
        subscription_id: str,
        routes: list[TopicRoute] | tuple[TopicRoute, ...],
        event_types: set[EventKind] | frozenset[EventKind],
        mode: SubscriptionMode,
        handler: EventHandler,
        consumer: ConsumerDeclaration | None = None,
    ) -> None:
        """Request an inbound subscription under this plugin's manifest key."""
        self._router.subscribe(
            plugin_id=self._plugin_id,
            subscription_id=subscription_id,
            routes=routes,
            event_types=event_types,
            mode=mode,
            handler=handler,
            consumer=consumer,
        )

    def enqueue_text(self, *, idempotency_key: str, route: TopicRoute, text: str) -> str:
        """Request a durable host-validated text delivery; no adapter is exposed."""
        if self._enqueue_text is None:
            raise PermissionError("plugin outbound messaging is unavailable")
        return self._enqueue_text(idempotency_key=idempotency_key, route=route, text=text)


@dataclass(frozen=True)
class MessagingDispatchOutcome:
    """Host-owned Phase 2 routing result; only ``claim`` suppresses the agent."""

    observer_deliveries: int
    action: Literal["allow", "claim", "reject", "conflict", "error"]
    consumer_plugin_id: str | None = None
    audit_reason: str | None = None


class PluginMessageRouter:
    """Host-owned observer fan-out and deterministic Phase 2 consumer router."""

    def __init__(self, permissions: HostMessagingPermissions | None = None) -> None:
        self._permissions = permissions or HostMessagingPermissions.empty()
        self._subscriptions: dict[tuple[str, str], _Subscription] = {}

    @property
    def has_subscriptions(self) -> bool:
        """Whether any plugin has requested a Phase 1 subscription."""
        return bool(self._subscriptions)

    def set_permissions(self, permissions: HostMessagingPermissions) -> None:
        self._permissions = permissions

    def subscribe(
        self,
        *,
        plugin_id: str,
        subscription_id: str,
        routes: list[TopicRoute] | tuple[TopicRoute, ...],
        event_types: set[EventKind] | frozenset[EventKind],
        mode: SubscriptionMode,
        handler: EventHandler,
        consumer: ConsumerDeclaration | None = None,
    ) -> None:
        if not isinstance(plugin_id, str) or not plugin_id.strip():
            raise SubscriptionError("plugin_id must be a non-empty string")
        if not isinstance(subscription_id, str) or not subscription_id.strip():
            raise SubscriptionError("subscription_id must be a non-empty string")
        key = (plugin_id, subscription_id)
        if key in self._subscriptions:
            raise SubscriptionError(f"duplicate subscription {plugin_id}:{subscription_id}")
        route_set = frozenset(routes)
        if not route_set or not all(isinstance(route, TopicRoute) for route in route_set):
            raise SubscriptionError("subscription routes must contain one or more TopicRoute values")
        event_set = frozenset(event_types)
        if not event_set or not event_set.issubset({"message", "callback"}):
            raise SubscriptionError("subscription event_types must contain message and/or callback")
        if mode not in {"observer", "consumer"}:
            raise SubscriptionError("subscription mode must be observer or consumer")
        if not callable(handler):
            raise SubscriptionError("subscription handler must be callable")
        if mode == "consumer" and consumer is None:
            raise SubscriptionError("consumer declaration is required for consumer subscriptions")
        if mode == "observer" and consumer is not None:
            raise SubscriptionError("observer subscriptions cannot declare a consumer namespace")
        self._subscriptions[key] = _Subscription(
            plugin_id=plugin_id,
            subscription_id=subscription_id,
            routes=route_set,
            event_types=event_set,
            mode=mode,
            handler=handler,
            consumer=consumer,
        )

    def _eligible(self, envelope: PluginMessageEvent, mode: SubscriptionMode) -> list[_Subscription]:
        return [
            subscription for subscription in self._subscriptions.values()
            if subscription.mode == mode
            and envelope.route in subscription.routes
            and envelope.kind in subscription.event_types
            and self._permissions.allows(subscription.plugin_id, envelope.route, envelope.kind)
        ]

    @staticmethod
    def _command_matches(envelope: PluginMessageEvent, declaration: ConsumerDeclaration) -> bool:
        if declaration.command_namespace is not None:
            text = (envelope.text or "").strip()
            command = text[1:].split(maxsplit=1)[0].split("@", 1)[0].lower() if text.startswith("/") else ""
            return command == declaration.command_namespace
        # Callback transport is deferred; a future adapter must create kind=callback.
        return envelope.kind == "callback" and declaration.callback_ownership is not None

    async def route(self, event: Any) -> MessagingDispatchOutcome:
        """Fan out observers, then permit at most one exact authorized consumer claim."""
        envelope = event if isinstance(event, PluginMessageEvent) else PluginMessageEvent.from_message_event(event)
        delivered = 0
        for subscription in self._eligible(envelope, "observer"):
            try:
                result = subscription.handler(envelope)
                if inspect.isawaitable(result):
                    await result
                delivered += 1
            except Exception:
                # Observer failures are isolated; they cannot affect the agent or consumers.
                continue

        candidates = [
            subscription for subscription in self._eligible(envelope, "consumer")
            if subscription.consumer is not None and self._command_matches(envelope, subscription.consumer)
        ]
        if not candidates:
            return MessagingDispatchOutcome(delivered, "allow")
        highest = max(subscription.consumer.priority for subscription in candidates if subscription.consumer is not None)
        winners = [s for s in candidates if s.consumer is not None and s.consumer.priority == highest]
        if len(winners) != 1:
            return MessagingDispatchOutcome(delivered, "conflict", audit_reason="consumer-priority-conflict")
        winner = winners[0]
        try:
            result = winner.handler(envelope)
            if inspect.isawaitable(result):
                result = await result
        except Exception:
            return MessagingDispatchOutcome(delivered, "error", consumer_plugin_id=winner.plugin_id, audit_reason="consumer-error")
        action = result.get("action") if isinstance(result, Mapping) else result
        if action not in {"allow", "claim", "reject"}:
            return MessagingDispatchOutcome(delivered, "error", consumer_plugin_id=winner.plugin_id, audit_reason="invalid-consumer-outcome")
        return MessagingDispatchOutcome(delivered, action, consumer_plugin_id=winner.plugin_id)

    async def dispatch(self, event: Any) -> int:
        """Phase 1 compatibility facade returning observer delivery count only."""
        return (await self.route(event)).observer_deliveries
