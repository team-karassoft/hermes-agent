"""Host-owned delivery capability scoped to authenticated Telegram topics.

Plugins only receive ``PluginTopicDelivery`` through PluginContext hook binding.
It exposes an immediate reply pinned to the current authenticated topic and a
bounded outbox dispatcher for that plugin's explicit, pre-approved routes.
Neither API exposes an adapter, credential, or arbitrary destination.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


@dataclass(frozen=True)
class PluginTopicRoute:
    """An exact Telegram forum route approved during plugin registration."""

    chat_id: str
    thread_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.chat_id, str) or not self.chat_id:
            raise ValueError("chat_id is required")
        if not isinstance(self.thread_id, str) or not self.thread_id:
            raise ValueError("thread_id is required")


@dataclass(frozen=True)
class PluginOutboxRecord:
    """A plugin-owned record already durably claimed for host delivery."""

    id: str
    chat_id: str
    thread_id: str
    content: str


@dataclass(frozen=True)
class PluginTopicReply:
    """An opaque, target-free immediate-reply request for GatewayRunner."""

    content: str
    _token: object | None = None


@dataclass(frozen=True)
class _RegisteredOutboxDispatcher:
    approved_routes: frozenset[PluginTopicRoute]
    claim: Callable[[], Optional[PluginOutboxRecord]]
    mark_delivered: Callable[[str, str], None]
    mark_failed: Callable[[str, str], None]


def validate_outbox_dispatcher(
    *,
    approved_routes: Iterable[PluginTopicRoute],
    claim: Callable[[], Optional[PluginOutboxRecord]],
    mark_delivered: Callable[[str, str], None],
    mark_failed: Callable[[str, str], None],
) -> _RegisteredOutboxDispatcher:
    """Validate a durable plugin outbox contract at registration time."""
    routes = frozenset(approved_routes)
    if not routes or not all(isinstance(route, PluginTopicRoute) for route in routes):
        raise ValueError("approved_routes must contain PluginTopicRoute values")
    if not all(callable(callback) for callback in (claim, mark_delivered, mark_failed)):
        raise TypeError("outbox dispatcher callbacks must be callable")
    return _RegisteredOutboxDispatcher(routes, claim, mark_delivered, mark_failed)


class PluginTopicDelivery:
    """Private plugin-bound view; it never exposes a route or adapter."""

    def __init__(self, host: "ScopedPluginDelivery", plugin_id: str) -> None:
        self._host = host
        self._plugin_id = plugin_id

    def reply(self, content: str) -> PluginTopicReply:
        """Return a target-free reply request for GatewayRunner to deliver."""
        if not isinstance(content, str) or not content.strip():
            raise ValueError("content must be a non-empty string")
        return self._host._issue_reply(self._plugin_id, content)

    async def dispatch_outbox(self, *, max_records: int = 1) -> int:
        """Dispatch at most ``max_records`` from this plugin's durable outbox."""
        return await self._host._dispatch_outbox(self._plugin_id, max_records=max_records)


class ScopedPluginDelivery:
    """Host-only root constructed from a trusted inbound forum-topic event."""

    _MAX_OUTBOX_RECORDS_PER_DISPATCH = 32

    def __init__(
        self,
        *,
        adapter: Any,
        inbound_event: MessageEvent,
        authorized: bool,
        outbox_dispatchers: Optional[Mapping[str, _RegisteredOutboxDispatcher]] = None,
    ) -> None:
        if not isinstance(inbound_event, MessageEvent):
            raise TypeError("inbound_event must be a MessageEvent")
        if not isinstance(inbound_event.source, SessionSource):
            raise TypeError("inbound_event.source must be a SessionSource")
        source = inbound_event.source
        if (
            source.platform != Platform.TELEGRAM
            or source.chat_type != "forum"
            or not source.chat_id
            or not source.thread_id
        ):
            raise ValueError("plugin delivery requires an inbound Telegram forum topic")
        if adapter is None or not callable(getattr(adapter, "send", None)):
            raise TypeError("plugin delivery requires a live platform adapter")
        self._adapter = adapter
        self._authorized = authorized is True
        self._inbound_route = PluginTopicRoute(str(source.chat_id), str(source.thread_id))
        self._dispatchers = dict(outbox_dispatchers or {})
        self._issued_reply_tokens: dict[object, str] = {}

    def for_plugin(self, plugin_id: str) -> PluginTopicDelivery:
        if not isinstance(plugin_id, str) or not plugin_id:
            raise ValueError("plugin_id is required")
        return PluginTopicDelivery(self, plugin_id)

    def register_outbox_dispatcher(
        self,
        *,
        plugin_id: str,
        approved_routes: Iterable[PluginTopicRoute],
        claim: Callable[[], Optional[PluginOutboxRecord]],
        mark_delivered: Callable[[str, str], None],
        mark_failed: Callable[[str, str], None],
    ) -> None:
        """Host-only helper retained for integration construction and tests."""
        if not isinstance(plugin_id, str) or not plugin_id:
            raise ValueError("plugin_id is required")
        if plugin_id in self._dispatchers:
            raise ValueError(f"outbox dispatcher already registered for {plugin_id}")
        self._dispatchers[plugin_id] = validate_outbox_dispatcher(
            approved_routes=approved_routes,
            claim=claim,
            mark_delivered=mark_delivered,
            mark_failed=mark_failed,
        )

    def _issue_reply(self, plugin_id: str, content: str) -> PluginTopicReply:
        """Issue a single-use request bound to this root and calling plugin."""
        token = object()
        self._issued_reply_tokens[token] = plugin_id
        return PluginTopicReply(content=content, _token=token)

    async def _deliver_reply(self, request: PluginTopicReply) -> str:
        """GatewayRunner-only send path for a bound, target-free reply request."""
        if not isinstance(request, PluginTopicReply):
            raise TypeError("request must be a PluginTopicReply")
        if request._token not in self._issued_reply_tokens:
            raise PermissionError("reply request was not issued by this plugin capability")
        self._issued_reply_tokens.pop(request._token)
        self._require_authorized()
        return await self._send(self._inbound_route, request.content)

    async def _dispatch_outbox(self, plugin_id: str, *, max_records: int) -> int:
        self._require_authorized()
        if not isinstance(max_records, int) or not 1 <= max_records <= self._MAX_OUTBOX_RECORDS_PER_DISPATCH:
            raise ValueError(f"max_records must be between 1 and {self._MAX_OUTBOX_RECORDS_PER_DISPATCH}")
        dispatcher = self._dispatchers.get(plugin_id)
        if dispatcher is None:
            raise PermissionError("plugin has no registered outbox dispatcher")

        processed = 0
        for _ in range(max_records):
            record = dispatcher.claim()
            if record is None:
                break
            processed += 1
            if not isinstance(record, PluginOutboxRecord):
                raise TypeError("outbox claim must return PluginOutboxRecord or None")
            try:
                route = PluginTopicRoute(str(record.chat_id), str(record.thread_id))
            except ValueError:
                dispatcher.mark_failed(record.id, "invalid-route")
                continue
            if route not in dispatcher.approved_routes:
                dispatcher.mark_failed(record.id, "unapproved-route")
                continue
            try:
                reference = await self._send(route, record.content)
            except Exception as exc:
                dispatcher.mark_failed(record.id, type(exc).__name__)
            else:
                dispatcher.mark_delivered(record.id, reference)
        return processed

    def _require_authorized(self) -> None:
        if not self._authorized:
            raise PermissionError("plugin delivery requires an authenticated inbound event")

    async def _send(self, route: PluginTopicRoute, content: str) -> str:
        if not isinstance(content, str) or not content.strip():
            raise ValueError("content must be a non-empty string")
        result = await self._adapter.send(
            chat_id=route.chat_id,
            content=content,
            reply_to=None,
            metadata={"thread_id": route.thread_id},
        )
        if not getattr(result, "success", False):
            raise RuntimeError(getattr(result, "error", None) or "platform delivery failed")
        return str(getattr(result, "message_id", ""))
