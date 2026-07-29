"""PluginContext integration contract for scoped delivery binding."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.plugin_delivery import PluginOutboxRecord, PluginTopicRoute, ScopedPluginDelivery
from gateway.session import SessionSource
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest


def _event() -> MessageEvent:
    return MessageEvent(
        text="/debate",
        message_id="99",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            user_id="owner",
            chat_id="-1001",
            chat_type="forum",
            thread_id="42",
        ),
    )


@pytest.mark.asyncio
async def test_context_binds_outbox_capability_to_registering_plugin_only() -> None:
    manager = PluginManager()
    owner = PluginContext(PluginManifest(name="idea-incubator", source="user"), manager)
    other = PluginContext(PluginManifest(name="other-plugin", source="user"), manager)
    claimed = [PluginOutboxRecord(id="one", chat_id="-1001", thread_id="42", content="ready")]
    owner.register_topic_outbox_dispatcher(
        approved_routes=[PluginTopicRoute(chat_id="-1001", thread_id="42")],
        claim=lambda: claimed.pop(0) if claimed else None,
        mark_delivered=lambda _record_id, _reference: None,
        mark_failed=lambda _record_id, _reason: None,
    )
    received: dict[str, object] = {}

    def owner_hook(*, delivery):
        received["owner"] = delivery

    def other_hook(*, delivery):
        received["other"] = delivery

    owner.register_hook("pre_gateway_dispatch", owner_hook)
    other.register_hook("pre_gateway_dispatch", other_hook)
    adapter = SimpleNamespace(
        send=AsyncMock(return_value=SimpleNamespace(success=True, message_id="100"))
    )
    root = ScopedPluginDelivery(
        adapter=adapter,
        inbound_event=_event(),
        authorized=True,
        outbox_dispatchers=manager._plugin_topic_outbox_dispatchers,
    )

    manager.invoke_hook("pre_gateway_dispatch", plugin_delivery=root)

    assert await received["owner"].dispatch_outbox() == 1  # type: ignore[union-attr]
    with pytest.raises(PermissionError):
        await received["other"].dispatch_outbox()  # type: ignore[union-attr]
    adapter.send.assert_awaited_once()
