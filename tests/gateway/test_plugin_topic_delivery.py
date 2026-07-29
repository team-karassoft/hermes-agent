"""TDD contracts for host-owned, topic-scoped plugin delivery."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.plugin_delivery import (
    PluginOutboxRecord,
    PluginTopicReply,
    PluginTopicRoute,
    ScopedPluginDelivery,
)
from gateway.session import SessionSource


def _topic_event(
    *,
    chat_id: str = "-1001",
    thread_id: str | None = "42",
    chat_type: str = "forum",
) -> MessageEvent:
    return MessageEvent(
        text="/debate",
        message_id="99",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            user_id="owner",
            chat_id=chat_id,
            chat_type=chat_type,
            thread_id=thread_id,
        ),
    )


@pytest.mark.asyncio
async def test_plugin_reply_routes_only_to_authenticated_inbound_forum_topic() -> None:
    adapter = SimpleNamespace(
        send=AsyncMock(return_value=SimpleNamespace(success=True, message_id="100"))
    )
    root = ScopedPluginDelivery(
        adapter=adapter,
        inbound_event=_topic_event(),
        authorized=True,
    )
    delivery = root.for_plugin("idea-incubator")

    request = delivery.reply("queued")
    adapter.send.assert_not_awaited()
    assert await root._deliver_reply(request) == "100"

    adapter.send.assert_awaited_once_with(
        chat_id="-1001",
        content="queued",
        reply_to=None,
        metadata={"thread_id": "42"},
    )


@pytest.mark.asyncio
async def test_host_rejects_reply_request_not_issued_to_a_plugin_capability() -> None:
    adapter = SimpleNamespace(send=AsyncMock())
    root = ScopedPluginDelivery(adapter=adapter, inbound_event=_topic_event(), authorized=True)

    with pytest.raises(PermissionError):
        await root._deliver_reply(PluginTopicReply(content="forged"))

    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_unauthorized_or_non_topic_event_cannot_send_a_reply() -> None:
    adapter = SimpleNamespace(send=AsyncMock())
    root = ScopedPluginDelivery(
        adapter=adapter,
        inbound_event=_topic_event(),
        authorized=False,
    )
    delivery = root.for_plugin("idea-incubator")

    with pytest.raises(PermissionError):
        await root._deliver_reply(delivery.reply("blocked"))
    with pytest.raises(ValueError):
        ScopedPluginDelivery(
            adapter=adapter,
            inbound_event=_topic_event(thread_id=None),
            authorized=True,
        )

    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_delivery_refuses_telegram_thread_that_is_not_a_forum_topic() -> None:
    adapter = SimpleNamespace(send=AsyncMock())

    with pytest.raises(ValueError):
        ScopedPluginDelivery(
            adapter=adapter,
            inbound_event=_topic_event(chat_type="group"),
            authorized=True,
        )

    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_outbox_rejects_cross_topic_record_before_any_send() -> None:
    adapter = SimpleNamespace(send=AsyncMock())
    settled: list[tuple[str, str, str]] = []
    delivery = ScopedPluginDelivery(
        adapter=adapter,
        inbound_event=_topic_event(),
        authorized=True,
    )
    delivery.register_outbox_dispatcher(
        plugin_id="idea-incubator",
        approved_routes=[PluginTopicRoute(chat_id="-1001", thread_id="42")],
        claim=lambda: PluginOutboxRecord(
            id="wrong-topic", chat_id="-1001", thread_id="43", content="blocked"
        ),
        mark_delivered=lambda record_id, reference: settled.append(("delivered", record_id, reference)),
        mark_failed=lambda record_id, reason: settled.append(("failed", record_id, reason)),
    )

    assert await delivery.for_plugin("idea-incubator").dispatch_outbox() == 1
    assert settled == [("failed", "wrong-topic", "unapproved-route")]
    adapter.send.assert_not_awaited()
