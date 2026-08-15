"""RED contract tests for RFC 0001 Phase 3 plugin outbound intents.

These tests use an isolated delivery ledger and never invoke a real adapter.
"""

from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway import delivery_ledger as dl

from gateway.platforms.base import Platform, SendResult
from gateway.plugin_messaging import HostMessagingPermissions, TopicRoute
from gateway.plugin_outbox import PluginOutboundIntent, PluginOutboxService, OutboundPermissionError


ROUTE = TopicRoute(platform="telegram", chat_id="-100123", thread_id="42")


@pytest.fixture(autouse=True)
def _isolated_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "_db_path", lambda: tmp_path / "state.db")


def _permissions(plugin_id: str = "idea-incubator") -> HostMessagingPermissions:
    return HostMessagingPermissions.from_raw(
        {
            "plugin_messaging": {
                plugin_id: {
                    "outbound": [
                        {
                            "platform": "telegram",
                            "chat_id": "-100123",
                            "thread_id": "42",
                            "types": ["text"],
                        }
                    ]
                }
            }
        }
    )


def _state(obligation_id: str) -> str:
    with sqlite3.connect(dl._db_path()) as conn:
        row = conn.execute(
            "SELECT state FROM delivery_obligations WHERE obligation_id=?",
            (obligation_id,),
        ).fetchone()
    assert row is not None
    return row[0]


def test_unapproved_plugin_or_route_cannot_enqueue() -> None:
    service = PluginOutboxService(_permissions())
    intent = PluginOutboundIntent(
        idempotency_key="idea:1:queued",
        route=ROUTE,
        text="Queued",
    )
    service.enqueue(plugin_id="idea-incubator", intent=intent)

    try:
        service.enqueue(plugin_id="other-plugin", intent=intent)
    except OutboundPermissionError:
        pass
    else:
        raise AssertionError("unapproved plugin must not enqueue")


def test_idempotency_key_produces_one_durable_plugin_obligation() -> None:
    service = PluginOutboxService(_permissions())
    intent = PluginOutboundIntent(
        idempotency_key="idea:1:result",
        route=ROUTE,
        text="Result",
    )
    first = service.enqueue(plugin_id="idea-incubator", intent=intent)
    second = service.enqueue(plugin_id="idea-incubator", intent=intent)
    assert first == second


class _Adapter:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.result

    def set_plugin_callback_router(self, router) -> None:
        self.plugin_callback_router = router


def _manager_with_config(monkeypatch):
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    monkeypatch.setattr(
        "hermes_cli.config.load_config_readonly",
        lambda: {
            "plugin_messaging": {
                "idea-incubator": {
                    "outbound": [
                        {
                            "platform": "telegram",
                            "chat_id": ROUTE.chat_id,
                            "thread_id": ROUTE.thread_id,
                            "types": ["text"],
                        }
                    ]
                }
            }
        },
    )
    return manager


def test_gateway_binds_callback_router_to_capable_adapter(monkeypatch) -> None:
    from gateway.run import GatewayRunner

    adapter = _Adapter(SendResult(success=True))
    manager = _manager_with_config(monkeypatch)
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._background_tasks = set()

    runner._bind_plugin_messaging_dispatcher(manager)

    assert adapter.plugin_callback_router.__self__ is manager
    assert adapter.plugin_callback_router.__func__ is manager.route_plugin_callback.__func__


@pytest.mark.asyncio
async def test_gateway_bound_telegram_adapter_invokes_manager_callback(monkeypatch) -> None:
    from gateway.platforms.base import PlatformConfig
    from gateway.run import GatewayRunner
    from plugins.platforms.telegram.adapter import TelegramAdapter

    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="test", extra={}))
    adapter._is_callback_user_authorized = lambda *args, **kwargs: True
    manager = _manager_with_config(monkeypatch)
    manager.route_plugin_callback = AsyncMock(return_value=SimpleNamespace(action="reject"))
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._background_tasks = set()

    runner._bind_plugin_messaging_dispatcher(manager)
    assert adapter._plugin_callback_router is manager.route_plugin_callback
    query = SimpleNamespace(
        data="pc1.opaque.signature",
        message=SimpleNamespace(chat_id=-100123, message_id="sent-99", message_thread_id=42, chat=SimpleNamespace(type="supergroup")),
        from_user=SimpleNamespace(id="actor-1", first_name="Owner"),
        answer=AsyncMock(),
    )
    await adapter._handle_callback_query(SimpleNamespace(callback_query=query), None)

    manager.route_plugin_callback.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter", "expected_state"),
    [
        (_Adapter(SendResult(success=True, message_id="sent-1")), "delivered"),
        (_Adapter(error=RuntimeError("transport down")), "attempting"),
        (_Adapter(SendResult(success=False, error="rejected")), "failed"),
    ],
)
async def test_gateway_immediate_dispatch_settles_persisted_intent(
    monkeypatch, adapter, expected_state
) -> None:
    from gateway.run import GatewayRunner

    manager = _manager_with_config(monkeypatch)
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._background_tasks = set()
    runner._bind_plugin_messaging_dispatcher(manager)

    obligation_id = manager.enqueue_plugin_text(
        plugin_id="idea-incubator",
        idempotency_key=f"settle:{expected_state}:{type(adapter.error).__name__}",
        route=ROUTE,
        text="Persist before send",
    )

    assert _state(obligation_id) == "pending"
    assert len(runner._background_tasks) == 1
    await asyncio.gather(*runner._background_tasks)

    assert _state(obligation_id) == expected_state
    assert adapter.calls == [
        {
            "chat_id": ROUTE.chat_id,
            "content": "Persist before send",
            "reply_to": None,
            "metadata": {"thread_id": ROUTE.thread_id},
        }
    ]
    assert runner._background_tasks == set()


@pytest.mark.asyncio
async def test_permission_denial_and_idempotency_do_not_spawn_dispatch_tasks(
    monkeypatch,
) -> None:
    from gateway.run import GatewayRunner
    from hermes_cli.plugins import PluginContext, PluginManifest

    adapter = _Adapter(SendResult(success=True))
    manager = _manager_with_config(monkeypatch)
    approved = PluginContext(
        PluginManifest(name="display-name", key="idea-incubator"), manager
    )
    denied = PluginContext(
        PluginManifest(name="display-name", key="not-the-manifest"), manager
    )
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._background_tasks = set()
    runner._bind_plugin_messaging_dispatcher(manager)

    with pytest.raises(OutboundPermissionError):
        denied.messaging.enqueue_text(
            idempotency_key="denied",
            route=ROUTE,
            text="No",
        )
    assert runner._background_tasks == set()

    first = approved.messaging.enqueue_text(
        idempotency_key="same",
        route=ROUTE,
        text="Once",
    )
    second = approved.messaging.enqueue_text(
        idempotency_key="same",
        route=ROUTE,
        text="Once",
    )
    assert first == second
    assert len(runner._background_tasks) == 1
    await asyncio.gather(*runner._background_tasks)
    assert len(adapter.calls) == 1


@pytest.mark.asyncio
async def test_manifest_bound_delivery_confirmation_is_safe_exact_and_idempotent(
    monkeypatch,
) -> None:
    from hermes_cli.plugins import PluginContext, PluginManifest

    manager = _manager_with_config(monkeypatch)
    context = PluginContext(
        PluginManifest(name="display-name", key="idea-incubator"), manager
    )
    received = []
    context.messaging.register_delivery_confirmation(
        idempotency_key="confirmed-once",
        route=ROUTE,
        handler=received.append,
    )
    intent = PluginOutboundIntent("confirmed-once", ROUTE, "Delivered")
    service = PluginOutboxService(_permissions())
    obligation_id = service.enqueue(plugin_id="idea-incubator", intent=intent)
    adapter = _Adapter(SendResult(success=True, message_id="platform-42"))

    assert await service.deliver_persisted(
        adapter=adapter,
        obligation_id=obligation_id,
        intent=intent,
        plugin_id="idea-incubator",
        confirmation_notifier=manager.notify_plugin_delivery_confirmation,
    )
    # A retry of the host settlement path must not invoke the plugin twice.
    assert await service.deliver_persisted(
        adapter=adapter,
        obligation_id=obligation_id,
        intent=intent,
        plugin_id="idea-incubator",
        confirmation_notifier=manager.notify_plugin_delivery_confirmation,
    )

    assert len(received) == 1
    confirmation = received[0]
    assert confirmation.state == "delivered"
    assert confirmation.message_id == "platform-42"
    assert vars(confirmation) == {
        "state": "delivered",
        "message_id": "platform-42",
        "confirmation_id": confirmation.confirmation_id,
    }
    from gateway.plugin_messaging import delivery_confirmation_id
    assert confirmation.confirmation_id == delivery_confirmation_id(
        obligation_id, "platform-42"
    )
    # A plugin that did not request a confirmation must not leave every
    # ordinary outbound delivery permanently replay-pending; it also must not
    # observe this other exact route.
    assert await manager.notify_plugin_delivery_confirmation(
        plugin_id="idea-incubator",
        idempotency_key="confirmed-once",
        route=TopicRoute("telegram", ROUTE.chat_id, "different-thread"),
        message_id="platform-42",
        obligation_id=obligation_id,
    )
    assert len(received) == 1


@pytest.mark.asyncio
async def test_success_without_actual_adapter_message_id_is_not_confirmed(
    monkeypatch,
) -> None:
    manager = _manager_with_config(monkeypatch)
    manager.notify_plugin_delivery_confirmation = AsyncMock(return_value=True)
    intent = PluginOutboundIntent("missing-id", ROUTE, "Not confirmed")
    service = PluginOutboxService(_permissions())
    obligation_id = service.enqueue(plugin_id="idea-incubator", intent=intent)

    assert not await service.deliver_persisted(
        adapter=_Adapter(SendResult(success=True)),
        obligation_id=obligation_id,
        intent=intent,
        plugin_id="idea-incubator",
        confirmation_notifier=manager.notify_plugin_delivery_confirmation,
    )
    assert _state(obligation_id) == "failed"
    manager.notify_plugin_delivery_confirmation.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_workers_only_one_sends_plugin_obligation() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingAdapter(_Adapter):
        async def send(self, **kwargs):
            self.calls.append(kwargs)
            entered.set()
            await release.wait()
            return SendResult(success=True, message_id="only-send")

    service = PluginOutboxService(_permissions())
    intent = PluginOutboundIntent("send-race", ROUTE, "Once")
    obligation_id = service.enqueue(plugin_id="idea-incubator", intent=intent)
    adapter = BlockingAdapter()
    first = asyncio.create_task(service.deliver_persisted(
        adapter=adapter, obligation_id=obligation_id, intent=intent,
        plugin_id="idea-incubator",
    ))
    await entered.wait()
    second = asyncio.create_task(service.deliver_persisted(
        adapter=adapter, obligation_id=obligation_id, intent=intent,
        plugin_id="idea-incubator",
    ))
    release.set()

    assert await asyncio.gather(first, second) == [True, False]
    assert len(adapter.calls) == 1


def test_confirmation_lease_expiry_replays_with_stale_token_fenced() -> None:
    service = PluginOutboxService(_permissions())
    intent = PluginOutboundIntent("confirmation-lease", ROUTE, "Delivered")
    obligation_id = service.enqueue(plugin_id="idea-incubator", intent=intent)
    send_token = dl.claim_plugin_send(obligation_id, now=1, lease_seconds=60)
    assert send_token is not None
    # Use real time for begin because its expiry guard is intentionally based
    # on the process clock; then bind delivery with the fenced token.
    with sqlite3.connect(dl._db_path()) as conn:
        conn.execute(
            "UPDATE delivery_obligations SET send_claim_expires=? WHERE obligation_id=?",
            (10**12, obligation_id),
        )
    assert dl.begin_plugin_send(obligation_id, send_token)
    assert dl.confirm_plugin_delivery(obligation_id, "delivered-1", send_token)

    first = dl.claim_plugin_confirmation(
        obligation_id, "delivered-1", now=10, lease_seconds=60,
    )
    assert first is not None
    assert dl.claim_plugin_confirmation(obligation_id, "delivered-1", now=69) is None
    with sqlite3.connect(dl._db_path()) as conn:
        conn.execute(
            """UPDATE delivery_obligations
               SET confirmation_claim_owner_pid=999999999,
                   confirmation_claim_owner_started_at=1
               WHERE obligation_id=?""",
            (obligation_id,),
        )
    replay = dl.claim_plugin_confirmation(obligation_id, "delivered-1", now=70)
    assert replay is not None and replay != first
    assert not dl.mark_confirmation_notified(obligation_id, "delivered-1", first)
    assert dl.mark_confirmation_notified(obligation_id, "delivered-1", replay)
