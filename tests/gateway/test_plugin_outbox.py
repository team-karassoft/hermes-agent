"""RED contract tests for RFC 0001 Phase 3 plugin outbound intents.

These tests use an isolated delivery ledger and never invoke a real adapter.
"""

from __future__ import annotations

import asyncio
import sqlite3

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter", "expected_state"),
    [
        (_Adapter(SendResult(success=True)), "delivered"),
        (_Adapter(error=RuntimeError("transport down")), "failed"),
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
