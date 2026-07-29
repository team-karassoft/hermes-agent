"""RED contract tests for RFC 0001 Phase 3 plugin outbound intents.

These tests use an isolated delivery ledger and never invoke a real adapter.
"""

from __future__ import annotations

import pytest
from gateway import delivery_ledger as dl

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
