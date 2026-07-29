"""Contract tests for Phase 1 of the plugin messaging bus.

These tests primarily exercise the host-owned router directly.  The config
propagation regression loads a temporary profile through the real read-only
loader; no real plugin is loaded.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.plugin_messaging import (
    ConsumerDeclaration,
    HostMessagingPermissions,
    PluginMessageEvent,
    PluginMessageRouter,
    SubscriptionError,
    TopicRoute,
)
from gateway.session import SessionSource


APPROVED_TOPIC = TopicRoute(platform="telegram", chat_id="-100123", thread_id="42")
OTHER_TOPIC = TopicRoute(platform="telegram", chat_id="-100123", thread_id="43")


def _permissions(*plugin_ids: str) -> HostMessagingPermissions:
    return HostMessagingPermissions.from_raw(
        {
            "plugin_messaging": {
                plugin_id: {
                    "inbound": [
                        {
                            "platform": APPROVED_TOPIC.platform,
                            "chat_id": APPROVED_TOPIC.chat_id,
                            "thread_id": APPROVED_TOPIC.thread_id,
                            "events": ["message"],
                        }
                    ]
                }
                for plugin_id in plugin_ids
            }
        }
    )


def test_profile_config_round_trips_exact_messaging_grants_through_loader(
    tmp_path, monkeypatch, capsys
) -> None:
    from hermes_cli.config import load_config_readonly, set_config_value

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    # Exercise repeated real config writes from an entirely absent open path.
    base = "plugin_messaging.route-auditor.outbound.0"
    set_config_value(f"{base}.platform", "telegram")
    set_config_value(f"{base}.chat_id", APPROVED_TOPIC.chat_id)
    set_config_value(f"{base}.thread_id", APPROVED_TOPIC.thread_id)
    set_config_value(f"{base}.types.0", "text")
    loaded = load_config_readonly()
    permissions = HostMessagingPermissions.from_raw(loaded)

    assert loaded["plugin_messaging"]["route-auditor"]["outbound"] == [
        {
            "platform": "telegram",
            "chat_id": APPROVED_TOPIC.chat_id,
            "thread_id": 42,
            "types": ["text"],
        }
    ]
    assert permissions.allows_outbound_text("route-auditor", APPROVED_TOPIC)
    assert not permissions.allows_outbound_text("route-auditor", OTHER_TOPIC)
    assert "not a recognized config key" not in capsys.readouterr().out


def _trusted_event(*, thread_id: str | None = APPROVED_TOPIC.thread_id) -> MessageEvent:
    return MessageEvent(
        text="evidence",
        message_id="m-1",
        platform_update_id=77,
        source=SessionSource(
            platform=Platform.TELEGRAM,
            chat_id=APPROVED_TOPIC.chat_id,
            thread_id=thread_id,
            user_id="user-9",
            chat_type="group",
        ),
        raw_message={
            "platform": "discord",
            "chat_id": "attacker-chat",
            "thread_id": "attacker-thread",
            "sender_id": "attacker",
        },
        timestamp=datetime(2026, 7, 29, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_two_approved_observers_receive_same_exact_topic_event() -> None:
    received_one: list[PluginMessageEvent] = []
    received_two: list[PluginMessageEvent] = []
    router = PluginMessageRouter(_permissions("observer-one", "observer-two"))

    router.subscribe(
        plugin_id="observer-one",
        subscription_id="topic-observer",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received_one.append,
    )
    router.subscribe(
        plugin_id="observer-two",
        subscription_id="topic-observer",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received_two.append,
    )

    delivered = await router.dispatch(_trusted_event())

    assert delivered == 2
    assert received_one == received_two
    assert received_one[0].route == APPROVED_TOPIC
    with pytest.raises(FrozenInstanceError):
        received_one[0].text = "mutated"  # type: ignore[misc]


@pytest.mark.asyncio
async def test_unapproved_subscription_receives_no_events() -> None:
    received: list[PluginMessageEvent] = []
    router = PluginMessageRouter(_permissions("approved-plugin"))
    router.subscribe(
        plugin_id="unapproved-plugin",
        subscription_id="unapproved",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received.append,
    )

    assert await router.dispatch(_trusted_event()) == 0
    assert received == []


@pytest.mark.asyncio
async def test_route_matching_requires_exact_thread_identity() -> None:
    received: list[PluginMessageEvent] = []
    router = PluginMessageRouter(_permissions("observer"))
    router.subscribe(
        plugin_id="observer",
        subscription_id="topic-only",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received.append,
    )

    assert await router.dispatch(_trusted_event(thread_id=OTHER_TOPIC.thread_id)) == 0
    assert received == []


@pytest.mark.asyncio
async def test_envelope_uses_only_trusted_message_source_identity() -> None:
    received: list[PluginMessageEvent] = []
    router = PluginMessageRouter(_permissions("observer"))
    router.subscribe(
        plugin_id="observer",
        subscription_id="trusted-source-only",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received.append,
    )

    assert await router.dispatch(_trusted_event()) == 1
    envelope = received[0]
    assert envelope.platform == "telegram"
    assert envelope.chat_id == APPROVED_TOPIC.chat_id
    assert envelope.thread_id == APPROVED_TOPIC.thread_id
    assert envelope.sender_id == "user-9"
    assert envelope.event_id == "telegram:77"


@pytest.mark.asyncio
async def test_plugin_context_subscription_uses_manifest_identity_only() -> None:
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

    received: list[PluginMessageEvent] = []
    manager = PluginManager()
    context = PluginContext(
        PluginManifest(name="display-name", key="trusted-plugin"), manager
    )

    context.messaging.subscribe(
        subscription_id="manifest-bound",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received.append,
    )
    manager.messaging_router.set_permissions(_permissions("trusted-plugin"))

    assert await manager.messaging_router.dispatch(_trusted_event()) == 1
    assert len(received) == 1


@pytest.mark.asyncio
async def test_no_subscription_does_not_read_host_config(monkeypatch) -> None:
    from hermes_cli.plugins import PluginManager

    def _unexpected_config_read():
        raise AssertionError("no messaging subscription must not read host config")

    monkeypatch.setattr("hermes_cli.config.load_config_readonly", _unexpected_config_read)

    assert await PluginManager().dispatch_messaging_event(_trusted_event()) == 0


@pytest.mark.asyncio
async def test_gateway_observes_after_legacy_hook_without_changing_agent_dispatch(monkeypatch) -> None:
    from hermes_cli import plugins as plugins_module
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    from gateway.run import GatewayRunner

    received: list[PluginMessageEvent] = []
    manager = PluginManager()
    PluginContext(PluginManifest(name="observer", key="observer"), manager).messaging.subscribe(
        subscription_id="topic-observer",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=received.append,
    )
    monkeypatch.setattr(plugins_module, "_plugin_manager", manager)
    monkeypatch.setattr(
        "hermes_cli.config.load_config_readonly",
        lambda: {
            "plugin_messaging": {
                "observer": {
                    "inbound": [
                        {
                            "platform": "telegram",
                            "chat_id": APPROVED_TOPIC.chat_id,
                            "thread_id": APPROVED_TOPIC.thread_id,
                            "events": ["message"],
                        }
                    ]
                }
            }
        },
    )
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", lambda *args, **kwargs: [{"action": "allow"}])
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "*")

    runner = object.__new__(GatewayRunner)
    runner.config = type("Config", (), {"platforms": {Platform.TELEGRAM: object()}})()
    runner.adapters = {Platform.TELEGRAM: object()}
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    runner._scale_to_zero_note_real_inbound = lambda: None

    agent_calls: list[str] = []

    async def _agent(event, source, quick_key, generation):
        agent_calls.append(event.text)
        return "normal-dispatch"

    runner._handle_message_with_agent = _agent

    assert await runner._handle_message(_trusted_event()) == "normal-dispatch"
    assert [event.text for event in received] == ["evidence"]
    assert agent_calls == ["evidence"]


def test_duplicate_subscription_id_fails() -> None:
    router = PluginMessageRouter(_permissions("observer"))
    handler = lambda event: None
    router.subscribe(
        plugin_id="observer",
        subscription_id="duplicate",
        routes=[APPROVED_TOPIC],
        event_types={"message"},
        mode="observer",
        handler=handler,
    )

    with pytest.raises(SubscriptionError, match="duplicate"):
        router.subscribe(
            plugin_id="observer",
            subscription_id="duplicate",
            routes=[APPROVED_TOPIC],
            event_types={"message"},
            mode="observer",
            handler=handler,
        )


def test_consumer_requires_a_valid_namespace_declaration() -> None:
    router = PluginMessageRouter(_permissions("consumer"))
    handler = lambda event: None

    with pytest.raises(SubscriptionError, match="consumer declaration"):
        router.subscribe(
            plugin_id="consumer",
            subscription_id="missing-declaration",
            routes=[APPROVED_TOPIC],
            event_types={"message"},
            mode="consumer",
            handler=handler,
        )



@pytest.mark.asyncio
async def test_consumer_priority_claims_after_observers() -> None:
    observed: list[str] = []
    called: list[str] = []
    router = PluginMessageRouter(_permissions("observer", "low", "high"))
    router.subscribe(plugin_id="observer", subscription_id="audit", routes=[APPROVED_TOPIC], event_types={"message"}, mode="observer", handler=lambda event: observed.append(event.text or ""))
    for plugin_id, priority in (("low", 1), ("high", 10)):
        router.subscribe(
            plugin_id=plugin_id, subscription_id="consume", routes=[APPROVED_TOPIC], event_types={"message"}, mode="consumer",
            handler=lambda event, p=plugin_id: (called.append(p) or {"action": "claim"}),
            consumer=ConsumerDeclaration(command_namespace="idea", priority=priority),
        )
    event = _trusted_event()
    event.text = "/idea"
    outcome = await router.route(event)
    assert outcome.action == "claim"
    assert outcome.consumer_plugin_id == "high"
    assert observed == ["/idea"]
    assert called == ["high"]


@pytest.mark.asyncio
async def test_equal_priority_conflict_and_consumer_error_fail_open() -> None:
    router = PluginMessageRouter(_permissions("one", "two"))
    for plugin_id in ("one", "two"):
        router.subscribe(
            plugin_id=plugin_id, subscription_id="consume", routes=[APPROVED_TOPIC], event_types={"message"}, mode="consumer",
            handler=lambda event: {"action": "claim"}, consumer=ConsumerDeclaration(command_namespace="idea", priority=1),
        )
    event = _trusted_event(); event.text = "/idea"
    assert (await router.route(event)).action == "conflict"

    broken = PluginMessageRouter(_permissions("broken"))
    def _raise(event): raise RuntimeError("no leak")
    broken.subscribe(plugin_id="broken", subscription_id="consume", routes=[APPROVED_TOPIC], event_types={"message"}, mode="consumer", handler=_raise, consumer=ConsumerDeclaration(command_namespace="idea"))
    assert (await broken.route(event)).action == "error"


@pytest.mark.asyncio
async def test_consumer_allow_and_reject_never_claim() -> None:
    for action in ("allow", "reject"):
        router = PluginMessageRouter(_permissions("consumer"))
        router.subscribe(plugin_id="consumer", subscription_id=action, routes=[APPROVED_TOPIC], event_types={"message"}, mode="consumer", handler=lambda event, a=action: {"action": a}, consumer=ConsumerDeclaration(command_namespace="idea"))
        event = _trusted_event(); event.text = "/idea"
        assert (await router.route(event)).action == action
