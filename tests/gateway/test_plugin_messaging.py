"""Contract tests for Phase 1 of the plugin messaging bus.

These tests primarily exercise the host-owned router directly.  The config
propagation regression loads a temporary profile through the real read-only
loader; no real plugin is loaded.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
import shutil
import subprocess

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


def _gateway_runner_for_messaging():
    """Build the smallest real GatewayRunner path up to slash fallback."""
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = type("Config", (), {"platforms": {Platform.TELEGRAM: object()}})()
    runner.adapters = {Platform.TELEGRAM: object()}
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    runner._scale_to_zero_note_real_inbound = lambda: None
    return runner


def _install_messaging_manager(monkeypatch, manager) -> None:
    from hermes_cli import plugins as plugins_module

    monkeypatch.setattr(plugins_module, "_plugin_manager", manager)
    monkeypatch.setattr(
        "hermes_cli.config.load_config_readonly",
        lambda: {
            "plugin_messaging": {
                plugin_id: {
                    "inbound": [{
                        "platform": "telegram", "chat_id": APPROVED_TOPIC.chat_id,
                        "thread_id": APPROVED_TOPIC.thread_id, "events": ["message"],
                    }]
                }
                for plugin_id in ("consumer", "other")
            }
        },
    )
    monkeypatch.setattr("hermes_cli.lifecycle.invoke_hook", lambda *args, **kwargs: [])
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "*")


def _subscribe_gateway_consumer(
    manager,
    handler,
    *,
    route=APPROVED_TOPIC,
    plugin_id="consumer",
    subscription_id="gateway-restart",
    command_namespace="gateway-restart",
    hash_command_namespace=None,
) -> None:
    from hermes_cli.plugins import PluginContext, PluginManifest

    PluginContext(PluginManifest(name=plugin_id, key=plugin_id), manager).messaging.subscribe(
        subscription_id=subscription_id, routes=[route], event_types={"message"},
        mode="consumer", handler=handler,
        consumer=ConsumerDeclaration(
            command_namespace=command_namespace,
            hash_command_namespace=hash_command_namespace,
        ),
    )


def _gateway_restart_event() -> MessageEvent:
    event = _trusted_event()
    event.text = "/gateway-restart"
    return event


def _gateway_hash_restart_event(text="#gateway-restart default") -> MessageEvent:
    event = _trusted_event()
    event.text = text
    return event


def _subscribe_gateway_hash_consumer(manager, handler, **kwargs) -> None:
    kwargs.setdefault("command_namespace", None)
    kwargs.setdefault("hash_command_namespace", "gateway-restart")
    _subscribe_gateway_consumer(manager, handler, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ("claim", "reject"))
async def test_gateway_exact_hash_consumer_terminal_actions_precede_agent(monkeypatch, action) -> None:
    """An authorized exact hash namespace can terminally own only its declared command."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_hash_consumer(manager, lambda event: {"action": action})
    _install_messaging_manager(monkeypatch, manager)
    runner = _gateway_runner_for_messaging()
    agent_calls: list[str] = []

    async def _agent(event, source, quick_key, generation):
        agent_calls.append(event.text)
        return "normal-dispatch"

    runner._handle_message_with_agent = _agent
    assert await runner._handle_message(_gateway_hash_restart_event()) is None
    assert agent_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ("allow",))
async def test_gateway_hash_consumer_allow_falls_through_to_agent(monkeypatch, action) -> None:
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_hash_consumer(manager, lambda event: {"action": action})
    _install_messaging_manager(monkeypatch, manager)
    runner = _gateway_runner_for_messaging()

    async def _agent(event, source, quick_key, generation):
        return event.text

    runner._handle_message_with_agent = _agent
    assert await runner._handle_message(_gateway_hash_restart_event()) == "#gateway-restart default"


@pytest.mark.asyncio
@pytest.mark.parametrize("handler", (lambda event: (_ for _ in ()).throw(RuntimeError("private consumer failure")),))
async def test_gateway_hash_consumer_error_fails_closed(monkeypatch, handler) -> None:
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_hash_consumer(manager, handler)
    _install_messaging_manager(monkeypatch, manager)
    result = await _gateway_runner_for_messaging()._handle_message(_gateway_hash_restart_event())
    assert result == "This message could not be processed safely."


@pytest.mark.asyncio
async def test_gateway_hash_consumer_conflict_fails_closed(monkeypatch) -> None:
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_hash_consumer(manager, lambda event: {"action": "claim"})
    _subscribe_gateway_hash_consumer(
        manager, lambda event: {"action": "claim"}, plugin_id="other", subscription_id="other-restart"
    )
    _install_messaging_manager(monkeypatch, manager)
    assert await _gateway_runner_for_messaging()._handle_message(_gateway_hash_restart_event()) == "This message could not be processed safely."


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ("#ordinary note", "#gateway-restart@bot default", "#Gateway-restart default"))
async def test_gateway_undeclared_or_invalid_hash_text_reaches_agent_unchanged(monkeypatch, text) -> None:
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_hash_consumer(manager, lambda event: {"action": "claim"})
    _install_messaging_manager(monkeypatch, manager)
    runner = _gateway_runner_for_messaging()

    async def _agent(event, source, quick_key, generation):
        return event.text

    runner._handle_message_with_agent = _agent
    assert await runner._handle_message(_gateway_hash_restart_event(text)) == text


@pytest.mark.asyncio
async def test_gateway_exact_consumer_claims_before_unknown_slash_fallback(monkeypatch) -> None:
    """An authorized exact /gateway-restart consumer owns its command."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    claimed: list[str] = []
    _subscribe_gateway_consumer(manager, lambda event: (claimed.append(event.text or "") or {"action": "claim"}))
    _install_messaging_manager(monkeypatch, manager)

    assert await _gateway_runner_for_messaging()._handle_message(_gateway_restart_event()) is None
    assert claimed == ["/gateway-restart"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "handler_name", "expected"),
    [
        ("approve", "_handle_approve_command", "host-approve"),
        ("deny", "_handle_deny_command", "host-deny"),
    ],
)
async def test_gateway_control_commands_are_host_precedent_over_exact_plugin_consumers(
    monkeypatch, command, handler_name, expected
) -> None:
    """An authorized exact consumer cannot observe or take over host approval flow."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    received: list[str] = []
    _subscribe_gateway_consumer(
        manager,
        lambda event: (received.append(event.text or "") or {"action": "claim"}),
        command_namespace=command,
    )
    _install_messaging_manager(monkeypatch, manager)
    runner = _gateway_runner_for_messaging()
    host_calls: list[str] = []

    async def _host_handler(event):
        host_calls.append(event.text)
        return expected

    setattr(runner, handler_name, _host_handler)
    event = _trusted_event()
    event.text = f"/{command}"

    assert await runner._handle_message(event) == expected
    assert received == []
    assert host_calls == [f"/{command}"]


@pytest.mark.asyncio
async def test_gateway_consumer_reject_suppresses_unknown_slash_fallback(monkeypatch) -> None:
    """A consumer rejection is terminal and must not become an LLM/unknown command."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_consumer(manager, lambda event: {"action": "reject"})
    _install_messaging_manager(monkeypatch, manager)

    assert await _gateway_runner_for_messaging()._handle_message(_gateway_restart_event()) is None


@pytest.mark.asyncio
async def test_gateway_consumer_allow_preserves_unknown_slash_fallback(monkeypatch) -> None:
    """Consumer allow means normal slash handling remains unchanged."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_consumer(manager, lambda event: {"action": "allow"})
    _install_messaging_manager(monkeypatch, manager)

    result = await _gateway_runner_for_messaging()._handle_message(_gateway_restart_event())
    assert result is not None
    assert "Unknown command `/gateway-restart`" in result


@pytest.mark.asyncio
async def test_gateway_no_matching_consumer_preserves_unknown_slash_fallback(monkeypatch) -> None:
    """A consumer registered for a different exact topic cannot bypass fallback."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_consumer(manager, lambda event: {"action": "claim"}, route=OTHER_TOPIC)
    _install_messaging_manager(monkeypatch, manager)

    result = await _gateway_runner_for_messaging()._handle_message(_gateway_restart_event())
    assert result is not None
    assert "Unknown command `/gateway-restart`" in result


@pytest.mark.asyncio
async def test_gateway_consumer_error_rejects_safely_before_agent_or_fallback(monkeypatch) -> None:
    """A broken consumer must fail closed without exposing implementation details."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()

    def _broken_consumer(event):
        raise RuntimeError("private consumer failure")

    _subscribe_gateway_consumer(manager, _broken_consumer)
    _install_messaging_manager(monkeypatch, manager)

    result = await _gateway_runner_for_messaging()._handle_message(_gateway_restart_event())
    assert result is not None
    assert "could not be processed" in result.lower()
    assert "Unknown command" not in result
    assert "private consumer failure" not in result


@pytest.mark.asyncio
async def test_gateway_consumer_conflict_rejects_safely_before_unknown_fallback(monkeypatch) -> None:
    """Equal-priority eligible consumers fail closed rather than choosing one."""
    from hermes_cli.plugins import PluginManager

    manager = PluginManager()
    _subscribe_gateway_consumer(manager, lambda event: {"action": "claim"})
    _subscribe_gateway_consumer(
        manager, lambda event: {"action": "claim"}, plugin_id="other", subscription_id="other-restart"
    )
    _install_messaging_manager(monkeypatch, manager)

    result = await _gateway_runner_for_messaging()._handle_message(_gateway_restart_event())
    assert result == "This message could not be processed safely."


@pytest.mark.asyncio
async def test_gateway_restart_bridge_fixture_claims_the_exact_authorized_hash_command_from_an_isolated_root(
    tmp_path, monkeypatch
) -> None:
    """A repository fixture proves directory discovery and routing without live bridge dependencies."""
    from hermes_cli.plugins import PluginManager

    fixture_plugin = Path(__file__).parents[1] / "fixtures" / "gateway_restart_bridge"
    assert fixture_plugin.is_dir()
    hermes_home = tmp_path / "hermes-home"
    copied_plugin = hermes_home / "plugins" / "gateway-restart-bridge"
    shutil.copytree(fixture_plugin, copied_plugin, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    restart_db = tmp_path / "gateway-restart.sqlite3"
    (copied_plugin / "config.yaml").write_text(
        "\n".join((
            "active: true",
            f"database_path: {restart_db}",
            'telegram_chat_id: "-1004411640215"',
            'owner_id: "telegram-user:9189955"',
            "approval_ttl_seconds: 28800",
            "",
        )),
        encoding="utf-8",
    )
    (hermes_home / "config.yaml").write_text(
        """plugins:
  enabled: [gateway-restart-bridge]
plugin_messaging:
  gateway-restart-bridge:
    inbound:
      - platform: telegram
        chat_id: "-1004411640215"
        events: [message, callback]
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    monkeypatch.setattr(
        subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no subprocesses in this reproduction")),
    )

    manager = PluginManager()
    manager.discover_and_load()
    outcome = await manager.route_messaging_event(
        MessageEvent(
            text="#gateway-restart default",
            message_id="production-shaped-command",
            platform_update_id=9189955,
            source=SessionSource(
                platform=Platform.TELEGRAM,
                chat_id="-1004411640215",
                thread_id=None,
                user_id="9189955",
                chat_type="group",
            ),
        )
    )

    assert outcome is not None
    assert (outcome.action, outcome.consumer_plugin_id, outcome.audit_reason) == (
        "claim", "gateway-restart-bridge", None,
    )
    assert not restart_db.exists()
    assert not (hermes_home / "gateway.pid").exists()


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

    assert ConsumerDeclaration(hash_command_namespace="gateway-restart").hash_command_namespace == "gateway-restart"
    for invalid in ("Gateway", "gateway.restart", "gateway@bot", "gateway restart"):
        with pytest.raises(SubscriptionError, match="hash command namespace"):
            ConsumerDeclaration(hash_command_namespace=invalid)
    with pytest.raises(SubscriptionError, match="exactly one namespace"):
        ConsumerDeclaration(command_namespace="slash", hash_command_namespace="hash")



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
