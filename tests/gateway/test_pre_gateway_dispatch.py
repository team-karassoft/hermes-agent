"""Tests for the pre_gateway_dispatch plugin hook.

The hook allows plugins to intercept incoming messages before auth and
agent dispatch. It runs in _handle_message and acts on returned action
dicts: {"action": "skip"|"rewrite"|"allow"}.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource


def _clear_auth_env(monkeypatch) -> None:
    for key in (
        "TELEGRAM_ALLOWED_USERS",
        "WHATSAPP_ALLOWED_USERS",
        "GATEWAY_ALLOWED_USERS",
        "TELEGRAM_ALLOW_ALL_USERS",
        "WHATSAPP_ALLOW_ALL_USERS",
        "GATEWAY_ALLOW_ALL_USERS",
    ):
        monkeypatch.delenv(key, raising=False)


def _make_event(text: str = "hello", platform: Platform = Platform.WHATSAPP) -> MessageEvent:
    return MessageEvent(
        text=text,
        message_id="m1",
        source=SessionSource(
            platform=platform,
            user_id="15551234567@s.whatsapp.net",
            chat_id="15551234567@s.whatsapp.net",
            user_name="tester",
            chat_type="dm",
        ),
    )


def _make_runner(platform: Platform):
    from gateway.run import GatewayRunner

    config = GatewayConfig(
        platforms={platform: PlatformConfig(enabled=True)},
    )
    runner = object.__new__(GatewayRunner)
    runner.config = config
    adapter = SimpleNamespace(send=AsyncMock())
    runner.adapters = {platform: adapter}
    runner.pairing_store = MagicMock()
    runner.pairing_store.is_approved.return_value = False
    runner.pairing_store._is_rate_limited.return_value = False
    runner.session_store = MagicMock()
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    return runner, adapter


@pytest.mark.asyncio
async def test_hook_skip_short_circuits_dispatch(monkeypatch):
    """A plugin returning {'action': 'skip'} drops the message before auth."""
    _clear_auth_env(monkeypatch)

    async def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            return [{"action": "skip", "reason": "plugin-handled"}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook_async", _fake_hook)

    runner, adapter = _make_runner(Platform.WHATSAPP)

    result = await runner._handle_message(_make_event("hi"))

    assert result is None
    adapter.send.assert_not_awaited()
    runner.pairing_store.generate_code.assert_not_called()


@pytest.mark.asyncio
async def test_hook_rewrite_replaces_event_text(monkeypatch):
    """A plugin returning {'action': 'rewrite', 'text': ...} mutates event.text."""
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "*")

    seen_text = {}

    async def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            return [{"action": "rewrite", "text": "REWRITTEN"}]
        return []

    async def _capture(event, source, _quick_key, _run_generation):
        seen_text["value"] = event.text
        return "ok"

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook_async", _fake_hook)

    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner._handle_message_with_agent = _capture  # noqa: SLF001

    await runner._handle_message(_make_event("original"))

    assert seen_text.get("value") == "REWRITTEN"


@pytest.mark.asyncio
async def test_hook_allow_falls_through_to_auth(monkeypatch):
    """A plugin returning {'action': 'allow'} continues to normal dispatch."""
    _clear_auth_env(monkeypatch)
    # No allowed users set → auth fails → pairing flow triggers.
    monkeypatch.delenv("WHATSAPP_ALLOWED_USERS", raising=False)

    async def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            return [{"action": "allow"}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook_async", _fake_hook)

    runner, adapter = _make_runner(Platform.WHATSAPP)
    runner.pairing_store.generate_code.return_value = "12345"

    result = await runner._handle_message(_make_event("hi"))

    # auth chain ran → pairing code was generated
    assert result is None
    runner.pairing_store.generate_code.assert_called_once()


@pytest.mark.asyncio
async def test_hook_exception_does_not_break_dispatch(monkeypatch):
    """A raising plugin hook does not break the gateway."""
    _clear_auth_env(monkeypatch)
    monkeypatch.delenv("WHATSAPP_ALLOWED_USERS", raising=False)

    async def _fake_hook(name, **kwargs):
        raise RuntimeError("plugin blew up")

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook_async", _fake_hook)

    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner.pairing_store.generate_code.return_value = None

    # Should not raise; falls through to auth chain.
    result = await runner._handle_message(_make_event("hi"))
    assert result is None


@pytest.mark.asyncio
async def test_internal_events_bypass_hook(monkeypatch):
    """Internal events (event.internal=True) skip the plugin hook entirely."""
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "*")

    called = {"count": 0}

    async def _fake_hook(name, **kwargs):
        called["count"] += 1
        return [{"action": "skip"}]

    async def _capture(event, source, _quick_key, _run_generation):
        return "ok"

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook_async", _fake_hook)

    runner, _adapter = _make_runner(Platform.WHATSAPP)
    runner._handle_message_with_agent = _capture  # noqa: SLF001

    event = _make_event("hi")
    event.internal = True

    # Even though the hook would say skip, internal events bypass it.
    await runner._handle_message(event)
    assert called["count"] == 0

@pytest.mark.asyncio
async def test_hook_fires_without_session_store_attribute(monkeypatch):
    """A runner missing session_store still delivers the event to plugins.

    Regression: the hook kwargs read ``self.session_store`` directly, so a
    partially-initialized runner raised AttributeError inside the dispatch
    try-block — the hook never fired, and every message logged
    "pre_gateway_dispatch invocation failed: 'GatewayRunner' object has no
    attribute 'session_store'". Plugins must receive the event (with
    session_store=None) instead.
    """
    _clear_auth_env(monkeypatch)

    seen = {}

    async def _fake_hook(name, **kwargs):
        if name == "pre_gateway_dispatch":
            seen["session_store"] = kwargs.get("session_store", "MISSING")
            return [{"action": "skip", "reason": "plugin-handled"}]
        return []

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook_async", _fake_hook)

    runner, adapter = _make_runner(Platform.WHATSAPP)
    del runner.session_store

    result = await runner._handle_message(_make_event("hi"))
    assert result is None
    # Hook actually fired (skip short-circuited before auth) with a None store.
    assert seen == {"session_store": None}
    adapter.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("claimed_action", ["skip", "claim"])
async def test_loaded_async_legacy_hook_claims_real_telegram_event(
    monkeypatch, tmp_path, claimed_action
):
    """A loaded legacy hook may asynchronously claim a Telegram command.

    This exercises the real PluginManager singleton instead of mocking
    ``invoke_hook``.  The messaging bus is intentionally left unsubscribed:
    legacy hooks and new observer/consumer routing are independent contracts.
    """
    from hermes_cli import plugins

    hermes_home = tmp_path / "hermes"
    plugin_dir = hermes_home / "plugins" / "gateway-compat"
    plugin_dir.mkdir(parents=True)
    (hermes_home / "config.yaml").write_text(
        "plugins:\n  enabled:\n    - gateway-compat\n",
        encoding="utf-8",
    )
    (plugin_dir / "plugin.yaml").write_text(
        "name: gateway-compat\nversion: 1.0.0\n",
        encoding="utf-8",
    )
    (plugin_dir / "__init__.py").write_text(
        "def register(ctx):\n"
        "    async def handle(event, gateway, session_store, **host_metadata):\n"
        "        gateway.legacy_hook_events.append((event, host_metadata))\n"
        f"        return {claimed_action!r}\n"
        "    ctx.register_hook('pre_gateway_dispatch', handle)\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))
    manager = plugins.PluginManager()
    monkeypatch.setattr(plugins, "_plugin_manager", manager)
    manager.discover_and_load()

    runner, adapter = _make_runner(Platform.TELEGRAM)
    runner.legacy_hook_events = []
    runner._run_agent = AsyncMock(
        side_effect=AssertionError("claimed command reached normal dispatch")
    )
    event = MessageEvent(
        text="/idea",
        message_id="42",
        platform_update_id=314,
        reply_to_message_id="41",
        reply_to_text="A realistic replied-to idea",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            user_id="1001",
            chat_id="-1002003",
            user_name="operator",
            chat_type="forum",
            thread_id="77",
        ),
    )

    result = await runner._handle_message(event)

    assert result is None
    assert len(runner.legacy_hook_events) == 1
    handled_event, host_metadata = runner.legacy_hook_events[0]
    assert handled_event is event
    assert host_metadata["telemetry_schema_version"]
    assert not manager.messaging_router.has_subscriptions
    adapter.send.assert_not_awaited()
    runner._run_agent.assert_not_awaited()
