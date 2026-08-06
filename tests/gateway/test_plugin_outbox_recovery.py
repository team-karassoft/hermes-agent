"""End-to-end durable recovery for host-owned plugin keyboard intents."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway import delivery_ledger as dl
from gateway.platforms.base import Platform, SendResult
from gateway.plugin_callbacks import HostCallbackRegistry, TrustedCallback
from gateway.plugin_messaging import (
    Button,
    ConsumerDeclaration,
    HostMessagingPermissions,
    InlineKeyboard,
    TopicRoute,
)
from gateway.plugin_outbox import PluginOutboundIntent, PluginOutboxService


ROUTE = TopicRoute("telegram", "-100123", "42")
NOW = datetime(2026, 8, 6, 12, tzinfo=UTC)


def _config(*, keyboard: bool = True):
    types = ["text", "inline_keyboard"] if keyboard else ["text"]
    return {
        "plugin_messaging": {
            "owner": {
                "inbound": [{
                    "platform": ROUTE.platform,
                    "chat_id": ROUTE.chat_id,
                    "thread_id": ROUTE.thread_id,
                    "events": ["callback"],
                }],
                "outbound": [{
                    "platform": ROUTE.platform,
                    "chat_id": ROUTE.chat_id,
                    "thread_id": ROUTE.thread_id,
                    "types": types,
                }]
            }
        }
    }


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / "real-hermes-home"
    monkeypatch.setenv("HERMES_HOME", str(home))
    assert dl._db_path() == home / "state.db"
    return home


class _Adapter:
    def __init__(self, result: SendResult | None = None, error: Exception | None = None):
        self.calls = []
        self.result = result or SendResult(success=True, message_id="recovered-99")
        self.error = error

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


def _orphan(obligation_id: str) -> None:
    with sqlite3.connect(dl._db_path()) as conn:
        conn.execute(
            "UPDATE delivery_obligations SET owner_pid=999999999, owner_started_at=1 "
            "WHERE obligation_id=?", (obligation_id,)
        )


def _state(obligation_id: str) -> str:
    with sqlite3.connect(dl._db_path()) as conn:
        return conn.execute(
            "SELECT state FROM delivery_obligations WHERE obligation_id=?", (obligation_id,)
        ).fetchone()[0]


def _obligation_count() -> int:
    with dl._connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM delivery_obligations").fetchone()[0]


def _keyboard_intent() -> PluginOutboundIntent:
    return PluginOutboundIntent(
        idempotency_key="proposal:recovery",
        route=ROUTE,
        text="Review the durable proposal",
        keyboard=InlineKeyboard(rows=((Button("Approve", "approve_proposal", {"proposal_id": "7"}),),)),
    )


def _runner(adapter, registry):
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._plugin_callback_registry = registry
    runner.session_store = None
    runner._async_session_store = SimpleNamespace(clear_resume_pending=lambda *_: None)
    return runner


@pytest.mark.parametrize(
    "payload",
    [
        {"context": [{"detail": "pc1.raw.callback.token"}]},
        {"context": [{"detail": "bEaReR opaque-access-token"}]},
        {"context": [{"detail": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvd25lciJ9.c2lnbmF0dXJl"}]},
        {"context": [{"detail": "sk-proj-0123456789abcdefghijklmnop"}]},
    ],
    ids=["host-callback", "bearer", "jwt", "vendor-credential"],
)
def test_rejects_secret_values_under_neutral_nested_keys_without_persisting_ledger_row(
    hermes_home, payload
):
    registry = HostCallbackRegistry(signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db")
    service = PluginOutboxService(HostMessagingPermissions.from_raw(_config()), callback_registry=registry)
    intent = PluginOutboundIntent(
        idempotency_key="proposal:secret-payload",
        route=ROUTE,
        text="Review the durable proposal",
        keyboard=InlineKeyboard(rows=((Button("Approve", "approve_proposal", payload),),)),
    )

    with pytest.raises(ValueError, match="callback tokens or credentials"):
        service.accept(plugin_id="owner", intent=intent)

    assert _obligation_count() == 0


def test_accepts_normal_semantic_keyboard_payload(hermes_home):
    registry = HostCallbackRegistry(signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db")
    service = PluginOutboxService(HostMessagingPermissions.from_raw(_config()), callback_registry=registry)
    intent = PluginOutboundIntent(
        idempotency_key="proposal:normal-payload",
        route=ROUTE,
        text="Review the durable proposal",
        keyboard=InlineKeyboard(rows=((Button(
            "Approve", "approve_proposal",
            {"proposal_id": "7", "filters": {"state": "open", "tags": ["review", "owner"]}},
        ),),)),
    )

    obligation_id, accepted = service.accept(plugin_id="owner", intent=intent)

    assert accepted
    assert _obligation_count() == 1
    assert _state(obligation_id) == "pending"


@pytest.mark.asyncio
async def test_crash_before_keyboard_delivery_recovers_semantics_with_fresh_bound_token(
    hermes_home, monkeypatch
):
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: _config())
    registry = HostCallbackRegistry(
        signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db", now=lambda: NOW
    )
    intent = _keyboard_intent()
    obligation_id, accepted = PluginOutboxService(
        HostMessagingPermissions.from_raw(_config()), callback_registry=registry
    ).accept(plugin_id="owner", intent=intent)
    assert accepted
    _orphan(obligation_id)  # crash after accept, before adapter.send

    adapter = _Adapter()
    assert await _runner(adapter, registry)._redeliver_pending_obligations() == 1

    sent = adapter.calls[0]
    assert sent["content"] == intent.text
    wire = sent["metadata"]["inline_keyboard"][0][0]
    assert wire["text"] == "Approve"
    assert wire["callback_token"].startswith("pc1.")
    assert "approve_proposal" not in wire["callback_token"]
    claim = registry.validate_and_consume(TrustedCallback(
        wire["callback_token"], ROUTE, "recovered-99", "actor", "event-1", NOW
    ))
    assert (claim.plugin_id, claim.action, claim.payload) == ("owner", "approve_proposal", {"proposal_id": "7"})
    assert _state(obligation_id) == "delivered"


@pytest.mark.asyncio
async def test_reconnect_replacement_telegram_adapter_routes_recovered_keyboard_callback_through_manager(
    hermes_home, monkeypatch
):
    """A recovered keyboard remains host-routed after its adapter is replaced."""
    from gateway.config import GatewayConfig, PlatformConfig
    from gateway.run import GatewayRunner
    from hermes_cli.plugins import PluginManager
    from plugins.platforms.telegram.adapter import TelegramAdapter

    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: _config())
    manager = PluginManager()
    received = []
    manager._messaging_router.subscribe(
        plugin_id="owner",
        subscription_id="recovered-callback",
        routes=[ROUTE],
        event_types={"callback"},
        mode="consumer",
        handler=lambda event: received.append(event) or {"action": "claim"},
        consumer=ConsumerDeclaration(callback_ownership="actions"),
    )

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={Platform.TELEGRAM: PlatformConfig(enabled=True, token="test")}
    )
    runner.adapters = {}
    runner._background_tasks = set()
    runner._running = True
    runner._failed_platforms = {
        Platform.TELEGRAM: {
            "config": PlatformConfig(enabled=True, token="test"),
            "attempts": 0,
            "next_retry": time.monotonic() - 1,
        }
    }
    runner.delivery_router = SimpleNamespace(adapters={})
    runner.session_store = MagicMock()
    runner._handle_message = MagicMock()
    runner._handle_adapter_fatal_error = MagicMock()
    runner._handle_active_session_busy_message = MagicMock()
    runner._recover_telegram_topic_thread_id = MagicMock()
    runner._make_adapter_auth_check = MagicMock(return_value=lambda *_args, **_kwargs: True)
    runner._busy_text_mode = None
    runner._sync_voice_mode_state_to_adapter = MagicMock()
    runner._update_platform_runtime_status = MagicMock()
    runner._schedule_resume_pending_sessions = MagicMock()
    runner._async_session_store = SimpleNamespace(clear_resume_pending=lambda *_args: None)
    runner._bind_plugin_messaging_dispatcher(manager)

    obligation_id, accepted = PluginOutboxService(
        HostMessagingPermissions.from_raw(_config()),
        callback_registry=runner._plugin_callback_registry,
    ).accept(plugin_id="owner", intent=_keyboard_intent())
    assert accepted
    _orphan(obligation_id)

    replacement = TelegramAdapter(PlatformConfig(enabled=True, token="test", extra={}))
    replacement._is_callback_user_authorized = lambda *_args, **_kwargs: True
    replacement.send = AsyncMock(return_value=SendResult(success=True, message_id="recovered-99"))
    runner._create_adapter = MagicMock(return_value=replacement)
    runner._connect_adapter_with_timeout = AsyncMock(return_value=True)

    real_sleep = asyncio.sleep
    sleeps = 0

    async def stop_after_reconnect(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps > 2:
            runner._running = False
        await real_sleep(0)

    with patch("asyncio.sleep", side_effect=stop_after_reconnect), patch(
        "gateway.channel_directory.build_channel_directory", new=AsyncMock(return_value={})
    ):
        await runner._platform_reconnect_watcher()

    wire = replacement.send.await_args.kwargs["metadata"]["inline_keyboard"][0][0]
    query = SimpleNamespace(
        id="callback-1",
        data=wire["callback_token"],
        message=SimpleNamespace(
            chat_id=-100123,
            message_id="recovered-99",
            message_thread_id=42,
            chat=SimpleNamespace(type="supergroup"),
        ),
        from_user=SimpleNamespace(id="actor-1", first_name="Owner"),
        answer=AsyncMock(),
    )
    await replacement._handle_callback_query(SimpleNamespace(callback_query=query), None)

    assert replacement._plugin_callback_router is runner._plugin_callback_router
    assert replacement._plugin_callback_router.__self__ is manager
    assert (
        replacement._plugin_callback_router.__func__
        is manager.route_plugin_callback.__func__
    )
    assert [(event.action, event.payload) for event in received] == [
        ("approve_proposal", {"proposal_id": "7"})
    ]


@pytest.mark.asyncio
async def test_failed_keyboard_recovery_mints_new_token_and_keeps_ambiguity_marker(
    hermes_home, monkeypatch
):
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: _config())
    registry = HostCallbackRegistry(
        signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db", now=lambda: NOW
    )
    intent = _keyboard_intent()
    service = PluginOutboxService(HostMessagingPermissions.from_raw(_config()), callback_registry=registry)
    obligation_id, _ = service.accept(plugin_id="owner", intent=intent)
    first = _Adapter(result=SendResult(success=False, error="down"))
    assert not await service.deliver_persisted(adapter=first, obligation_id=obligation_id, intent=intent, plugin_id="owner", callback_registry=registry)
    first_token = first.calls[0]["metadata"]["inline_keyboard"][0][0]["callback_token"]
    _orphan(obligation_id)

    recovered = _Adapter()
    assert await _runner(recovered, registry)._redeliver_pending_obligations() == 1
    second_token = recovered.calls[0]["metadata"]["inline_keyboard"][0][0]["callback_token"]
    assert recovered.calls[0]["content"] == dl.RECOVERED_MARKER + intent.text
    assert second_token != first_token
    assert _state(obligation_id) == "delivered"


@pytest.mark.asyncio
async def test_malformed_or_legacy_keyboard_intent_fails_closed_without_text_fallback(
    hermes_home, monkeypatch
):
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: _config())
    registry = HostCallbackRegistry(signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db")
    intent = _keyboard_intent()
    obligation_id, _ = PluginOutboxService(
        HostMessagingPermissions.from_raw(_config()), callback_registry=registry
    ).accept(plugin_id="owner", intent=intent)
    with sqlite3.connect(dl._db_path()) as conn:
        conn.execute("UPDATE delivery_obligations SET plugin_intent=? WHERE obligation_id=?", (json.dumps({"keyboard": [[{"label": "only-label"}]]}), obligation_id))
    _orphan(obligation_id)

    adapter = _Adapter()
    assert await _runner(adapter, registry)._redeliver_pending_obligations() == 0
    assert adapter.calls == []
    assert _state(obligation_id) == "failed"


@pytest.mark.asyncio
async def test_recovery_rechecks_keyboard_grant_and_fails_closed(hermes_home, monkeypatch):
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: _config(keyboard=False))
    registry = HostCallbackRegistry(signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db")
    obligation_id, _ = PluginOutboxService(
        HostMessagingPermissions.from_raw(_config()), callback_registry=registry
    ).accept(plugin_id="owner", intent=_keyboard_intent())
    _orphan(obligation_id)

    adapter = _Adapter()
    assert await _runner(adapter, registry)._redeliver_pending_obligations() == 0
    assert adapter.calls == []
    assert _state(obligation_id) == "failed"


@pytest.mark.asyncio
async def test_recovery_requires_callback_registry_for_keyboard(hermes_home, monkeypatch):
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: _config())
    registry = HostCallbackRegistry(signing_key=b"test-signing-key", database_path=hermes_home / "callbacks.db")
    obligation_id, _ = PluginOutboxService(
        HostMessagingPermissions.from_raw(_config()), callback_registry=registry
    ).accept(plugin_id="owner", intent=_keyboard_intent())
    _orphan(obligation_id)

    adapter = _Adapter()
    assert await _runner(adapter, None)._redeliver_pending_obligations() == 0
    assert adapter.calls == []
    assert _state(obligation_id) == "failed"


def test_additive_ledger_migration_preserves_old_text_row(hermes_home):
    hermes_home.mkdir(parents=True)
    with sqlite3.connect(dl._db_path()) as conn:
        conn.execute("""CREATE TABLE delivery_obligations (
            obligation_id TEXT PRIMARY KEY, session_key TEXT NOT NULL, platform TEXT NOT NULL,
            chat_id TEXT NOT NULL, thread_id TEXT, content TEXT NOT NULL, state TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL,
            owner_pid INTEGER, owner_started_at INTEGER, last_error TEXT)""")
        conn.execute("INSERT INTO delivery_obligations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (
            "old-row", "agent:old", "telegram", ROUTE.chat_id, ROUTE.thread_id, "old text",
            "pending", 0, 1.0, 1.0, 999999999, 1, None,
        ))
    with dl._connect() as conn:
        row = conn.execute("SELECT content, plugin_intent FROM delivery_obligations WHERE obligation_id='old-row'").fetchone()
    assert row == ("old text", None)


@pytest.mark.asyncio
async def test_legacy_text_only_obligation_remains_recoverable(hermes_home):
    dl.record_obligation(
        obligation_id="legacy-text", session_key="agent:legacy", platform="telegram",
        chat_id=ROUTE.chat_id, thread_id=ROUTE.thread_id, content="old plain text",
    )
    _orphan("legacy-text")
    adapter = _Adapter()

    assert await _runner(adapter, None)._redeliver_pending_obligations() == 1
    assert adapter.calls == [{"chat_id": ROUTE.chat_id, "content": "old plain text", "metadata": {"thread_id": ROUTE.thread_id}}]
    assert _state("legacy-text") == "delivered"


@pytest.mark.asyncio
async def test_legacy_plugin_row_without_semantics_never_falls_back_to_text(hermes_home):
    dl.record_obligation(
        obligation_id="legacy-plugin", session_key="plugin:owner:telegram:-100123:42",
        platform="telegram", chat_id=ROUTE.chat_id, thread_id=ROUTE.thread_id,
        content="possibly old keyboard text",
    )
    _orphan("legacy-plugin")
    adapter = _Adapter()

    assert await _runner(adapter, None)._redeliver_pending_obligations() == 0
    assert adapter.calls == []
    assert _state("legacy-plugin") == "failed"
