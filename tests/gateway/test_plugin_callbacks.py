"""Isolated RFC 0001 Phase 4 inline-action and callback ownership tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from gateway.platforms.base import SendResult
from gateway.plugin_callbacks import (
    CallbackRejected,
    HostCallbackRegistry,
    TrustedCallback,
    load_or_create_callback_signing_key,
)
from gateway.plugin_messaging import (
    Button,
    ConsumerDeclaration,
    HostMessagingPermissions,
    InlineKeyboard,
    PluginMessageRouter,
    TopicRoute,
)
from gateway.plugin_outbox import PluginOutboundIntent, PluginOutboxService


ROUTE = TopicRoute("telegram", "-100123", "42")
OTHER_TOPIC = TopicRoute("telegram", "-100123", "43")
NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)


def _permissions(*plugin_ids: str) -> HostMessagingPermissions:
    return HostMessagingPermissions.from_raw(
        {
            "plugin_messaging": {
                plugin_id: {
                    "inbound": [
                        {
                            "platform": ROUTE.platform,
                            "chat_id": ROUTE.chat_id,
                            "thread_id": ROUTE.thread_id,
                            "events": ["callback"],
                        }
                    ],
                    "outbound": [
                        {
                            "platform": ROUTE.platform,
                            "chat_id": ROUTE.chat_id,
                            "thread_id": ROUTE.thread_id,
                            "types": ["text", "inline_keyboard"],
                        }
                    ],
                }
                for plugin_id in plugin_ids
            }
        }
    )


@pytest.fixture
def registry(tmp_path) -> HostCallbackRegistry:
    return HostCallbackRegistry(
        signing_key=b"isolated-test-key",
        database_path=tmp_path / "callbacks.db",
        now=lambda: NOW,
    )


def _intent() -> PluginOutboundIntent:
    return PluginOutboundIntent(
        idempotency_key="proposal:7",
        route=ROUTE,
        text="Review proposal",
        keyboard=InlineKeyboard(
            rows=(
                (
                    Button(
                        label="Approve",
                        action="approve_proposal",
                        payload={"proposal_id": "7"},
                    ),
                ),
            )
        ),
    )


def test_keyboard_contract_is_typed_and_rejects_raw_callback_data() -> None:
    button = _intent().keyboard.rows[0][0]
    assert button.label == "Approve"
    assert button.action == "approve_proposal"
    assert button.payload == {"proposal_id": "7"}
    assert not hasattr(button, "callback_data")
    with pytest.raises((TypeError, ValueError)):
        Button(label="Bad", action="not allowed", payload={})
    with pytest.raises(ValueError, match="JSON-compatible"):
        Button(label="Bad", action="bad_payload", payload={"value": object()})


def test_inline_keyboard_requires_its_own_host_grant(registry) -> None:
    text_only = HostMessagingPermissions.from_raw(
        {
            "plugin_messaging": {
                "owner": {
                    "outbound": [
                        {
                            "platform": ROUTE.platform,
                            "chat_id": ROUTE.chat_id,
                            "thread_id": ROUTE.thread_id,
                            "types": ["text"],
                        }
                    ]
                }
            }
        }
    )
    with pytest.raises(PermissionError, match="inline_keyboard"):
        PluginOutboxService(
            text_only, callback_registry=registry
        ).accept(plugin_id="owner", intent=_intent())


class _Adapter:
    def __init__(self) -> None:
        self.calls = []

    async def send(self, **kwargs):
        self.calls.append(kwargs)
        return SendResult(success=True, message_id="sent-99")


@pytest.mark.asyncio
async def test_host_renders_opaque_tokens_and_binds_sent_message(
    monkeypatch, tmp_path, registry
) -> None:
    from gateway import delivery_ledger as dl

    monkeypatch.setattr(dl, "_db_path", lambda: tmp_path / "ledger.db")
    adapter = _Adapter()
    intent = _intent()
    service = PluginOutboxService(_permissions("owner"), callback_registry=registry)
    obligation_id, accepted = service.accept(plugin_id="owner", intent=intent)

    assert accepted is True
    assert await service.deliver_persisted(
        adapter=adapter,
        obligation_id=obligation_id,
        intent=intent,
        plugin_id="owner",
        callback_registry=registry,
    )
    wire_button = adapter.calls[0]["metadata"]["inline_keyboard"][0][0]
    assert wire_button["text"] == "Approve"
    assert wire_button["callback_token"].startswith("pc1.")
    assert "approve_proposal" not in wire_button["callback_token"]
    claim = registry.validate_and_consume(
        TrustedCallback(
            token=wire_button["callback_token"],
            route=ROUTE,
            message_id="sent-99",
            sender_id="actor-1",
            event_id="telegram:update-8",
            received_at=NOW,
        )
    )
    assert claim.plugin_id == "owner"
    assert claim.action == "approve_proposal"
    assert claim.payload == {"proposal_id": "7"}


@pytest.mark.asyncio
async def test_valid_callback_routes_only_to_owner_plugin(registry) -> None:
    owner_received = []
    other_received = []
    router = PluginMessageRouter(
        _permissions("owner", "other"), callback_registry=registry
    )
    for plugin_id, received in (("owner", owner_received), ("other", other_received)):
        router.subscribe(
            plugin_id=plugin_id,
            subscription_id="callbacks",
            routes=[ROUTE],
            event_types={"callback"},
            mode="consumer",
            handler=lambda event, target=received: (
                target.append(event) or {"action": "claim"}
            ),
            consumer=ConsumerDeclaration(callback_ownership="actions"),
        )
    token = registry.issue(
        plugin_id="owner",
        route=ROUTE,
        action="approve_proposal",
        payload={"proposal_id": "7"},
        expires_at=NOW + timedelta(minutes=5),
    )
    registry.bind_message(token=token, message_id="sent-99")

    outcome = await router.route_callback(
        TrustedCallback(
            token=token,
            route=ROUTE,
            message_id="sent-99",
            sender_id="actor-1",
            event_id="telegram:update-8",
            received_at=NOW,
        )
    )

    assert outcome.action == "claim"
    assert outcome.consumer_plugin_id == "owner"
    assert len(owner_received) == 1
    assert owner_received[0].action == "approve_proposal"
    assert owner_received[0].payload == {"proposal_id": "7"}
    assert other_received == []


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda token: {"token": token[:-1] + ("A" if token[-1] != "A" else "B")}, "tampered"),
        (lambda token: {"route": OTHER_TOPIC}, "route"),
        (lambda token: {"message_id": "other-message"}, "message"),
    ],
)
def test_tampered_cross_topic_and_cross_message_callbacks_are_denied(
    registry, mutation, reason
) -> None:
    token = registry.issue(
        plugin_id="owner",
        route=ROUTE,
        action="approve",
        payload={},
        expires_at=NOW + timedelta(minutes=5),
    )
    registry.bind_message(token=token, message_id="sent-99")
    values = {
        "token": token,
        "route": ROUTE,
        "message_id": "sent-99",
        "sender_id": None,
        "event_id": "event-1",
        "received_at": NOW,
    }
    values.update(mutation(token))
    with pytest.raises(CallbackRejected, match=reason):
        registry.validate_and_consume(TrustedCallback(**values))


def test_expired_and_replayed_tokens_are_denied(registry) -> None:
    expired = registry.issue(
        plugin_id="owner",
        route=ROUTE,
        action="approve",
        payload={},
        expires_at=NOW - timedelta(seconds=1),
    )
    registry.bind_message(token=expired, message_id="sent-99")
    with pytest.raises(CallbackRejected, match="expired"):
        registry.validate_and_consume(
            TrustedCallback(expired, ROUTE, "sent-99", None, "event-1", NOW)
        )

    token = registry.issue(
        plugin_id="owner",
        route=ROUTE,
        action="approve",
        payload={},
        expires_at=NOW + timedelta(minutes=5),
    )
    registry.bind_message(token=token, message_id="sent-99")
    callback = TrustedCallback(token, ROUTE, "sent-99", None, "event-2", NOW)
    registry.validate_and_consume(callback)
    with pytest.raises(CallbackRejected, match="replayed"):
        registry.validate_and_consume(callback)


def test_callback_token_survives_registry_restart_with_profile_local_key(
    tmp_path,
) -> None:
    key_path = tmp_path / "host-private" / "plugin-callback-signing.key"
    database_path = tmp_path / "callbacks.db"
    first_registry = HostCallbackRegistry(
        signing_key=load_or_create_callback_signing_key(key_path),
        database_path=database_path,
        now=lambda: NOW,
    )
    token = first_registry.issue(
        plugin_id="owner",
        route=ROUTE,
        action="approve",
        payload={"proposal_id": "7"},
        expires_at=NOW + timedelta(minutes=5),
    )
    first_registry.bind_message(token=token, message_id="sent-99")

    restarted_registry = HostCallbackRegistry(
        signing_key=load_or_create_callback_signing_key(key_path),
        database_path=database_path,
        now=lambda: NOW,
    )
    claim = restarted_registry.validate_and_consume(
        TrustedCallback(token, ROUTE, "sent-99", None, "event-after-restart", NOW)
    )

    assert claim.plugin_id == "owner"
    assert claim.action == "approve"
    assert claim.payload == {"proposal_id": "7"}
    assert key_path.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_cross_plugin_callback_ownership_is_denied(registry) -> None:
    router = PluginMessageRouter(
        _permissions("other"), callback_registry=registry
    )
    called = []
    router.subscribe(
        plugin_id="other",
        subscription_id="callbacks",
        routes=[ROUTE],
        event_types={"callback"},
        mode="consumer",
        handler=called.append,
        consumer=ConsumerDeclaration(callback_ownership="actions"),
    )
    token = registry.issue(
        plugin_id="owner",
        route=ROUTE,
        action="approve",
        payload={},
        expires_at=NOW + timedelta(minutes=5),
    )
    registry.bind_message(token=token, message_id="sent-99")
    outcome = await router.route_callback(
        TrustedCallback(token, ROUTE, "sent-99", None, "event-1", NOW)
    )
    assert outcome.action == "reject"
    assert outcome.audit_reason == "callback-owner-unavailable"
    assert called == []


@pytest.mark.asyncio
async def test_router_fail_closed_does_not_invoke_owner_for_invalid_token(
    registry,
) -> None:
    called = []
    router = PluginMessageRouter(
        _permissions("owner"), callback_registry=registry
    )
    router.subscribe(
        plugin_id="owner",
        subscription_id="callbacks",
        routes=[ROUTE],
        event_types={"callback"},
        mode="consumer",
        handler=called.append,
        consumer=ConsumerDeclaration(callback_ownership="actions"),
    )
    outcome = await router.route_callback(
        TrustedCallback("pc1.invalid.invalid", ROUTE, "sent-99", None, "e-1", NOW)
    )
    assert outcome.action == "reject"
    assert outcome.audit_reason == "callback-validation-rejected"
    assert called == []


def test_plugin_facade_exposes_neither_registry_token_nor_adapter() -> None:
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

    messaging = PluginContext(
        PluginManifest(name="Owner", key="owner"), PluginManager()
    ).messaging
    assert not hasattr(messaging, "callback_registry")
    assert not hasattr(messaging, "adapter")
    assert not hasattr(messaging, "issue_callback_token")
