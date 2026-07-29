# RFC 0001: Plugin Messaging Bus

- **Status:** Proposed
- **Authors:** KarasSoft
- **Target:** `team-karassoft/hermes-agent`
- **Created:** 2026-07-29
- **Scope:** Telegram Forum Topics in v1; platform-neutral core contracts

## 1. Summary

Introduce a host-owned Plugin Messaging Bus so trusted plugins can subscribe to normalized events for explicitly approved platform routes and submit typed outbound intents through Hermes-owned transport. The bus supports multiple listeners for one route, one deterministic consumer for an explicit command or callback, durable outbound delivery, plugin-owned inline actions, and bounded host-managed jobs.

Plugins never receive platform credentials, raw transport adapters, or a generic sender.

## 2. Motivation

The current `pre_gateway_dispatch` hook is a generic lifecycle hook, not a complete messaging subsystem. It runs before normal authentication/pairing and has no declarative subscription model, scoped transport permission model, multi-listener semantics, callback ownership, durable generic outbox, or job lifecycle contract.

A first-class bus avoids per-plugin private gateway integrations and allows one Telegram group/Topic to host several independently authorized plugins without cross-project context mixing.

## 3. Goals

1. Exact route subscriptions using immutable `(platform, chat_id, thread_id)` identity.
2. Fan-out to multiple observer plugins for the same inbound event.
3. Deterministic single-consumer handling for explicit commands and plugin-owned callbacks.
4. Host-authorized, durable outbound text and inline keyboard delivery.
5. Callback ownership, expiration, audience and replay protection.
6. Host-managed bounded background jobs with deduplication, cancellation and recovery.
7. Telegram-first delivery without embedding Telegram or Idea Incubator business logic in core interfaces.
8. Deny-by-default plugin permissions and complete route/plugin audit records.

## 4. Non-goals

- No direct Bot API, token, adapter or arbitrary target access for plugins.
- No routing by mutable Topic title.
- No automatic authorization from group membership or a subscription declaration.
- No Topic creation, deletion, closing, reopening or renaming.
- No general workflow engine, billing engine or Idea Incubator schema in Hermes core.
- No forced migration of existing plugin hooks.

## 5. Architecture

```text
platform adapter
  -> normalized MessageEvent / trusted SessionSource
  -> gateway authorization and event normalization
  -> Plugin Messaging Bus subscription router
       -> observer subscriptions (fan-out)
       -> one eligible consumer (command/callback claim)
       -> normal agent dispatch when unclaimed
  -> host-owned durable outbound outbox
  -> platform adapter delivery
```

The gateway derives sender identity and routing only from trusted normalized source metadata. Plugin-provided fields, raw command text, tool input and model output cannot override it.

## 6. Event envelope

```python
@dataclass(frozen=True)
class PluginMessageEvent:
    event_id: str
    platform: str
    chat_id: str
    thread_id: str | None
    message_id: str | None
    sender_id: str | None
    chat_type: str | None
    kind: Literal["message", "callback"]
    text: str | None
    reply_to_message_id: str | None
    attachments: tuple[AttachmentRef, ...]
    received_at: datetime
```

`event_id` must be stable for transport deduplication. The v1 Telegram adapter uses its trusted update identifier when available and a host-issued durable identifier otherwise.

## 7. Subscription model

Plugins register during load; registration is a permission request, not authority.

```python
ctx.messaging.subscribe(
    subscription_id="idea-incubator",
    routes=[TopicRoute(platform="telegram", chat_id="…", thread_id=None)],
    event_types={"message", "callback"},
    mode="observer",
    handler=on_event,
)
```

The host validates every route against profile-local approved configuration. A route with `thread_id=None` may be approved only as a chat-scoped subscription; a plugin must still enforce its own bound-Topic policy before durable project writes.

### 7.1 Observer mode

Observers receive immutable events and cannot change routing, suppress delivery, or alter another subscription. They are appropriate for audit, classified intake, and project-specific contribution collection.

### 7.2 Consumer mode

Consumers must declare a command namespace or callback ownership pattern. The router selects at most one eligible consumer by explicit priority. A consumer may return `claim`, `allow` or `reject`.

Conflicting equal-priority consumers fail closed and produce an audit record. If no consumer claims an event, normal agent dispatch continues.

## 8. Permission model

```yaml
plugin_messaging:
  idea-incubator:
    inbound:
      - platform: telegram
        chat_id: "<approved-chat>"
        thread_policy: bound_topics_only
        events: [message, callback]
    outbound:
      - platform: telegram
        chat_id: "<approved-chat>"
        thread_policy: bound_topics_only
        types: [text, inline_keyboard]
    jobs:
      max_active_per_route: 1
```

The host denies by default. A plugin cannot derive an outbound permission from an inbound event and cannot broaden a route at runtime.

## 9. Outbound intents and outbox

Plugins submit typed intents, not adapter calls:

```python
await ctx.messaging.enqueue(
    OutboundIntent(
        idempotency_key="project:idea:run:result",
        destination=BoundTopicRef(platform="telegram", chat_id="…", thread_id="…"),
        message=TextMessage(text="Result", keyboard=keyboard),
    )
)
```

The gateway validates plugin identity, granted route, destination type, limits and idempotency. It persists the accepted intent before invoking the platform adapter. Delivery outcome is settled durably with retry classification and audit metadata.

## 10. Inline keyboard and callback routing

Plugins declare semantic actions, never raw unrestricted platform callback data.

```python
Button(label="Approve", action="approve_proposal", payload={"proposal_id": "…"})
```

The gateway creates an opaque callback token bound to:

- `plugin_id`;
- route `(platform, chat_id, thread_id)`;
- action and server-side payload;
- optional authorized actor or role;
- expiry and one-time/idempotent policy.

Callback events are normalized by the adapter and delivered only to the owning plugin’s consumer subscription. Invalid, expired, replayed or cross-route callbacks are rejected and audited.

## 11. Host-managed jobs

A plugin first durably writes its own job. It may then request a named, pre-registered host job signal for the same approved route. The gateway owns task lifecycle, concurrency, timeouts, cancellation, shutdown and restart recovery.

```text
validated event -> plugin durable job write -> host job signal
-> lease-backed worker -> plugin durable outbound intent -> gateway delivery
```

No plugin creates unmanaged background tasks. The initial concurrency key is `(plugin_id, platform, chat_id, thread_id)`.

## 12. Security and privacy

- Trusted identity originates only from normalized host source metadata.
- Plugins receive neither transport credentials nor adapter objects.
- Outbound destinations are host-authorized exact routes.
- Inbound content is data, not privileged instruction.
- Attachments require explicit per-subscription policy before extraction.
- Audit records include plugin, subscription, route, event ID, action and outcome, without credential material.
- Rate, size, retry and job limits are independently enforced by the host.

## 13. Compatibility and migration

`pre_gateway_dispatch` remains supported. The bus is opt-in and does not change legacy hook behavior.

Migration path:

1. Add core contracts, permission parsing and contract tests.
2. Add Telegram message subscriptions and observer routing.
3. Add scoped outbound intents and durable dispatcher.
4. Add inline action registry and callback routing.
5. Migrate Idea Incubator as the first subscriber.
6. Extend adapters incrementally to Discord and Slack.

## 14. Acceptance criteria

1. Two approved observer plugins receive an event from one approved Topic.
2. Unapproved plugins receive no event.
3. A consumer may claim only its declared command/callback namespace.
4. A plugin cannot target an unapproved group or Topic.
5. Gateway sends text and inline keyboard through its adapter only.
6. A callback reaches only the button-owning plugin.
7. Expired, replayed and cross-Topic callbacks are rejected.
8. One active job exists per configured plugin/route key.
9. Restart recovery does not duplicate a leased job or delivered intent.
10. No plugin has access to a token, raw adapter or generic sender.

## 15. Alternatives

### Separate bot per plugin

This is simpler for one isolated application, but duplicates tokens, identity boundaries, permissions, callbacks, delivery/retry and operations. It does not provide a common multi-plugin platform and is not selected for Idea Incubator.

### Keep bespoke `pre_gateway_dispatch` integrations

This is faster initially but recreates inconsistent routing and transport security logic per plugin. It is not selected.

## 16. Fork maintenance

`origin` remains the upstream NousResearch repository; `karassoft` is the user-owned fork remote. KarasSoft changes use feature branches and PRs. `main` remains upstream-aligned. The project is MIT-licensed; the required copyright and license notice are retained.
