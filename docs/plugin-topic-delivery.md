# Scoped plugin Topic delivery and jobs

`GatewayRunner` owns two narrowly scoped plugin capabilities for authenticated
Telegram Forum Topics. A plugin never receives a Telegram adapter, bot token,
configuration, generic sender, destination selector, or direct Bot API client.

## Immediate reply

A `pre_gateway_dispatch` callback may declare `delivery`. The host only binds it
after its real `_is_user_authorized(source)` decision, and only when the inbound
value is a trusted `MessageEvent` with a `SessionSource` for a Telegram Forum
Topic.

```python

def pre_dispatch(*, event, delivery=None, **_kwargs):
    if event.get_command() != "idea" or delivery is None:
        return None
    return delivery.reply("Idea request queued")
```

A hook returns `delivery.reply(content)`. The returned opaque request carries no
target and `GatewayRunner` immediately delivers it to that exact inbound
`(chat_id, thread_id)` using the normal adapter contract with
`metadata={"thread_id": thread_id}`, then stops normal dispatch for that event.

## Durable approved-route outbox

During `register(ctx)`, a plugin can register one durable outbox contract:

```python
ctx.register_topic_outbox_dispatcher(
    approved_routes=[PluginTopicRoute(chat_id="-100...", thread_id="42")],
    claim=store.claim_one_durable_record,
    mark_delivered=store.mark_delivered,
    mark_failed=store.mark_failed,
)
```

`claim` must atomically lease or claim a record persisted by the plugin before
host dispatch. `mark_delivered` and `mark_failed` must durably settle that same
record. A `PluginOutboxRecord` contains a record id, content, chat id, and
thread id; the host accepts a route only if it exactly equals one of that
plugin's registered routes. Unapproved chat/topic records are settled as
`unapproved-route` without any send. `delivery.dispatch_outbox(max_records=1)`
is plugin-bound and bounded to 1–32 claimed records. It cannot operate another
plugin's outbox.

## Topic jobs

`ctx.register_topic_job(...)` remains a separate host-owned scheduler contract.
The hook receives `topic_job.request()` only for the same authenticated Forum
Topic and only for that plugin's one registered worker. The plugin must first
perform its durable enqueue/write; it calls `request()` only after that durable
write succeeds. `GatewayRunner` owns task creation, has one active task per
`(plugin, chat_id, thread_id)`, applies the registered timeout, and cancels and
awaits tracked tasks during shutdown.

This API is intentionally limited to Topic-bound replies, explicit durable
outbox records, and bounded Topic workers. It is not a generic messaging or
background-task API.
