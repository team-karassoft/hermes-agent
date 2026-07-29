# Host-owned plugin Topic jobs

`GatewayRunner` can run a bounded async worker for an authorized Telegram Forum
Topic without allowing plugin code to create unmanaged `asyncio` tasks.

## Idea Incubator registration

The Idea Incubator plugin registers exactly one worker during `register(ctx)`:

```python
async def debate_worker(route):
    # Claim/process only the plugin's durable queue for this route.
    await store.process_one_debate(route.chat_id, route.thread_id)

ctx.register_topic_job(
    name="idea-incubator-debate-worker",
    callback=debate_worker,
    timeout_seconds=120,
)


def pre_dispatch(event, topic_job=None, **_kwargs):
    if event.get_command() != "debate" or topic_job is None:
        return None
    store.enqueue_debate_after_authorization()  # durable write first
    topic_job.request()                         # starts at most one active job
    return {"action": "skip", "reason": "idea-debate-queued"}

ctx.register_hook("pre_gateway_dispatch", pre_dispatch)
```

The plugin must durably write its queue record before calling `request()`.
The worker must make bounded, idempotent progress; a running worker is capped
by its host-enforced `timeout_seconds` (1–300).

## Security and lifecycle boundary

- `pre_gateway_dispatch` still executes before normal auth/pairing, preserving
  existing hook behavior. It gets `topic_job` only after the gateway's ordinary
  `_is_user_authorized` decision succeeds and only for an inbound Telegram
  Forum Topic.
- The capability has one operation: `request()` with no target, job-name,
  command, adapter, token, or sender argument. It resolves only to the calling
  plugin's one registered job and the authenticated inbound `(chat_id, topic)`.
- `GatewayRunner` creates and tracks the task, de-duplicates one active task
  per `(plugin, chat_id, topic)`, logs callback errors/timeouts, and cancels and
  awaits all such work before adapter teardown on shutdown.
- Plugin callbacks receive only `PluginTopicRoute`; plugin code must not call
  `asyncio.create_task` for this workflow.
- Internal, unauthorized, non-Telegram, and non-Topic events cannot obtain the
  capability and therefore cannot launch a plugin Topic job.

This API intentionally supports only the bounded Idea Incubator worker path;
it is not a generic command runner or background-task facility.
