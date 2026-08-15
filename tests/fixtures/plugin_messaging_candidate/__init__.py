"""Directory-discovered candidate plugin for the host messaging contract."""

from gateway.plugin_messaging import ConsumerDeclaration, TopicRoute

ROUTE = TopicRoute("telegram", "-100123", "42")
IDEMPOTENCY_KEY = "candidate-delivery"
direct_replies = []
confirmations = []
_messaging = None


def _claim_reply(event):
    direct_replies.append(event)
    return {"action": "claim"}


def request_delivery():
    return _messaging.enqueue_text(
        idempotency_key=IDEMPOTENCY_KEY,
        route=ROUTE,
        text="candidate outbound",
    )


def register(ctx):
    global _messaging
    _messaging = ctx.messaging
    _messaging.subscribe(
        subscription_id="candidate-direct-reply",
        routes=[ROUTE],
        event_types={"message"},
        mode="consumer",
        handler=_claim_reply,
        consumer=ConsumerDeclaration(direct_reply=True),
    )
    _messaging.register_delivery_confirmation(
        idempotency_key=IDEMPOTENCY_KEY,
        route=ROUTE,
        handler=confirmations.append,
    )
