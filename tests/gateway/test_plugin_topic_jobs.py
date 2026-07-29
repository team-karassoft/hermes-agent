"""Regression tests for host-owned bounded plugin Topic jobs."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import MessageEvent
from gateway.plugin_jobs import (
    PluginTopicJobRegistration,
    PluginTopicJobScheduler,
    PluginTopicRoute,
    ScopedPluginTopicJobs,
)
from gateway.session import SessionSource
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest


def _topic_event(*, user_id: str = "owner", thread_id: str | None = "42") -> MessageEvent:
    return MessageEvent(
        text="/debate",
        message_id="m1",
        source=SessionSource(
            platform=Platform.TELEGRAM,
            user_id=user_id,
            chat_id="-1001",
            chat_type="forum",
            thread_id=thread_id,
        ),
    )


def _make_runner() -> object:
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(
        platforms={Platform.TELEGRAM: PlatformConfig(enabled=True)},
    )
    runner.adapters = {Platform.TELEGRAM: SimpleNamespace()}
    runner.pairing_store = MagicMock()
    runner.pairing_store.is_approved.return_value = False
    runner.pairing_store._is_rate_limited.return_value = False
    runner.session_store = MagicMock()
    runner._running_agents = {}
    runner._update_prompt_pending = {}
    runner._plugin_topic_job_scheduler = PluginTopicJobScheduler()
    return runner


@pytest.mark.asyncio
async def test_scheduler_suppresses_duplicate_active_plugin_topic_job() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[PluginTopicRoute] = []

    async def worker(route: PluginTopicRoute) -> None:
        calls.append(route)
        started.set()
        await release.wait()

    scheduler = PluginTopicJobScheduler()
    registration = PluginTopicJobRegistration(
        name="idea-incubator-debate-worker",
        callback=worker,
        timeout_seconds=30,
    )
    route = PluginTopicRoute(chat_id="-1001", thread_id="42")

    assert scheduler.request(plugin_id="idea-incubator", route=route, registration=registration)
    await started.wait()
    assert not scheduler.request(plugin_id="idea-incubator", route=route, registration=registration)
    assert calls == [route]

    release.set()
    await asyncio.sleep(0)
    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_authorized_plugin_hook_starts_only_its_registered_topic_worker() -> None:
    started = asyncio.Event()

    async def worker(_route: PluginTopicRoute) -> None:
        started.set()

    manager = PluginManager()
    context = PluginContext(PluginManifest(name="idea-incubator", source="user"), manager)
    context.register_topic_job(
        name="idea-incubator-debate-worker",
        callback=worker,
        timeout_seconds=30,
    )

    def pre_dispatch(*, topic_job):
        assert topic_job.request()
        return {"action": "skip", "reason": "idea-debate-queued"}

    context.register_hook("pre_gateway_dispatch", pre_dispatch)
    scheduler = PluginTopicJobScheduler()
    topic_jobs = ScopedPluginTopicJobs(
        inbound_event=_topic_event(),
        authorized=True,
        scheduler=scheduler,
        registrations=manager._plugin_topic_jobs,
    )

    results = manager.invoke_hook("pre_gateway_dispatch", plugin_topic_jobs=topic_jobs)

    assert results == [{"action": "skip", "reason": "idea-debate-queued"}]
    await started.wait()
    await scheduler.shutdown()


@pytest.mark.asyncio
async def test_scheduler_cancels_active_jobs_during_gateway_shutdown() -> None:
    cancelled = asyncio.Event()

    async def worker(_route: PluginTopicRoute) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    scheduler = PluginTopicJobScheduler()
    assert scheduler.request(
        plugin_id="idea-incubator",
        route=PluginTopicRoute(chat_id="-1001", thread_id="42"),
        registration=PluginTopicJobRegistration(
            name="idea-incubator-debate-worker",
            callback=worker,
            timeout_seconds=30,
        ),
    )
    await asyncio.sleep(0)

    await scheduler.shutdown()

    assert cancelled.is_set()
    assert scheduler.active_count == 0


@pytest.mark.asyncio
async def test_unauthorized_or_non_topic_event_cannot_launch_plugin_job(monkeypatch) -> None:
    launched = 0

    def _fake_hook(name, **kwargs):
        nonlocal launched
        if name == "pre_gateway_dispatch" and kwargs.get("plugin_topic_jobs") is not None:
            launched += 1
        return [{"action": "skip", "reason": "handled"}]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _fake_hook)
    runner = _make_runner()
    monkeypatch.delenv("TELEGRAM_ALLOWED_USERS", raising=False)

    await runner._handle_message(_topic_event(user_id="not-owner"))
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "owner")
    await runner._handle_message(_topic_event(thread_id=None))

    assert launched == 0
    assert runner._plugin_topic_job_scheduler.active_count == 0


@pytest.mark.asyncio
async def test_gateway_exposes_delivery_only_after_authenticated_topic_check(monkeypatch) -> None:
    def _fake_hook(name, **kwargs):
        assert name == "pre_gateway_dispatch"
        delivery = kwargs["plugin_delivery"]
        assert delivery is not None
        return [delivery.for_plugin("idea-incubator").reply("queued")]

    monkeypatch.setattr("hermes_cli.plugins.invoke_hook", _fake_hook)
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "owner")
    runner = _make_runner()
    adapter = SimpleNamespace(
        send=AsyncMock(return_value=SimpleNamespace(success=True, message_id="100"))
    )
    runner.adapters = {Platform.TELEGRAM: adapter}

    await runner._handle_message(_topic_event())

    adapter.send.assert_awaited_once_with(
        chat_id="-1001", content="queued", reply_to=None, metadata={"thread_id": "42"}
    )
