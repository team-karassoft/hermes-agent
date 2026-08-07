"""Focused tests for host-owned plugin gateway interval tasks."""

from __future__ import annotations

import asyncio

import pytest
from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.run import (
    GatewayRunner,
    _PLUGIN_INTERVAL_BACKOFF_CAP_SECONDS,
    _plugin_interval_failure_backoff,
)
from hermes_cli.plugins import (
    GatewayIntervalTaskRegistration,
    PluginContext,
    PluginManager,
    PluginManifest,
)


def _context() -> tuple[PluginContext, PluginManager]:
    manager = PluginManager()
    return (
        PluginContext(
            PluginManifest(name="Example Plugin", key="example-plugin"),
            manager,
        ),
        manager,
    )


async def _callback_with_required_argument(_value) -> None:
    return None


@pytest.mark.asyncio
async def test_gateway_start_invokes_interval_start_after_running(monkeypatch, tmp_path) -> None:
    """Startup must not spend its only interval-start call before becoming live."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("GATEWAY_ALLOW_ALL_USERS", "true")
    config = GatewayConfig(
        platforms={
            Platform.YUANBAO: PlatformConfig(
                enabled=True,
                extra={"dm_policy": "open"},
            ),
        },
        sessions_dir=tmp_path / "sessions",
    )
    runner = GatewayRunner(config)
    monkeypatch.setattr(runner, "_create_adapter", lambda platform, config: None)
    running_at_interval_start = []

    def record_interval_start() -> None:
        running_at_interval_start.append(runner._running)

    monkeypatch.setattr(runner, "_start_gateway_interval_tasks", record_interval_start)

    assert await runner.start() is True
    assert running_at_interval_start == [True]

    heartbeat = getattr(runner, "_loop_heartbeat_task", None)
    if heartbeat is not None:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)


@pytest.mark.parametrize(
    ("name", "interval", "callback", "error"),
    [
        ("", 30, None, ValueError),
        ("bad name", 30, None, ValueError),
        ("task", True, None, TypeError),
        ("task", 29.9, None, ValueError),
        ("task", float("inf"), None, ValueError),
        ("task", 30, lambda: None, TypeError),
        ("task", 30, _callback_with_required_argument, TypeError),
    ],
)
def test_gateway_interval_task_registration_validation(
    name, interval, callback, error
) -> None:
    context, _manager = _context()

    async def valid_callback() -> None:
        return None

    with pytest.raises(error):
        context.register_gateway_interval_task(
            name,
            interval,
            valid_callback if callback is None else callback,
        )


def test_gateway_interval_task_registration_is_namespaced_and_unique() -> None:
    context, manager = _context()

    async def callback() -> None:
        return None

    context.register_gateway_interval_task("refresh", 30, callback)

    registrations = manager.get_gateway_interval_tasks()
    assert registrations == (
        GatewayIntervalTaskRegistration(
            name="example-plugin:refresh",
            interval_seconds=30.0,
            callback=callback,
        ),
    )
    with pytest.raises(ValueError, match="already registered"):
        context.register_gateway_interval_task("refresh", 30, callback)


@pytest.mark.asyncio
async def test_gateway_interval_tasks_start_once_only_while_running(monkeypatch) -> None:
    registration = GatewayIntervalTaskRegistration(
        name="example-plugin:refresh",
        interval_seconds=30.0,
        callback=asyncio.sleep,
    )

    class _Manager:
        def discover_and_load(self):
            return None

        def get_gateway_interval_tasks(self):
            return (registration,)

    monkeypatch.setattr(
        "hermes_cli.plugins.get_plugin_manager",
        lambda: _Manager(),
    )
    runner = object.__new__(GatewayRunner)
    runner._running = False
    runner._gateway_interval_tasks_started = False
    runner._gateway_interval_task_handles = {}

    runner._start_gateway_interval_tasks()
    assert runner._gateway_interval_task_handles == {}

    runner._running = True
    runner._start_gateway_interval_tasks()
    first_task = runner._gateway_interval_task_handles[registration.name]
    runner._start_gateway_interval_tasks()

    assert runner._gateway_interval_task_handles == {registration.name: first_task}
    assert first_task.get_name() == f"plugin-interval:{registration.name}"

    await runner._stop_gateway_interval_tasks()


@pytest.mark.asyncio
async def test_gateway_interval_tasks_discover_standalone_plugin_before_snapshot(
    monkeypatch,
) -> None:
    """Gateway startup must load standalone registrations before taking its snapshot."""
    context, manager = _context()
    discovered = False

    async def callback() -> None:
        return None

    def discover_and_load() -> None:
        nonlocal discovered
        discovered = True
        context.register_gateway_interval_task("refresh", 30, callback)

    monkeypatch.setattr(manager, "discover_and_load", discover_and_load)
    monkeypatch.setattr(
        "hermes_cli.plugins.get_plugin_manager",
        lambda: manager,
    )
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner._gateway_interval_tasks_started = False
    runner._gateway_interval_task_handles = {}

    runner._start_gateway_interval_tasks()

    assert discovered is True
    task = runner._gateway_interval_task_handles["example-plugin:refresh"]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_gateway_interval_callback_invocations_do_not_overlap() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    maximum_active = 0
    calls = 0

    async def callback() -> None:
        nonlocal active, maximum_active, calls
        calls += 1
        active += 1
        maximum_active = max(maximum_active, active)
        entered.set()
        try:
            await release.wait()
        finally:
            active -= 1

    registration = GatewayIntervalTaskRegistration(
        name="example-plugin:refresh",
        interval_seconds=0.001,
        callback=callback,
    )
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner._gateway_interval_task_handles = {}
    task = asyncio.create_task(runner._run_gateway_interval_task(registration))
    runner._gateway_interval_task_handles[registration.name] = task

    await asyncio.wait_for(entered.wait(), timeout=1)
    await asyncio.sleep(0.02)
    assert calls == 1
    assert maximum_active == 1

    runner._running = False
    release.set()
    await asyncio.wait_for(task, timeout=1)


def test_gateway_interval_failure_backoff_is_exponential_and_bounded() -> None:
    assert [
        _plugin_interval_failure_backoff(30, failures)
        for failures in range(1, 6)
    ] == [30, 60, 120, 240, _PLUGIN_INTERVAL_BACKOFF_CAP_SECONDS]
    assert (
        _plugin_interval_failure_backoff(30, 100)
        == _PLUGIN_INTERVAL_BACKOFF_CAP_SECONDS
    )


@pytest.mark.asyncio
async def test_failures_back_off_to_cap_and_success_resets_interval(
    monkeypatch,
) -> None:
    delays = []
    outcomes = iter(["fail"] * 5 + ["success", "final-fail"])
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner._gateway_interval_task_handles = {}

    async def fake_sleep(delay) -> None:
        delays.append(delay)

    async def callback() -> None:
        outcome = next(outcomes)
        if outcome == "success":
            return
        if outcome == "final-fail":
            runner._running = False
        raise RuntimeError("secret-bearing plugin failure")

    monkeypatch.setattr("gateway.run.asyncio.sleep", fake_sleep)
    await runner._run_gateway_interval_task(
        GatewayIntervalTaskRegistration(
            name="example-plugin:refresh",
            interval_seconds=30,
            callback=callback,
        )
    )

    assert delays == [30, 30, 60, 120, 240, 300, 30]


@pytest.mark.asyncio
async def test_stop_gateway_interval_tasks_cancels_awaits_and_clears_handles() -> None:
    callback_started = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def callback() -> None:
        callback_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            callback_cancelled.set()

    registration = GatewayIntervalTaskRegistration(
        name="example-plugin:refresh",
        interval_seconds=0,
        callback=callback,
    )
    runner = object.__new__(GatewayRunner)
    runner._running = True
    runner._gateway_interval_task_handles = {}
    task = asyncio.create_task(runner._run_gateway_interval_task(registration))
    runner._gateway_interval_task_handles[registration.name] = task

    await asyncio.wait_for(callback_started.wait(), timeout=1)
    await runner._stop_gateway_interval_tasks()

    assert task.cancelled()
    assert callback_cancelled.is_set()
    assert runner._gateway_interval_task_handles == {}
