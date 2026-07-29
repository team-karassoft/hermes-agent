"""Host-owned, topic-scoped async jobs for gateway plugins.

Plugins register one named, bounded callback.  They never create the task and
never receive a platform adapter, credentials, a generic sender, or a target
argument.  A pre-gateway-dispatch hook may only signal its own registered job
through a capability bound to an authenticated inbound Telegram forum Topic.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
from dataclasses import dataclass
from typing import Awaitable, Callable, Mapping

from gateway.config import Platform
from gateway.platforms.base import MessageEvent
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

_MAX_CALLBACK_TIMEOUT_SECONDS = 300
_JOB_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")


@dataclass(frozen=True)
class PluginTopicRoute:
    """A topic identity derived only from an authenticated inbound event."""

    chat_id: str
    thread_id: str


@dataclass(frozen=True)
class PluginTopicJobRegistration:
    """The sole bounded worker callback a plugin may register."""

    name: str
    callback: Callable[[PluginTopicRoute], Awaitable[None]]
    timeout_seconds: int

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _JOB_NAME_RE.fullmatch(self.name):
            raise ValueError("job name must be a lowercase hyphenated identifier")
        if not inspect.iscoroutinefunction(self.callback):
            raise TypeError("plugin topic job callback must be async")
        if not isinstance(self.timeout_seconds, int) or not 1 <= self.timeout_seconds <= _MAX_CALLBACK_TIMEOUT_SECONDS:
            raise ValueError(f"timeout_seconds must be between 1 and {_MAX_CALLBACK_TIMEOUT_SECONDS}")


class PluginTopicJobScheduler:
    """Gateway-owned task tracking, de-duplication, cancellation, and logging."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[str, str, str], asyncio.Task[None]] = {}
        self._closing = False

    @property
    def active_count(self) -> int:
        return sum(not task.done() for task in self._tasks.values())

    def request(
        self,
        *,
        plugin_id: str,
        route: PluginTopicRoute,
        registration: PluginTopicJobRegistration,
    ) -> bool:
        """Start exactly one active job per plugin and Topic; return whether started."""
        if self._closing:
            logger.info("Plugin topic job request ignored during gateway shutdown: plugin=%s", plugin_id)
            return False
        key = (plugin_id, route.chat_id, route.thread_id)
        current = self._tasks.get(key)
        if current is not None and not current.done():
            logger.info("Plugin topic job already active: plugin=%s topic=%s/%s", plugin_id, route.chat_id, route.thread_id)
            return False
        task = asyncio.create_task(
            self._run(key, registration, route),
            name=f"plugin-topic-job:{plugin_id}:{registration.name}:{route.chat_id}:{route.thread_id}",
        )
        self._tasks[key] = task
        return True

    async def _run(
        self,
        key: tuple[str, str, str],
        registration: PluginTopicJobRegistration,
        route: PluginTopicRoute,
    ) -> None:
        try:
            await asyncio.wait_for(registration.callback(route), timeout=registration.timeout_seconds)
        except asyncio.CancelledError:
            logger.info("Plugin topic job cancelled: plugin=%s topic=%s/%s", key[0], route.chat_id, route.thread_id)
            raise
        except asyncio.TimeoutError:
            logger.error("Plugin topic job timed out: plugin=%s job=%s", key[0], registration.name)
        except Exception:
            logger.exception("Plugin topic job failed: plugin=%s job=%s", key[0], registration.name)
        finally:
            current = self._tasks.get(key)
            if current is asyncio.current_task():
                self._tasks.pop(key, None)

    async def shutdown(self) -> None:
        """Cancel and await all host-tracked jobs before gateway teardown."""
        self._closing = True
        tasks = [task for task in self._tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()


class ScopedPluginTopicJob:
    """Plugin-bound signal capability with no job-name or target argument."""

    def __init__(
        self,
        *,
        scheduler: PluginTopicJobScheduler,
        plugin_id: str,
        route: PluginTopicRoute,
        registrations: Mapping[str, PluginTopicJobRegistration],
    ) -> None:
        self._scheduler = scheduler
        self._plugin_id = plugin_id
        self._route = route
        self._registrations = registrations

    def request(self) -> bool:
        """Signal this plugin's sole job after it durably writes its queue record."""
        registration = self._registrations.get(self._plugin_id)
        if registration is None:
            raise PermissionError("plugin has no registered topic job")
        return self._scheduler.request(
            plugin_id=self._plugin_id,
            route=self._route,
            registration=registration,
        )


class ScopedPluginTopicJobs:
    """Host-only factory for capabilities tied to an authorized topic event."""

    def __init__(
        self,
        *,
        inbound_event: MessageEvent,
        authorized: bool,
        scheduler: PluginTopicJobScheduler,
        registrations: Mapping[str, PluginTopicJobRegistration],
    ) -> None:
        if authorized is not True:
            raise PermissionError("plugin topic jobs require an authenticated inbound event")
        if not isinstance(inbound_event, MessageEvent) or not isinstance(inbound_event.source, SessionSource):
            raise TypeError("inbound_event must have a trusted SessionSource")
        source = inbound_event.source
        if (
            source.platform != Platform.TELEGRAM
            or source.chat_type != "forum"
            or not source.chat_id
            or not source.thread_id
        ):
            raise ValueError("plugin topic jobs require an inbound Telegram forum Topic")
        self._scheduler = scheduler
        self._route = PluginTopicRoute(chat_id=str(source.chat_id), thread_id=str(source.thread_id))
        self._registrations = registrations

    def for_plugin(self, plugin_id: str) -> ScopedPluginTopicJob:
        if not isinstance(plugin_id, str) or not plugin_id:
            raise ValueError("plugin_id is required")
        return ScopedPluginTopicJob(
            scheduler=self._scheduler,
            plugin_id=plugin_id,
            route=self._route,
            registrations=self._registrations,
        )
