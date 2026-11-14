"""In-memory coordination between the two end-uploads of one batch.

Lives only in the process (single uvicorn worker). On restart every
non-terminal batch is failed by the startup recovery anyway, so nothing
here needs to survive a restart.
"""
from __future__ import annotations

import asyncio


class BatchRuntime:
    __slots__ = ("fail_event", "all_clear", "inflight", "failure_reason")

    def __init__(self) -> None:
        # Set as soon as either end fails; the peer's receive loop races its
        # body reads against this event and aborts without waiting for the
        # rest of a slow request body.
        self.fail_event = asyncio.Event()
        # Set while no end-upload of this batch is in flight.
        self.all_clear = asyncio.Event()
        self.all_clear.set()
        self.inflight: set[str] = set()
        self.failure_reason: str | None = None

    def enter(self, end: str) -> None:
        self.inflight.add(end)
        self.all_clear.clear()

    def exit(self, end: str) -> None:
        self.inflight.discard(end)
        if not self.inflight:
            self.all_clear.set()

    def fail(self, reason: str) -> None:
        if self.failure_reason is None:
            self.failure_reason = reason
        self.fail_event.set()


_runtimes: dict[str, BatchRuntime] = {}


def get_or_create(batch_id: str) -> BatchRuntime:
    rt = _runtimes.get(batch_id)
    if rt is None:
        rt = BatchRuntime()
        _runtimes[batch_id] = rt
    return rt


def pop_if(batch_id: str, rt: BatchRuntime) -> None:
    if _runtimes.get(batch_id) is rt:
        del _runtimes[batch_id]
