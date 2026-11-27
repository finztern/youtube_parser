"""Batches of items -> local STORE_ENDPOINT as {"items": [...]}."""
from __future__ import annotations

import asyncio
import logging
import math
import time

import httpx

log = logging.getLogger("sender")


class Sender:
    def __init__(self, cfg: dict, stats) -> None:
        self.endpoint = cfg["env"]["store_endpoint"]
        self.yt = cfg["youtube"]
        self.stats = stats
        self.q: asyncio.Queue = asyncio.Queue(maxsize=20_000)  # hard cap; the soft limit is buffer_size (see need)
        self.reload()

    def reload(self) -> None:
        self.size = max(1, min(100, int(self.yt.get("batch_size", 50))))
        self.rate = max(0.0, float(self.yt.get("target_rate", 0)))  # items/s to the collector, 0 = unlimited
        self.buffer = max(1, int(self.yt.get("buffer_size", 1000)))
        self.send_limit = max(1, int(self.yt.get("send_queue_limit", 500)))  # max items waiting in send_q

    def need(self, pending: int = 0) -> bool:
        """True if polling YouTube should go on: always when unlimited, else while queued items < buffer_size."""
        return self.rate <= 0 or self.q.qsize() + pending < self.buffer

    async def put(self, item: dict) -> None:
        while self.q.qsize() >= self.send_limit:  # backpressure: url-check workers wait, verified items are not lost
            await asyncio.sleep(0.2)
        try:
            self.q.put_nowait(item)
        except asyncio.QueueFull:
            self.stats.inc("send_queue_drop")

    async def _post(self, client: httpx.AsyncClient, batch: list[dict]) -> None:
        for attempt in range(3):
            try:
                r = await client.post(self.endpoint, json={"items": batch})
                if r.status_code < 300:
                    self.stats.inc("sent", len(batch))
                    return
                log.warning("store endpoint http %s", r.status_code)
            except Exception as exc:  # noqa: BLE001
                log.warning("store endpoint error: %s", str(exc)[:80])
            await asyncio.sleep(1 + attempt * 2)
        self.stats.inc("send_failed", len(batch))

    async def _worker(self, client: httpx.AsyncClient) -> None:
        while True:
            size = min(self.size, max(1, math.ceil(self.rate))) if self.rate else self.size
            batch = [await self.q.get()]
            try:
                while len(batch) < size:
                    batch.append(await asyncio.wait_for(self.q.get(), timeout=2.0))
            except asyncio.TimeoutError:
                pass
            if self.rate:  # shared steady pace across workers, no catch-up bursts
                now = time.monotonic()
                self._next_at = max(self._next_at, now)
                wait, self._next_at = self._next_at - now, self._next_at + len(batch) / self.rate
                if wait > 0:
                    await asyncio.sleep(wait)
            await self._post(client, batch)

    async def run(self) -> None:
        self._next_at = 0.0
        n = max(1, int(self.yt.get("send_workers", 4)))
        limits = httpx.Limits(max_connections=n * 2)
        async with httpx.AsyncClient(timeout=20, limits=limits) as client:
            await asyncio.gather(*(self._worker(client) for _ in range(n)))
