"""SeenCache (comment id, TTL) + url-check stage in `closed` mode (unverified items are dropped)."""
from __future__ import annotations

import asyncio
import logging
import time

from curl_cffi.requests import AsyncSession

log = logging.getLogger("dedup")


class SeenCache:
    def __init__(self, ttl_s: float) -> None:
        self.ttl, self.d = ttl_s, {}

    def add(self, key: str) -> bool:
        """True if the key is new."""
        if key in self.d:
            return False
        self.d[key] = time.monotonic()
        return True

    def purge(self) -> None:
        limit = time.monotonic() - self.ttl
        for k in [k for k, t in self.d.items() if t < limit]:  # insertion order == time order, but keep it simple
            del self.d[k]

    def __len__(self) -> int:
        return len(self.d)


class Pipeline:
    """offer(item) -> url-check batches -> sender. Known urls are dropped; a failed check drops the batch."""

    def __init__(self, cfg: dict, stats, sender, pool=None) -> None:
        yt = cfg["youtube"]
        self.stats, self.sender, self.pool = stats, sender, pool
        self.endpoint, self.proxy = cfg["env"]["url_check_endpoint"], cfg["env"]["url_check_proxy"]
        self.batch = int(yt.get("url_check_batch", 150))
        self.attempts = max(1, int(yt.get("url_check_attempts", 1)))
        self.workers = int(yt.get("url_check_workers", 2))
        self.seen = SeenCache(float(yt.get("seen_ttl_hours", 72)) * 3600)
        self.q: asyncio.Queue = asyncio.Queue(maxsize=50_000)

    def offer(self, item: dict) -> bool:
        """True if the comment was not seen before and went to the url-check queue."""
        if not self.seen.add(item["external_id"]):
            self.stats.inc("repeat")
            return False
        try:
            self.q.put_nowait(item)
        except asyncio.QueueFull:
            self.stats.inc("queue_drop")
        return True

    def _proxy(self) -> str | None:
        return self.pool.pick() if self.proxy == "pool" and self.pool else (self.proxy or None)

    async def _check(self, urls: list[str]) -> set[str] | None:
        for _ in range(self.attempts):
            proxy = self._proxy()
            try:
                async with AsyncSession(impersonate="chrome", timeout=15, proxies={"http": proxy, "https": proxy} if proxy else None) as session:
                    r = await session.post(self.endpoint, json={"urls": urls})
                data = r.json() if r.status_code == 200 else None
            except Exception:  # noqa: BLE001
                data = None
            if isinstance(data, list):
                return {u for u in data if isinstance(u, str)}
        return None

    async def _worker(self) -> None:
        while True:
            batch = [await self.q.get()]
            await asyncio.sleep(0.5)  # let a batch accumulate
            while len(batch) < self.batch and not self.q.empty():
                batch.append(self.q.get_nowait())
            known = await self._check([i["url"] for i in batch])
            if known is None:  # closed mode: unverified is not sent
                self.stats.inc("url_unverified", len(batch))
                self.stats.inc("url_check_fail")
                continue
            for item in batch:
                if item["url"] in known:
                    self.stats.inc("known")
                else:
                    self.stats.inc("new")
                    await self.sender.put(item)

    async def run(self) -> None:
        async def purge_loop() -> None:
            while True:
                await asyncio.sleep(300)
                self.seen.purge()
        await asyncio.gather(purge_loop(), *(self._worker() for _ in range(self.workers)))
