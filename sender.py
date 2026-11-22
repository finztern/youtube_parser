"""Batches of items -> local STORE_ENDPOINT as {"items": [...]}."""
from __future__ import annotations

import asyncio
import logging

import httpx

log = logging.getLogger("sender")


class Sender:
    def __init__(self, cfg: dict, stats) -> None:
        self.endpoint = cfg["env"]["store_endpoint"]
        self.size = max(1, min(100, int(cfg["youtube"].get("batch_size", 50))))
        self.stats = stats
        self.q: asyncio.Queue = asyncio.Queue(maxsize=20_000)

    async def put(self, item: dict) -> None:
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

    async def run(self) -> None:
        async with httpx.AsyncClient(timeout=20) as client:
            while True:
                batch = [await self.q.get()]
                try:
                    while len(batch) < self.size:
                        batch.append(await asyncio.wait_for(self.q.get(), timeout=2.0))
                except asyncio.TimeoutError:
                    pass
                await self._post(client, batch)
