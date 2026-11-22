"""Counters and the periodic log line."""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter

log = logging.getLogger("stats")


class Stats:
    def __init__(self) -> None:
        self.c: Counter[str] = Counter()

    def inc(self, key: str, n: int = 1) -> None:
        self.c[key] += n

    async def run(self, pool, hot, pipeline, interval: float = 10.0) -> None:
        last, t_last = Counter(self.c), time.monotonic()
        while True:
            await asyncio.sleep(interval)
            now = time.monotonic()
            dt, c, d = now - t_last, self.c, lambda k: self.c[k] - last[k]
            req = max(1, d("req") + d("blocked") + d("http_err") + d("err"))
            log.info(
                "polls/s %.1f | fresh %d | known %d | NEW %d | sent/s %.1f | blocked %.0f%% | nodes %d/%d | %.1f Mbit/s"
                " | hot %d videos %d hot_int %.0fs seen %d | unverified %d send_failed %d",
                d("polls") / dt, d("fresh"), d("known"), d("new"), d("sent") / dt, 100 * (d("blocked") + d("err")) / req,
                pool.live_count(), len(pool.proxies), d("bytes") * 8 / 1e6 / dt,
                len(hot.hot), len(hot.videos), hot.pacer.current, len(pipeline.seen), d("url_unverified"), d("send_failed"),
            )
            last, t_last = Counter(c), now
