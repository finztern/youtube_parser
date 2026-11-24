"""Workers: explore videos (Newest token) and poll hot videos with the cached token."""
from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import datetime, timezone

from client import HEADERS, YtError, new_session, post_next
from hotlist import HotList, Video
from mapper import build_item
from parse import NEWEST_RE, NEXT_RE, VID_RE, first_token, parse_comments

log = logging.getLogger("explorer")


class Explorer:
    def __init__(self, cfg: dict, pool, hot: HotList, pipeline, stats) -> None:
        self.yt, self.pool, self.hot, self.pipe, self.stats = cfg["youtube"], pool, hot, pipeline, stats
        self.reload()

    def reload(self) -> None:
        self.max_pages = int(self.yt.get("max_pages", 3))
        self.hot_pages = int(self.yt.get("hot_max_pages", 2))  # hot videos: the 2nd page only if almost all is new
        self.hot_gain = float(self.yt.get("hot_page_gain", 0.8))
        self.max_age_s = int(float(self.yt.get("max_age_hours", 23)) * 3600)
        self.parent_mode = self.yt.get("parent_toplevel_mode", "self")
        self.exact_keys = list(self.yt.get("exact_time_keys") or [])
        self.reseed_s = float(self.yt.get("reseed_minutes", 10)) * 60  # 0 = no periodic reseed

    async def seed(self) -> None:
        async def one(ch: str) -> None:
            for _ in range(3):
                proxy = self.pool.pick()
                if proxy is None:
                    await asyncio.sleep(2)
                    continue
                try:
                    async with new_session(proxy, timeout=15) as s:
                        r = await s.get(f"https://www.youtube.com/feeds/videos.xml?channel_id={ch}", headers=HEADERS)
                    if r.status_code == 200:
                        for vid in re.findall(r"<yt:videoId>([^<]+)</yt:videoId>", r.text):
                            self.hot.add(vid, fresh=True)
                        return
                except Exception:  # noqa: BLE001
                    pass
        await asyncio.gather(*(one(ch) for ch in self.yt.get("seed_channels", [])))
        for vid in self.yt.get("fallback_videos", []):
            self.hot.add(vid)
        self._last_seed = time.monotonic()
        log.info("seed videos: %d", len(self.hot.videos))

    def _take(self, text: str, v: Video) -> tuple[int, int]:
        """(comments in response, new accepted comments)."""
        comments = parse_comments(text, v.vid, self.exact_keys)
        if comments is None:
            self.stats.inc("parse_fail")
            return 0, 0
        now, new = datetime.now(timezone.utc), 0
        for c in comments:
            self.stats.inc("received")
            item, reason = build_item(c, now, self.max_age_s, self.parent_mode)
            if item is None:
                self.stats.inc(f"skip_{reason}")
            elif self.pipe.offer(item):
                new += 1
                self.stats.inc("fresh")
        return len(comments), new

    async def _post(self, session, payload: dict, acc: list[int]) -> str:
        text, size = await post_next(session, payload)
        self.stats.inc("bytes", size)
        self.stats.inc("req")
        acc[0] += size
        return text

    async def poll(self, session, v: Video, kind: str) -> None:
        acc = [0, 0]  # bytes spent, fresh comments gained: feeds the explore/hot balance
        try:
            await self._poll(session, v, kind, acc)
        finally:
            self.hot.record(kind, acc[1], acc[0])

    async def _poll(self, session, v: Video, kind: str, acc: list[int]) -> None:
        if v.token is None:  # bootstrap: related videos + comments token, then the "Newest" token
            text = await self._post(session, {"videoId": v.vid}, acc)
            for found in VID_RE.findall(text):
                self.hot.add(found)
            token = first_token(text)
            if not token:
                v.dead = True
                self.stats.inc("no_comments")
                return
            text = await self._post(session, {"continuation": token}, acc)
            m = NEWEST_RE.search(text)
            v.token = m.group(1) if m else token
            self.stats.inc("newest_found" if m else "newest_missing")
        token, fresh_total = v.token, 0
        pages, gain = (self.hot_pages, self.hot_gain) if kind == "hot" else (self.max_pages, 0.5)
        for page in range(pages):
            try:
                text = await self._post(session, {"continuation": token}, acc)
            except YtError:
                v.token = None
                raise
            total, new = self._take(text, v)
            if page == 0 and total == 0:
                v.token = None  # stale token or no comments: bootstrap is expensive, do not repeat it soon
                self.hot.chill(v)
                self.stats.inc("token_stale")
                return
            fresh_total += new
            acc[1] += new
            nxt = NEXT_RE.findall(text)
            if not nxt or new < gain * total:
                break  # the rest of the list is older or already seen
            token = nxt[-1]
        self.hot.update(v, fresh_total)
        self.stats.inc("polls")
        self.stats.inc(f"polls_{kind}")

    async def worker(self) -> None:
        proxy, session = None, None
        try:
            while True:
                if not self.pool.usable(proxy):  # node gone, replaced or quarantined
                    if session is not None:
                        await session.close()
                        session = None
                    proxy = self.pool.pick()
                    if proxy is None:
                        await asyncio.sleep(2)
                        continue
                    session = new_session(proxy)
                if not self.pipe.wants_more():  # target_rate reached: buffer is full, do not hit YouTube
                    self.stats.inc("throttled")
                    await asyncio.sleep(0.5)
                    continue
                v, kind = self.hot.pick()
                if v is None:
                    await asyncio.sleep(0.5)
                    continue
                try:
                    await self.poll(session, v, kind)
                    self.pool.report(proxy, True)
                except YtError as exc:
                    self.stats.inc("blocked" if exc.status in (403, 429) else "http_err")
                    if exc.status == 429:
                        self.hot.pacer.slow_down()
                    self.pool.report(proxy, False, exc.status)
                    await asyncio.sleep(0.2)
                except Exception as exc:  # noqa: BLE001
                    self.stats.inc("err")
                    self.pool.report(proxy, False)
                    log.debug("poll error: %s", str(exc)[:80])
                    await asyncio.sleep(0.2)
        finally:
            if session is not None:
                await session.close()

    async def housekeeping(self) -> None:
        while True:
            await asyncio.sleep(30)
            self.hot.pacer.decay()
            self.hot.gc()
            last = getattr(self, "_last_seed", None)  # set by the first seed() after nodes are up
            if last is not None and self.reseed_s > 0 and time.monotonic() - last >= self.reseed_s:
                await self.seed()
