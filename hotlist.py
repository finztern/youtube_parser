"""Video pool: explore queue, hot set (priority by fresh-comment gain), global pacing."""
from __future__ import annotations

import random
import time
from collections import deque


class Video:
    __slots__ = ("vid", "token", "last_poll", "ema", "polls", "dead", "nr")

    def __init__(self, vid: str) -> None:
        self.vid, self.token, self.last_poll, self.ema, self.polls, self.dead = vid, None, 0.0, 0.0, 0, False
        self.nr = 0.5  # EMA of the share of comments unknown to url-check (NEW)


class Pacer:
    """hot_interval grows after 429 and decays back to the configured value."""

    def __init__(self, base: float) -> None:
        self.base, self.current, self._last_slow = base, base, 0.0

    def set_base(self, base: float) -> None:
        self.base = base
        self.current = min(max(self.current, base), base * 8)

    def slow_down(self) -> None:
        now = time.monotonic()
        if now - self._last_slow > 10:  # one step per burst of 429s
            self.current = min(self.current * 1.25, self.base * 8)
            self._last_slow = now

    def decay(self) -> None:
        if time.monotonic() - self._last_slow > 60:
            self.current = max(self.base, self.current * 0.9)


class HotList:
    def __init__(self, yt: dict, pacer: Pacer) -> None:
        self.pacer, self.yt = pacer, yt
        self.reload()
        self.videos: dict[str, Video] = {}
        self.ids: list[str] = []
        self.explore: deque[str] = deque()
        self.hot: set[Video] = set()

    def reload(self) -> None:
        yt = self.yt
        self.explore_share = float(yt.get("explore_share", 0.7))
        self.hot_min = float(yt.get("hot_min_gain", 1.0))
        self.max_hot = int(yt.get("max_hot_videos", 2000))
        self.max_videos = int(yt.get("max_videos", 100_000))
        self.pacer.set_base(float(yt.get("hot_interval", 30)))

    def add(self, vid: str) -> None:
        if vid not in self.videos and len(self.videos) < self.max_videos:
            self.videos[vid] = Video(vid)
            self.ids.append(vid)
            self.explore.append(vid)

    def pick(self) -> tuple[Video | None, str]:
        now, interval = time.monotonic(), self.pacer.current
        if random.random() >= self.explore_share:
            best = None
            for v in self.hot:
                if now - v.last_poll >= interval and (best is None or v.ema * (0.1 + v.nr) > best.ema * (0.1 + best.nr)):
                    best = v
            if best:
                best.last_poll = now
                return best, "hot"
        while self.explore:
            v = self.videos.get(self.explore.popleft())
            if v is not None and not v.polls and not v.dead:
                v.last_poll = now
                return v, "explore"
        for _ in range(20):  # nothing to explore: re-poll a random known video
            if not self.ids:
                break
            v = self.videos.get(random.choice(self.ids))
            if v is not None and not v.dead and now - v.last_poll >= interval:
                v.last_poll = now
                return v, "repoll"
        return None, ""

    def update(self, v: Video, fresh: int) -> None:
        v.polls += 1
        # the first poll returns the backlog of the last day: it must not make a video hot, the re-poll is a trial
        v.ema = self.hot_min * 1.5 if v.polls == 1 else 0.5 * v.ema + 0.5 * fresh
        if v.ema >= self.hot_min:
            self.hot.add(v)
            if len(self.hot) > self.max_hot:
                self.hot.discard(min(self.hot, key=lambda x: x.ema))
        else:
            self.hot.discard(v)

    def feedback(self, vid: str, new: int, total: int) -> None:
        """url-check result for a video: new of total comments were unknown to the network."""
        v = self.videos.get(vid)
        if v is not None and total:
            v.nr = 0.7 * v.nr + 0.3 * (new / total)

    def gc(self) -> None:
        """Keep memory bounded: over the cap drop dead videos, then the coldest polled ones."""
        if len(self.videos) <= self.max_videos * 0.9:
            return
        drop = [v for v in self.videos.values() if v.dead]
        if len(self.videos) - len(drop) > self.max_videos * 0.9:
            cold = sorted((v for v in self.videos.values() if v.polls and v not in self.hot and not v.dead), key=lambda x: x.last_poll)
            drop += cold[: len(cold) // 2]
        for v in drop:
            self.videos.pop(v.vid, None)
            self.hot.discard(v)
        self.ids = list(self.videos)
