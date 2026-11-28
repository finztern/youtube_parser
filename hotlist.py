"""Video pool: explore queue, hot set (priority by fresh-comment gain), global pacing."""
from __future__ import annotations

import random
import time
from collections import deque


class Video:
    __slots__ = ("vid", "token", "last_poll", "ema", "polls", "dead", "nr", "checks", "cold_until")

    def __init__(self, vid: str) -> None:
        self.vid, self.token, self.last_poll, self.ema, self.polls, self.dead = vid, None, 0.0, 0.0, 0, False
        self.nr = 0.5  # EMA of the share of comments unknown to url-check (NEW)
        self.checks, self.cold_until = 0, 0.0  # url-check samples; no polling before this time


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
        self.videos: dict[str, Video] = {}
        self.ids: list[str] = []
        self.explore_fresh: deque[str] = deque()  # new uploads (RSS): explored first
        self.explore: deque[str] = deque()  # related videos; bounded, the oldest are dropped
        self.hot: set[Video] = set()
        self.yield_mb: dict[str, float | None] = {"hot": None, "explore": None}  # EMA of fresh comments per MB
        self.reload()

    def reload(self) -> None:
        yt = self.yt
        self.explore_share = float(yt.get("explore_share", 0.7))
        self.hot_min = float(yt.get("hot_min_gain", 1.0))
        self.max_hot = int(yt.get("max_hot_videos", 2000))
        self.max_videos = int(yt.get("max_videos", 20_000))
        self.explore_min = float(yt.get("explore_min", 0.2))
        self.cold_s = float(yt.get("cold_hours", 6)) * 3600
        self.nr_min = float(yt.get("min_new_ratio", 0.1))
        self.explore_fresh = deque(self.explore_fresh, maxlen=max(100, int(yt.get("explore_fresh_max", 2000))))
        self.explore = deque(self.explore, maxlen=max(100, int(yt.get("explore_max", 3000))))
        self.pacer.set_base(float(yt.get("hot_interval", 30)))

    def add(self, vid: str, fresh: bool = False) -> None:
        """fresh=True: a new upload from a seed channel (bypasses the cap, explored first)."""
        if vid in self.videos or len(self.videos) >= self.max_videos * (1.2 if fresh else 1):
            return
        self.videos[vid] = Video(vid)
        self.ids.append(vid)
        (self.explore_fresh if fresh else self.explore).append(vid)

    def record(self, kind: str, fresh: int, nbytes: int) -> None:
        """Cost-effectiveness of a poll: fresh comments per MB, EMA per kind (hot / explore+repoll)."""
        if nbytes <= 0:
            return
        k, y = ("hot" if kind == "hot" else "explore"), fresh / (nbytes / 1e6)
        old = self.yield_mb[k]
        self.yield_mb[k] = y if old is None else 0.8 * old + 0.2 * y

    def share(self) -> float:
        """Explore share: follows the relative yield per MB, between explore_min and explore_share."""
        e, h = self.yield_mb["explore"], self.yield_mb["hot"]
        if e is None or h is None or e + h <= 0:
            return self.explore_share
        return min(self.explore_share, max(self.explore_min, e / (e + h)))

    def pick(self) -> tuple[Video | None, str]:
        now, interval = time.monotonic(), self.pacer.current
        if random.random() >= self.share():
            best = None
            for v in self.hot:
                if v.cold_until <= now and now - v.last_poll >= interval and (best is None or v.ema * v.nr > best.ema * best.nr):
                    best = v
            if best:
                best.last_poll = now
                return best, "hot"
        while self.explore_fresh or self.explore:
            v = self.videos.get((self.explore_fresh or self.explore).popleft())
            if v is not None and not v.polls and not v.dead:
                v.last_poll = now
                return v, "explore"
        for _ in range(20):  # nothing to explore: re-poll a random known video
            if not self.ids:
                break
            v = self.videos.get(random.choice(self.ids))
            if v is not None and not v.dead and v.cold_until <= now and now - v.last_poll >= interval:
                v.last_poll = now
                return v, "repoll"
        return None, ""

    def update(self, v: Video, fresh: int) -> None:
        v.polls += 1
        if v.polls == 1 and fresh == 0:  # nothing from the last day: an old video, do not come back for a while
            self.chill(v)
            return
        # the first poll returns the backlog of the last day: it must not make a video hot, the re-poll is a trial
        v.ema = self.hot_min * 1.5 if v.polls == 1 else 0.5 * v.ema + 0.5 * fresh
        if v.ema >= self.hot_min and v.cold_until <= time.monotonic():
            self.hot.add(v)
            if len(self.hot) > self.max_hot:
                self.hot.discard(min(self.hot, key=lambda x: x.ema))
        else:
            self.hot.discard(v)
            if fresh == 0:  # a cold re-poll gave nothing: stop re-polling it for a while (1/6 of cold_hours)
                self.chill(v, 1 / 6)

    def feedback(self, vid: str, new: int, total: int) -> None:
        """url-check result for a video: new of total comments were unknown to the network."""
        v = self.videos.get(vid)
        if v is not None and total:
            v.nr = 0.7 * v.nr + 0.3 * (new / total)
            v.checks += 1
            if v.checks >= 3 and v.nr < self.nr_min:  # the network already knows (almost) all of its comments
                self.chill(v)

    def chill(self, v: Video, frac: float = 1.0) -> None:
        v.cold_until = time.monotonic() + self.cold_s * frac
        self.hot.discard(v)

    def gc(self) -> None:
        """Keep memory bounded: over the cap drop dead videos, then the coldest polled ones."""
        if len(self.videos) <= self.max_videos * 0.5:
            return
        queued = set(self.explore_fresh) | set(self.explore)
        drop = [v for v in self.videos.values() if v.dead or (not v.polls and v.vid not in queued)]  # dead, or evicted from the bounded queues and never polled
        if len(self.videos) - len(drop) > self.max_videos * 0.8:
            cold = sorted((v for v in self.videos.values() if v.polls and v not in self.hot and not v.dead), key=lambda x: x.last_poll)
            drop += cold[: len(cold) // 2]
        for v in drop:
            self.videos.pop(v.vid, None)
            self.hot.discard(v)
        self.ids = list(self.videos)
