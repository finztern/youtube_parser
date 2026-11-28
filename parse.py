"""Parsing of youtubei/v1/next responses: tokens, related videos, comment entities."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone

VID_RE = re.compile(r'"videoId":"([A-Za-z0-9_-]{11})"')
TOKEN_RE = re.compile(r'"token":"([^"]+)"')
NEXT_RE = re.compile(r'"continuationCommand":\{"token":"([^"]+)"')
NEWEST_RE = re.compile(r'"title":"Newest(?: first)?".{0,1500}?"continuationCommand":\{"token":"([^"]+)"', re.S)
AGE_RE = re.compile(r"(\d+)\s+(second|minute|hour|day|week|month|year)")
UNIT_SECONDS = {"second": 1, "minute": 60, "hour": 3600}
UNIT_ALL = {**UNIT_SECONDS, "day": 86400, "week": 604800, "month": 2_592_000, "year": 31_536_000}
VIDEO_AGE_RE = re.compile(r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago")
CONTENT_RE = re.compile(r'"contentId":"([A-Za-z0-9_-]{11})"')


@dataclass(slots=True)
class Comment:
    id: str
    video_id: str
    text: str
    age_label: str
    channel_id: str | None
    username: str | None
    likes: int | None
    reply_count: int | None
    level: int
    is_verified: bool | None
    is_creator: bool | None
    exact_ts: datetime | None


def first_token(text: str) -> str | None:
    idx = text.find('"sectionIdentifier":"comment-item-section"')
    if idx < 0:
        return None
    tokens = TOKEN_RE.findall(text[max(0, idx - 4000):idx])
    return tokens[-1] if tokens else None


def age_seconds(label: str) -> int | None:
    """Relative age in seconds; only second/minute/hour units (older ages are not accepted)."""
    low = label.lower()
    if "just now" in low:
        return 0
    m = AGE_RE.search(low)
    if not m or m.group(2) not in UNIT_SECONDS:
        return None
    return int(m.group(1)) * UNIT_SECONDS[m.group(2)]


def _count(value) -> int | None:
    s = str(value or "").replace(",", "").strip()
    return int(s) if s.isdigit() else None


def _exact_ts(props: dict, keys: list[str]) -> datetime | None:
    for key in keys:
        raw = props.get(key)
        if not raw or not str(raw).isdigit():
            continue
        n = int(raw)
        for div in (1, 1_000, 1_000_000):  # seconds, ms, us
            try:
                ts = datetime.fromtimestamp(n / div, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                continue
            if 2015 <= ts.year <= 2100:
                return ts
    return None


def parse_comments(text: str, video_id: str, exact_keys: list[str] | None = None) -> list[Comment] | None:
    """Comment entities of a response (None if it is not JSON). Entities without id/text/age are skipped."""
    try:
        data = json.loads(text)
    except ValueError:
        return None
    try:
        mutations = data["frameworkUpdates"]["entityBatchUpdate"]["mutations"]
    except (KeyError, TypeError):
        return []
    out: dict[str, Comment] = {}
    for mut in mutations or []:
        try:
            p = mut["payload"]["commentEntityPayload"]
            props = p["properties"]
            cid = props["commentId"]
            body = (props.get("content") or {}).get("content") or ""
            age = props.get("publishedTime") or ""
        except (KeyError, TypeError):
            continue
        if not (cid and body and age) or cid in out:
            continue
        author = p.get("author") or {}
        toolbar = p.get("toolbar") or {}
        level = props.get("replyLevel")
        out[cid] = Comment(
            id=cid, video_id=video_id, text=body, age_label=age,
            channel_id=author.get("channelId") or None,
            username=author.get("displayName") or None,
            likes=_count(toolbar.get("likeCountNotliked")),
            reply_count=_count(toolbar.get("replyCount")),
            level=level if isinstance(level, int) else (1 if "." in cid else 0),
            is_verified=author.get("isVerified") if isinstance(author.get("isVerified"), bool) else None,
            is_creator=author.get("isCreator") if isinstance(author.get("isCreator"), bool) else None,
            exact_ts=_exact_ts(props, exact_keys or []),
        )
    return list(out.values())


def related_videos(text: str) -> dict[str, int]:
    """{video id: age in seconds} for related videos whose renderer carries an 'N units ago' label.

    Works for compactVideoRenderer (videoId) and lockupViewModel (contentId): a node that names exactly one video id
    and holds an age label. Videos without a label (live, shorts, mixes) are not returned.
    """
    try:
        data = json.loads(text)
    except ValueError:
        return {}
    out: dict[str, int] = {}

    def walk(n) -> None:
        if isinstance(n, dict):
            vid = n.get("videoId") or n.get("contentId")
            if isinstance(vid, str) and len(vid) == 11 and vid not in out:
                blob = json.dumps(n, ensure_ascii=False, separators=(",", ":"))
                m = VIDEO_AGE_RE.search(blob)
                if m and set(VID_RE.findall(blob)) | set(CONTENT_RE.findall(blob)) == {vid}:
                    out[vid] = int(m.group(1)) * UNIT_ALL[m.group(2)]
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)

    walk(data)
    return out


def parse_page(text: str, video_id: str, exact_keys: list[str] | None = None) -> tuple[list[Comment] | None, list[str]]:
    """(comments, continuation tokens) of one comments page; runs in a worker process."""
    return parse_comments(text, video_id, exact_keys), NEXT_RE.findall(text)


def parse_watch(text: str) -> tuple[dict[str, int], str | None]:
    """(related videos {id: age s}, comments token) of a watch response; runs in a worker process."""
    return related_videos(text), first_token(text)


def low_priority() -> None:
    """Worker-process initializer: parsing yields the CPU to the event loop (HTTP/queues)."""
    try:
        os.nice(10)
    except OSError:
        pass
