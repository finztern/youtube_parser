"""Parsing of youtubei/v1/next responses: tokens, related videos, comment entities."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone

VID_RE = re.compile(r'"videoId":"([A-Za-z0-9_-]{11})"')
TOKEN_RE = re.compile(r'"token":"([^"]+)"')
NEXT_RE = re.compile(r'"continuationCommand":\{"token":"([^"]+)"')
NEWEST_RE = re.compile(r'"title":"Newest(?: first)?".{0,1500}?"continuationCommand":\{"token":"([^"]+)"', re.S)
AGE_RE = re.compile(r"(\d+)\s+(second|minute|hour|day|week|month|year)")
UNIT_SECONDS = {"second": 1, "minute": 60, "hour": 3600}


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
