"""Comment -> Exorde item. No data -> skip the comment (or the key); nothing is invented."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from parse import Comment, age_seconds

MIN_TEXT = 7


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_item(c: Comment, now: datetime, max_age_s: int, parent_mode: str = "self") -> tuple[dict | None, str]:
    """(item, "") or (None, skip reason). v1: top-level comments only."""
    if c.level != 0:
        return None, "reply"
    if not c.channel_id:
        return None, "no_channel"
    text = c.text.strip()
    if len(text) < MIN_TEXT:
        return None, "too_short"
    if not any(ch.isalnum() for ch in text):  # emoji / punctuation only
        return None, "no_text"
    if c.exact_ts is not None:
        created, age = c.exact_ts, (now - c.exact_ts).total_seconds()
        if age < 0:
            return None, "bad_age"
    else:
        sec = age_seconds(c.age_label)
        if sec is None:
            return None, "old"
        created, age = now - timedelta(seconds=sec), sec
    if age > max_age_s:
        return None, "old"
    summary: dict = {"video_id": c.video_id, "channel_id": c.channel_id, "kind": "comment"}
    if c.likes is not None:
        summary["likes"] = c.likes
    if c.reply_count is not None:
        summary["reply_count"] = c.reply_count
    if c.is_verified is not None:
        summary["is_verified"] = c.is_verified
    if c.is_creator is not None:
        summary["is_creator"] = c.is_creator
    item = {
        "content": text,
        "external_id": c.id,
        "external_parent_id": c.id if parent_mode == "self" else None,
        "created_at": iso(created),
        "collected_at": iso(now),
        "url": f"https://www.youtube.com/watch?v={c.video_id}&lc={c.id}",
        "title": "",
        "author": hashlib.sha1(c.channel_id.encode()).hexdigest(),
        "domain": "youtube.com",
        "summary": json.dumps(summary, ensure_ascii=False),
    }
    if c.username:
        item["username"] = c.username
    return item, ""
