"""Probe (TZ p.0): look for absolute timestamps and author channelId in youtubei/v1/next comment responses.

Usage: PROXY=pool python3 (or PROXY=socks5h://...) tools/yt_time_probe.py [video_id ...]
Raw responses go to docs/probe/ (git-ignored); the report goes to docs/yt_time_probe_report.md.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from client import new_session, post_next  # noqa: E402
from parse import NEWEST_RE, first_token  # noqa: E402

EPOCH = re.compile(r"^\d{10}(\d{3}|\d{6})?$")
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")


def walk(node, path=""):
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, f"{path}.{k}")
    elif isinstance(node, list):
        for v in node:
            yield from walk(v, f"{path}[]")
    else:
        yield path, node


async def main() -> None:
    videos = sys.argv[1:] or ["dQw4w9WgXcQ", "9bZkp7q19f0", "kJQP7kiw5Fk"]
    raw_dir = Path("docs/probe")
    raw_dir.mkdir(parents=True, exist_ok=True)
    abs_hits: Counter[str] = Counter()
    keys: Counter[str] = Counter()
    ents = with_channel = 0
    proxy = os.getenv("PROXY") or None
    if proxy == "pool":  # start the VPN nodes from .env/config.yaml and use one of them
        import config
        from nodes import NodePool
        from stats import Stats
        pool = NodePool(config.load(), Stats())
        if not await pool.refresh():
            raise SystemExit("no working VPN node")
        proxy = pool.pick()
    async with new_session(proxy) as s:
        for vid in videos:
            try:
                text, _ = await post_next(s, {"videoId": vid})
                tok = first_token(text)
                text, _ = await post_next(s, {"continuation": tok})
                m = NEWEST_RE.search(text)
                text, _ = await post_next(s, {"continuation": m.group(1) if m else tok})
            except Exception as exc:  # noqa: BLE001
                print(vid, "failed:", exc)
                continue
            (raw_dir / f"{vid}.json").write_text(text, encoding="utf-8")
            data = json.loads(text)
            for path, val in walk(data):
                if isinstance(val, (str, int)) and (ISO.match(str(val)) or (EPOCH.match(str(val)) and re.search(r"time|date|ts|publish|epoch", path, re.I))):
                    abs_hits[re.sub(r"\[\]", "[]", path)] += 1
            for mut in data.get("frameworkUpdates", {}).get("entityBatchUpdate", {}).get("mutations", []):
                p = (mut.get("payload") or {}).get("commentEntityPayload")
                if p:
                    ents += 1
                    with_channel += bool((p.get("author") or {}).get("channelId"))
                    for path, _ in walk(p):
                        keys[path] += 1
    lines = ["# YouTube time/author probe", "", f"videos: {', '.join(videos)}", f"commentEntityPayload: {ents}, with author.channelId: {with_channel}", "",
             "## Absolute time candidates (path: count)"] + ([f"- `{k}`: {v}" for k, v in abs_hits.most_common()] or ["- none found"]) + ["", "## commentEntityPayload paths"] + [f"- `{k}`: {v}" for k, v in sorted(keys.items())]
    Path("docs/yt_time_probe_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines[:12]))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        pass
