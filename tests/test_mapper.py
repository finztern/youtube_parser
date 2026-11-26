"""Tests on SYNTHETIC responses shaped like commentEntityPayload (replace with saved real ones after the probe)."""
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from mapper import build_item  # noqa: E402
from nodes import build_singbox_config, parse_hy2, parse_vless  # noqa: E402
from parse import age_seconds, parse_comments  # noqa: E402

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def entity(cid, text, age, channel="UCabc", level=0):
    return {"payload": {"commentEntityPayload": {
        "properties": {"commentId": cid, "content": {"content": text}, "publishedTime": age, "replyLevel": level},
        "author": {"channelId": channel, "displayName": "@user"} if channel else {"displayName": "@user"},
        "toolbar": {"likeCountNotliked": "3", "replyCount": ""}}}}


def resp(*ents):
    return json.dumps({"frameworkUpdates": {"entityBatchUpdate": {"mutations": list(ents)}}})


def test_item_fields():
    c = parse_comments(resp(entity("Ugx1", "hello world", "5 minutes ago")), "vid12345678")[0]
    item, why = build_item(c, NOW, 23 * 3600)
    assert why == "" and item["created_at"] == "2026-01-01T11:55:00Z" and item["collected_at"].endswith("Z")
    assert item["author"] == hashlib.sha1(b"UCabc").hexdigest() and len(item["author"]) == 40
    assert item["url"] == "https://www.youtube.com/watch?v=vid12345678&lc=Ugx1" and item["external_parent_id"] == "Ugx1"
    s = json.loads(item["summary"])
    assert isinstance(item["summary"], str) and s["likes"] == 3 and "reply_count" not in s and s["kind"] == "comment"


def test_skips():
    cs = parse_comments(resp(entity("a", "short", "1 minute ago"), entity("b", "long enough", "2 days ago"), entity("c", "long enough", "1 hour ago", channel=None),
                              entity("d", "😀😀😀😀😀😀😀😀", "1 hour ago"), entity("e.f", "reply text ok", "1 hour ago", level=1)), "v")
    assert [build_item(c, NOW, 82800)[1] for c in cs] == ["too_short", "old", "no_channel", "no_text", "reply"]


def test_age_and_null_parent():
    assert age_seconds("3 hours ago") == 10800 and age_seconds("1 day ago") is None
    c = parse_comments(resp(entity("x", "some text here", "10 seconds ago")), "v")[0]
    assert build_item(c, NOW, 82800, "null")[0]["external_parent_id"] is None


def test_not_json():
    assert parse_comments("<html>", "v") is None and parse_comments("{}", "v") == []


def test_singbox_config():
    n = parse_vless("vless://11111111-1111-1111-1111-111111111111@example.com:443?type=ws&security=tls&sni=example.com&path=%2Fws#x")
    h = parse_hy2("hysteria2://pw%40x@example.org:8443,20000-30000/?sni=a.com&insecure=1&obfs=salamander&obfs-password=op#n")
    cfg = build_singbox_config([n, h], 20000)
    assert cfg["inbounds"][1]["listen_port"] == 20001 and cfg["outbounds"][0]["transport"]["path"] == "/ws"
    o = cfg["outbounds"][1]
    assert o["type"] == "hysteria2" and o["server_port"] == 8443 and o["password"] == "pw@x" and o["obfs"]["password"] == "op" and o["tls"]["insecure"]
