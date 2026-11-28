"""SYNTHETIC related-video shapes (compactVideoRenderer and lockupViewModel)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from parse import related_videos  # noqa: E402


def test_both_shapes():
    data = {"contents": {"secondaryResults": [
        {"compactVideoRenderer": {"videoId": "aaaaaaaaaaa", "publishedTimeText": {"simpleText": "6 hours ago"},
                                  "navigationEndpoint": {"watchEndpoint": {"videoId": "aaaaaaaaaaa"}}}},
        {"lockupViewModel": {"contentId": "bbbbbbbbbbb", "metadata": {"parts": [{"text": {"content": "1.2M views"}},
                                                                                   {"text": {"content": "17 years ago"}}]}}},
        {"compactVideoRenderer": {"videoId": "ccccccccccc", "viewCountText": {"simpleText": "5 watching"}}},
        {"lockupViewModel": {"contentId": "ddddddddddd", "metadata": {"parts": [{"text": {"content": "59 minutes ago"}}]}}},
    ]}}
    assert related_videos(json.dumps(data)) == {"aaaaaaaaaaa": 6 * 3600, "bbbbbbbbbbb": 17 * 31_536_000, "ddddddddddd": 59 * 60}
    assert related_videos("<html>") == {}


def test_parse_in_worker_process():
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    from parse import parse_page, parse_watch
    text = json.dumps({"frameworkUpdates": {"entityBatchUpdate": {"mutations": [{"payload": {"commentEntityPayload": {
        "properties": {"commentId": "Ugx1", "content": {"content": "hello world"}, "publishedTime": "1 minute ago", "replyLevel": 0},
        "author": {"channelId": "UCabc"}, "toolbar": {}}}}]}}, "x": {"continuationCommand": {"token": "T1"}}}, separators=(",", ":"))
    with ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn")) as ex:
        comments, nxt = ex.submit(parse_page, text, "vid12345678").result(timeout=30)
        rel, tok = ex.submit(parse_watch, "<html>").result(timeout=30)
    assert [c.id for c in comments] == ["Ugx1"] and nxt == ["T1"] and rel == {} and tok is None
