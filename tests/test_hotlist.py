import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from hotlist import HotList, Pacer  # noqa: E402


def test_fresh_first_and_bounded():
    h = HotList({"explore_max": 100, "max_videos": 150}, Pacer(30))
    for i in range(300):
        h.add(f"rel{i:08d}")
    assert len(h.explore) == 100 and len(h.videos) == 150  # queue bounded, cap respected
    h.add("new00000001", fresh=True)  # bypasses the cap
    assert h.pick()[0].vid == "new00000001"


def test_share_follows_yield():
    h = HotList({"explore_share": 0.7, "explore_min": 0.2}, Pacer(30))
    assert h.share() == 0.7
    h.record("explore", 1, 1_000_000)
    h.record("hot", 20, 1_000_000)
    assert h.share() == 0.2  # hot is far cheaper per MB
    h.yield_mb.update(explore=50.0, hot=1.0)
    assert h.share() == 0.7


def test_chill_known_and_empty():
    h = HotList({}, Pacer(30))
    h.add("a" * 11)
    h.add("b" * 11)
    a, b = h.videos["a" * 11], h.videos["b" * 11]
    h.update(a, 0)  # first poll without fresh comments
    assert a.cold_until > 0 and a not in h.hot
    h.update(b, 5)
    h.update(b, 5)
    assert b in h.hot
    for _ in range(12):
        h.feedback(b.vid, 0, 10)
    assert b not in h.hot and b.cold_until > 0
    h.update(b, 5)
    assert b not in h.hot


def test_cold_repoll_without_fresh_is_chilled():
    h = HotList({}, Pacer(30))
    h.add("c" * 11)
    c = h.videos["c" * 11]
    h.update(c, 5)
    h.update(c, 0)
    h.update(c, 0)
    assert c.cold_until > 0 and c not in h.hot


def test_gc_drops_evicted_unpolled():
    h = HotList({"explore_max": 100, "max_videos": 200}, Pacer(30))
    for i in range(190):
        h.add(f"rel{i:08d}")  # 90 of them fall out of the bounded queue
    h.gc()
    assert len(h.videos) == 100 and all(v in h.explore for v in h.videos)
