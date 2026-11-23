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
