import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashboard import Sampler  # noqa: E402
from hotlist import HotList, Pacer  # noqa: E402
from stats import Stats  # noqa: E402


class _Pool:
    proxies = ["a", "b"]

    def live_count(self):
        return 1


class _Obj:
    def __init__(self):
        self.q, self.seen = asyncio.Queue(), {}


def test_sample_and_state():
    st, hot = Stats(), HotList({}, Pacer(30))
    sm = Sampler(st, _Pool(), hot, _Obj(), _Obj())
    st.inc("new", 4)
    st.inc("skip_old", 2)
    p = sm.sample()
    d = sm.state()
    assert p["new"] > 0 and d["gauges"]["nodes"] == "1/2" and d["filtered"] == {"skip_old": 2}
    assert d["totals"]["new"] == 4 and len(d["points"]) == 1


def test_metrics():
    st, hot = Stats(), HotList({}, Pacer(30))
    sm = Sampler(st, _Pool(), hot, _Obj(), _Obj())
    st.inc("new", 4)
    st.inc("skip_old", 2)
    m = sm.metrics()
    assert "ytp_new_total 4" in m and "ytp_skip_old_total 2" in m and "ytp_blocked_total 0" in m
    assert "ytp_nodes_live 1" in m and "ytp_nodes_total 2" in m and m.endswith("\n")
