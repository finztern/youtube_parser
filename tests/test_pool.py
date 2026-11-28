import asyncio
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import dedup  # noqa: E402
import nodes  # noqa: E402
from stats import Stats  # noqa: E402


def mkpool():
    pool = nodes.NodePool({"vpn": {}, "youtube": {}, "env": {}}, Stats())
    pool._set_nodes(["a", "b", "c"], {"a": 1.0, "b": 3.0, "c": 2.0})
    return pool


def test_round_robin_and_fastest():
    pool = mkpool()
    p = [pool.pick() for _ in range(6)]
    assert sorted(p[:3]) == ["a", "b", "c"] and p[:3] == p[3:]  # no reuse before a full cycle
    assert pool.fastest(2) == ["a", "c"]
    pool.banned["a"] = 1e12
    assert "a" not in [pool.pick() for _ in range(4)] and pool.fastest(2) == ["a", "c"]


class FakeSession:
    calls: list = []

    def __init__(self, **kw):
        self.proxy = kw["proxies"]["https"]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        pass

    async def post(self, url, json=None):
        FakeSession.calls.append(self.proxy)
        if self.proxy == "a":
            raise RuntimeError("api down")
        return types.SimpleNamespace(status_code=200, json=lambda: ["u1"])


def test_url_check_failover(monkeypatch):
    monkeypatch.setattr(dedup, "AsyncSession", FakeSession)
    cfg = {"youtube": {"url_check_attempts": 2}, "env": {"url_check_endpoint": "http://x", "url_check_proxy": "pool"}}
    p = dedup.Pipeline(cfg, Stats(), None, mkpool())
    p._k = -1  # first rotation starts at the fastest node
    assert asyncio.run(p._check(["u1"])) == {"u1"} and FakeSession.calls == ["a", "c"]


def test_age_bucket():
    mk = lambda c: {"created_at": c, "collected_at": "2026-01-01T12:00:00Z"}  # noqa: E731
    assert [dedup.age_bucket(mk(c)) for c in ("2026-01-01T11:58:00Z", "2026-01-01T11:00:00Z", "2026-01-01T07:00:00Z", "2025-12-31T20:00:00Z")] == ["5m", "2h", "6h", "23h"]
