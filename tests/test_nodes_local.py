import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import nodes  # noqa: E402
from stats import Stats  # noqa: E402


def test_refresh_local(monkeypatch):
    cfg = {"vpn": {}, "youtube": {}, "env": {"nodes_endpoint": "http://x/nodes", "nodes_host": "h", "nodes_scheme": "socks5h"}}
    alive, ok = [], set()

    async def fake_fetch(_):
        return alive

    async def fake_probe(proxy, *_):
        return 0.5 if proxy in ok else None

    monkeypatch.setattr(nodes, "fetch_alive", fake_fetch)
    monkeypatch.setattr(nodes, "_probe", fake_probe)
    pool = nodes.NodePool(cfg, Stats())
    alive[:] = [1, 2, 3]
    ok.update({"socks5h://h:1", "socks5h://h:2"})
    assert asyncio.run(pool.refresh()) and pool.proxies == ["socks5h://h:1", "socks5h://h:2"]
    pool.banned["socks5h://h:1"] = 1e12
    alive[:] = [1, 3, 4]  # 2 died, 3 came back, 4 does not answer YouTube
    ok.update({"socks5h://h:3"})
    assert asyncio.run(pool.refresh()) and pool.proxies == ["socks5h://h:1", "socks5h://h:3"]
    assert "socks5h://h:1" in pool.banned  # state of kept nodes survives a refresh

    async def down(_):
        return None

    monkeypatch.setattr(nodes, "fetch_alive", down)
    assert asyncio.run(pool.refresh()) is False and len(pool.proxies) == 2  # service down: list kept
