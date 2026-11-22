import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from sender import Sender  # noqa: E402
from stats import Stats  # noqa: E402


def mk(yt):
    return Sender({"env": {"store_endpoint": "http://x"}, "youtube": yt}, Stats())


def test_need_and_reload():
    yt = {"target_rate": 50, "buffer_size": 3}
    s = mk(yt)
    assert s.need(0) and s.need(2) and not s.need(3)
    yt["target_rate"] = 0
    s.reload()
    assert s.need(10_000)  # unlimited: no throttling
    yt.update(target_rate=10, buffer_size=5)
    s.reload()
    assert s.rate == 10 and s.buffer == 5


def test_watch_reload(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("youtube:\n  target_rate: 5\n")
    os.environ["CONFIG_PATH"] = str(p)
    cfg = config._read()
    hits = []

    async def go():
        t = asyncio.create_task(config.watch(cfg, [lambda: hits.append(1)], 0.05))
        await asyncio.sleep(0.1)
        p.write_text("youtube:\n  target_rate: 9\n")
        os.utime(p, ns=(time.time_ns() + 10**9,) * 2)
        await asyncio.sleep(0.2)
        p.write_text("youtube: [broken\n")  # broken edit keeps the old values
        os.utime(p, ns=(time.time_ns() + 2 * 10**9,) * 2)
        await asyncio.sleep(0.2)
        t.cancel()

    asyncio.run(go())
    assert cfg["youtube"]["target_rate"] == 9 and hits == [1]
