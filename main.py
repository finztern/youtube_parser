"""YouTube parser for Exorde: entry point."""
from __future__ import annotations

import asyncio
import logging
import signal

import config
from dashboard import Sampler, serve as serve_dashboard
from dedup import Pipeline
from explorer import Explorer
from hotlist import HotList, Pacer
from nodes import NodePool
from sender import Sender
from stats import Stats

log = logging.getLogger("main")


async def amain() -> None:
    cfg = config.load()
    logging.basicConfig(level=cfg["env"]["log_level"], format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for noisy in ("httpx", "httpcore"):  # httpx logs full request URLs: the subscription URL carries a token
        logging.getLogger(noisy).setLevel(logging.WARNING)
    yt = cfg["youtube"]
    stats = Stats()
    pool = NodePool(cfg, stats)
    hot = HotList(yt, Pacer(float(yt.get("hot_interval", 30))))
    sender = Sender(cfg, stats)
    pipeline = Pipeline(cfg, stats, sender, pool, hot)
    explorer = Explorer(cfg, pool, hot, pipeline, stats)

    coros = [config.watch(cfg, [sender.reload, pipeline.reload, explorer.reload, hot.reload]), pool.run(), sender.run(), pipeline.run(), stats.run(pool, hot, pipeline), explorer.housekeeping()]
    if cfg["env"]["dash_port"]:
        sampler = Sampler(stats, pool, hot, pipeline, sender)
        coros += [sampler.run(), serve_dashboard(sampler, cfg["env"]["dash_host"], cfg["env"]["dash_port"])]
    tasks = [asyncio.create_task(c) for c in coros]
    while not pool.proxies:  # wait for the first working generation of nodes
        await asyncio.sleep(2)
    await explorer.seed()
    tasks += [asyncio.create_task(explorer.worker()) for _ in range(int(yt.get("workers", 100)))]

    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    done_waiter = asyncio.create_task(stop.wait())
    await asyncio.wait([done_waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
    for t in tasks:
        t.cancel()
    pool.close()
    if explorer.procs is not None:
        explorer.procs.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    asyncio.run(amain())
