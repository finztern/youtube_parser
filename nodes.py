"""Free vless nodes: download lists, test with Xray, keep nodes that answer YouTube youtubei/v1/next.

One Xray process serves one generation of nodes (local SOCKS inbound per node). A refresh starts the next generation on
another port range, swaps the list, and stops the old process after a grace period, so polling never stops.
Links stay in memory and are never logged.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import re
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import httpx

from client import YtError, new_session, post_next

log = logging.getLogger("nodes")
LOCAL_PREFIX = "socks5h://127.0.0.1:"
XRAY_LOG = os.path.join(tempfile.gettempdir(), "xray_vpn.log")
PORT_BASES = (20000, 30000)


def parse_vless(link: str) -> dict:
    u = urlsplit(link.strip())
    if u.scheme != "vless" or not (u.username and u.hostname and u.port):
        raise ValueError("not a vless link")
    q = {k: unquote(v[0]) for k, v in parse_qs(u.query).items()}
    node = {
        "id": unquote(u.username), "host": u.hostname, "port": u.port, "net": q.get("type", "tcp"),
        "sec": q.get("security", "none"), "flow": q.get("flow", ""), "sni": q.get("sni", ""), "host_header": q.get("host", ""),
        "path": q.get("path", "/"), "fp": q.get("fp") or "chrome", "alpn": q.get("alpn", ""), "pbk": q.get("pbk", ""),
        "sid": q.get("sid", ""), "spx": q.get("spx", ""), "service": q.get("serviceName", ""),
    }
    if q.get("encryption", "none") != "none" or node["net"] not in ("tcp", "ws", "grpc", "httpupgrade"):
        raise ValueError("unsupported vless variant")
    if node["sec"] not in ("none", "tls", "reality") or (node["sec"] == "reality" and not node["pbk"]):
        raise ValueError("unsupported security")
    if node["net"] == "tcp" and q.get("headerType", "none") != "none":
        raise ValueError("unsupported tcp header")
    return node


def _outbound(n: dict, tag: str) -> dict:
    user = {"id": n["id"], "encryption": "none"}
    if n["flow"]:
        user["flow"] = n["flow"]
    stream: dict = {"network": n["net"], "security": n["sec"]}
    if n["sec"] == "tls":
        tls = {"serverName": n["sni"] or n["host_header"] or n["host"], "fingerprint": n["fp"], "allowInsecure": False}
        if n["alpn"]:
            tls["alpn"] = n["alpn"].split(",")
        stream["tlsSettings"] = tls
    elif n["sec"] == "reality":
        stream["realitySettings"] = {"serverName": n["sni"], "fingerprint": n["fp"], "publicKey": n["pbk"], "shortId": n["sid"], "spiderX": n["spx"]}
    if n["net"] == "ws":
        stream["wsSettings"] = {"path": n["path"], "headers": {"Host": n["host_header"]} if n["host_header"] else {}}
    elif n["net"] == "grpc":
        stream["grpcSettings"] = {"serviceName": n["service"]}
    elif n["net"] == "httpupgrade":
        stream["httpupgradeSettings"] = {"path": n["path"], "host": n["host_header"]}
    return {"tag": tag, "protocol": "vless", "settings": {"vnext": [{"address": n["host"], "port": n["port"], "users": [user]}]}, "streamSettings": stream}


def build_xray_config(nodes: list[dict], base_port: int) -> dict:
    inbounds, outbounds, rules = [], [], []
    for i, n in enumerate(nodes):
        inbounds.append({"tag": f"in-{i}", "listen": "127.0.0.1", "port": base_port + i, "protocol": "socks", "settings": {"auth": "noauth", "udp": False}})
        outbounds.append(_outbound(n, f"out-{i}"))
        rules.append({"type": "field", "inboundTag": [f"in-{i}"], "outboundTag": f"out-{i}"})
    return {"log": {"loglevel": "warning"}, "inbounds": inbounds, "outbounds": outbounds, "routing": {"domainStrategy": "AsIs", "rules": rules}}


def _decode_list(text: str) -> list[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(l.startswith(("vless://", "vmess://", "trojan://", "ss://")) for l in lines):
        return lines
    try:  # base64 subscription
        body = text.strip()
        raw = base64.b64decode(body + "=" * (-len(body) % 4)).decode("utf-8", "ignore")
        return [l.strip() for l in raw.splitlines() if l.strip()]
    except Exception:  # noqa: BLE001
        return []


async def _download(urls: list[str]) -> list[str]:
    links: list[str] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for url in urls:
            try:
                r = await client.get(url)
                r.raise_for_status()
                links += [l for l in _decode_list(r.text) if l.startswith("vless://")]
            except Exception as exc:  # noqa: BLE001 - one dead source must not stop the others
                log.warning("vpn source %s failed: %s", url[:70], str(exc)[:80])
    return list(dict.fromkeys(links))


async def _xray_accepts(xray: str, node: dict, tmp: Path, base_port: int, sem: asyncio.Semaphore, index: int) -> bool:
    async with sem:
        try:
            path = tmp / f"t{index}.json"
            path.write_text(json.dumps(build_xray_config([node], base_port)), encoding="utf-8")
            proc = await asyncio.create_subprocess_exec(xray, "run", "-test", "-c", str(path), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            return await asyncio.wait_for(proc.wait(), timeout=15) == 0
        except Exception:  # noqa: BLE001 - unsupported node
            return False


async def _probe(port: int, video: str, max_latency: float, sem: asyncio.Semaphore) -> float | None:
    """Latency if the node answers youtubei/v1/next for a real video, else None."""
    async with sem:
        started = time.monotonic()
        try:
            async with new_session(f"{LOCAL_PREFIX}{port}", timeout=max_latency * 2.5) as session:
                text, _ = await post_next(session, {"videoId": video})
            latency = time.monotonic() - started
            return latency if '"responseContext"' in text and latency <= max_latency else None
        except (YtError, Exception):  # noqa: BLE001 - dead or blocked node
            return None


def _stop(proc: asyncio.subprocess.Process | None) -> None:
    if proc is not None and proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass


async def harvest(cfg: dict, base_port: int) -> tuple[list[str], asyncio.subprocess.Process | None]:
    """(working local proxies, the Xray process serving them). Empty list when nothing worked."""
    v, xray = cfg["vpn"], cfg["env"]["xray_bin"]
    own = [u for u in re.split(r"[\s,]+", os.getenv("VPN_SOURCES_EXTRA", "")) if u.startswith("http")]  # secret: .env only
    only_own = os.getenv("VPN_ONLY", "").lower() in ("1", "true", "yes") and bool(own)
    sources = [] if only_own else [s for s in (v.get("sources") or []) if isinstance(s, str) and s.strip()]
    if not (sources or own) or not xray:
        log.error("vpn: no sources or no xray binary")
        return [], None
    own_links = await _download(own) if own else []
    free_links = [l for l in (await _download(sources) if sources else []) if l not in set(own_links)]
    random.shuffle(free_links)
    links = own_links + free_links
    candidates, is_own = [], []
    for link in own_links + free_links[: int(v.get("candidates", 800))]:
        try:
            candidates.append(parse_vless(link))
            is_own.append(link in own_links)
        except ValueError:
            continue
    log.info("vpn: %d links (%d own), %d parsed as supported vless", len(links), len(own_links), len(candidates))
    with tempfile.TemporaryDirectory() as tmp:
        sem = asyncio.Semaphore(8)
        ok = await asyncio.gather(*(_xray_accepts(xray, n, Path(tmp), base_port, sem, i) for i, n in enumerate(candidates)))
        acc = [(n, o) for n, good, o in zip(candidates, ok, is_own) if good]
        n_own = sum(o for _, o in acc)
        acc = [x for x in acc if x[1]] + [x for x in acc if not x[1]][: max(0, int(v.get("nodes", 250)) - n_own)]
        nodes, own_flags = [n for n, _ in acc], [o for _, o in acc]
        if not nodes:
            log.warning("vpn: %d links, no node accepted by xray", len(links))
            return [], None
        path = Path(tmp) / "xray.json"
        path.write_text(json.dumps(build_xray_config(nodes, base_port)), encoding="utf-8")
        log_file = open(XRAY_LOG, "wb")  # noqa: SIM115 - handed to the child process
        proc = await asyncio.create_subprocess_exec(xray, "run", "-c", str(path), stdout=log_file, stderr=log_file)
        log_file.close()
        await asyncio.sleep(3)  # xray has read the config by now
        if proc.returncode is not None:
            log.warning("vpn: xray exited with %s", proc.returncode)
            return [], None
    max_latency = float(v.get("max_latency_seconds", 8.0))
    video = cfg["youtube"].get("probe_video", "dQw4w9WgXcQ")
    sem = asyncio.Semaphore(int(v.get("probe_concurrency", 30)))
    lat = await asyncio.gather(*(_probe(base_port + i, video, max_latency, sem) for i in range(len(nodes))))
    ok_all = [(l, base_port + i, own_flags[i]) for i, l in enumerate(lat) if l is not None]
    mine = sorted((l, p) for l, p, o in ok_all if o)
    free = sorted((l, p) for l, p, o in ok_all if not o)
    good = sorted(mine + free[: max(0, int(v.get("keep", 150)) - len(mine))])  # own nodes are always kept
    if not good:
        log.warning("vpn: %d nodes started, none answered YouTube", len(nodes))
        _stop(proc)
        return [], None
    log.info("vpn: %d links, %d accepted by xray, %d answer YouTube (<=%.0fs), median %.1fs", len(links), len(nodes), len(good), max_latency, statistics.median(l for l, _ in good))
    return [f"{LOCAL_PREFIX}{p}" for _, p in good], proc


class NodePool:
    def __init__(self, cfg: dict, stats) -> None:
        self.cfg, self.stats = cfg, stats
        self.proxies: list[str] = []
        self._set: set[str] = set()
        self.banned: dict[str, float] = {}
        self.fails: Counter[str] = Counter()
        self.proc: asyncio.subprocess.Process | None = None
        self.quarantine = float(cfg["vpn"].get("quarantine_seconds", 120))
        self._gen = 0

    async def refresh(self) -> bool:
        proxies, proc = await harvest(self.cfg, PORT_BASES[self._gen % 2])
        if not proxies:
            return False
        old, self.proc = self.proc, proc
        self.proxies, self._set = proxies, set(proxies)
        self.banned.clear()
        self.fails.clear()
        self._gen += 1
        if old is not None:
            asyncio.get_running_loop().call_later(120, _stop, old)  # grace: workers move to the new list
        return True

    async def run(self) -> None:
        refresh_s = float(self.cfg["vpn"].get("refresh_minutes", 60)) * 60
        next_at = 0.0
        while True:
            dead = self.proc is not None and self.proc.returncode is not None
            if dead or time.monotonic() >= next_at:
                if dead:
                    log.warning("xray died (%s): %s", self.proc.returncode, self.tail())
                ok = await self.refresh()
                next_at = time.monotonic() + (refresh_s if ok else 60)
            await asyncio.sleep(10)

    def usable(self, proxy: str | None) -> bool:
        return proxy in self._set and self.banned.get(proxy, 0) <= time.monotonic()

    def pick(self) -> str | None:
        now = time.monotonic()
        live = [p for p in self.proxies if self.banned.get(p, 0) <= now]
        return random.choice(live) if live else None

    def report(self, proxy: str, ok: bool, status: int = 0) -> None:
        """Quarantine a node: at once on 429, after 2 consecutive failures otherwise."""
        if ok:
            self.fails[proxy] = 0
            return
        self.fails[proxy] += 1
        if status == 429 or self.fails[proxy] >= 2:
            self.banned[proxy] = time.monotonic() + self.quarantine * (2 if status == 429 else 1)
            self.fails[proxy] = 0
            self.stats.inc("node_quarantine")

    def live_count(self) -> int:
        now = time.monotonic()
        return sum(1 for p in self.proxies if self.banned.get(p, 0) <= now)

    @staticmethod
    def tail(lines: int = 6) -> str:
        try:
            return " | ".join(Path(XRAY_LOG).read_text(errors="ignore").splitlines()[-lines:])[:600]
        except OSError:
            return ""

    def close(self) -> None:
        _stop(self.proc)
