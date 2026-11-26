"""Subscription nodes (vless:// + hysteria2://): download, validate with sing-box, keep nodes that reach YouTube.

One sing-box process serves one generation of nodes (local SOCKS inbound per node). A refresh starts the next generation
on another port range, swaps the list, and stops the old process after a grace period, so polling never stops.
Nodes are handed out round-robin (every poll gets the least recently used node); the fastest ones serve url-check.
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
from collections import Counter, deque
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import httpx

from client import HEADERS, new_session

log = logging.getLogger("nodes")
LOCAL_PREFIX = "socks5h://127.0.0.1:"
SB_LOG = os.path.join(tempfile.gettempdir(), "singbox_vpn.log")
PROBE_URL = "https://www.youtube.com/generate_204"
SCHEMES = ("vless://", "hysteria2://", "hy2://")
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
        "sid": q.get("sid", ""), "spx": q.get("spx", ""), "service": q.get("serviceName", ""), "proto": "vless",
        "insecure": q.get("allowInsecure", q.get("insecure", "0")) in ("1", "true"),
    }
    if q.get("encryption", "none") != "none" or node["net"] not in ("tcp", "ws", "grpc", "httpupgrade"):
        raise ValueError("unsupported vless variant")
    if node["sec"] not in ("none", "tls", "reality") or (node["sec"] == "reality" and not node["pbk"]):
        raise ValueError("unsupported security")
    if node["net"] == "tcp" and q.get("headerType", "none") != "none":
        raise ValueError("unsupported tcp header")
    return node


def _hostport(u) -> tuple[str, int]:
    """Host and first port of a link; accepts hysteria2 port hopping like host:443,20000-30000."""
    hp = u.netloc.rpartition("@")[2]
    host, _, ports = hp.rpartition(":")
    m = re.match(r"\d+", ports)
    if not host or not m:
        raise ValueError("bad host:port")
    return host.strip("[]"), int(m.group())


def parse_hy2(link: str) -> dict:
    u = urlsplit(link.strip())
    if u.scheme not in ("hysteria2", "hy2"):
        raise ValueError("not a hysteria2 link")
    host, port = _hostport(u)
    q = {k: unquote(v[0]) for k, v in parse_qs(u.query).items()}
    pw = unquote(u.username or "") + (":" + unquote(u.password) if u.password is not None else "")
    if not pw or (q.get("obfs") and q["obfs"] != "salamander"):
        raise ValueError("unsupported hysteria2 variant")
    return {"proto": "hy2", "host": host, "port": port, "pw": pw, "sni": q.get("sni") or q.get("peer", ""), "alpn": q.get("alpn", ""),
            "insecure": q.get("insecure", "0") in ("1", "true"), "obfs": q.get("obfs", ""), "obfs_pw": q.get("obfs-password", "")}


def parse_link(link: str) -> dict:
    if link.startswith("vless://"):
        return parse_vless(link)
    return parse_hy2(link)


def _outbound(n: dict, tag: str) -> dict:
    if n["proto"] == "hy2":
        tls: dict = {"enabled": True, "server_name": n["sni"] or n["host"], "insecure": n["insecure"]}
        if n["alpn"]:
            tls["alpn"] = n["alpn"].split(",")
        out = {"type": "hysteria2", "tag": tag, "server": n["host"], "server_port": n["port"], "password": n["pw"], "tls": tls}
        if n["obfs"]:
            out["obfs"] = {"type": "salamander", "password": n["obfs_pw"]}
        return out
    out = {"type": "vless", "tag": tag, "server": n["host"], "server_port": n["port"], "uuid": n["id"]}
    if n["flow"]:
        out["flow"] = n["flow"]
    if n["sec"] in ("tls", "reality"):
        tls = {"enabled": True, "server_name": n["sni"] or n["host_header"] or n["host"], "insecure": n["insecure"], "utls": {"enabled": True, "fingerprint": n["fp"]}}
        if n["alpn"]:
            tls["alpn"] = n["alpn"].split(",")
        if n["sec"] == "reality":
            tls["reality"] = {"enabled": True, "public_key": n["pbk"], "short_id": n["sid"]}
        out["tls"] = tls
    if n["net"] == "ws":
        out["transport"] = {"type": "ws", "path": n["path"], **({"headers": {"Host": n["host_header"]}} if n["host_header"] else {})}
    elif n["net"] == "grpc":
        out["transport"] = {"type": "grpc", "service_name": n["service"]}
    elif n["net"] == "httpupgrade":
        out["transport"] = {"type": "httpupgrade", "path": n["path"], **({"host": n["host_header"]} if n["host_header"] else {})}
    return out


def build_singbox_config(nodes: list[dict], base_port: int) -> dict:
    inbounds, outbounds, rules = [], [], []
    for i, n in enumerate(nodes):
        inbounds.append({"type": "socks", "tag": f"in-{i}", "listen": "127.0.0.1", "listen_port": base_port + i})
        outbounds.append(_outbound(n, f"out-{i}"))
        rules.append({"inbound": [f"in-{i}"], "outbound": f"out-{i}"})
    return {"log": {"level": "warn"}, "inbounds": inbounds, "outbounds": outbounds, "route": {"rules": rules}}


def _decode_list(text: str) -> list[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(l.startswith(SCHEMES + ("vmess://", "trojan://", "ss://")) for l in lines):
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
                r = await client.get(url, headers={"User-Agent": "v2rayN/6.42"})
                r.raise_for_status()
                links += [l for l in _decode_list(r.text) if l.startswith(SCHEMES)]
            except Exception as exc:  # noqa: BLE001 - one dead source must not stop the others
                log.warning("vpn source %s failed: %s", urlsplit(url).netloc, type(exc).__name__)  # never log the url: it carries a token
    return list(dict.fromkeys(links))


async def _config_ok(binary: str, nodes: list[dict], tmp: Path, base_port: int, name: str, sem: asyncio.Semaphore | None = None) -> bool:
    """True if `sing-box check` accepts a config with these nodes."""
    async with sem or nullcontext():
        try:
            path = tmp / f"{name}.json"
            path.write_text(json.dumps(build_singbox_config(nodes, base_port)), encoding="utf-8")
            proc = await asyncio.create_subprocess_exec(binary, "check", "-c", str(path), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            return await asyncio.wait_for(proc.wait(), timeout=60) == 0
        except Exception:  # noqa: BLE001 - unsupported node
            return False


async def fetch_alive(endpoint: str) -> list[int] | None:
    """Ports of the working nodes from the local nodes service (JSON with alive_ports); None if it is unreachable."""
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            r = await client.get(endpoint)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("nodes endpoint failed: %s", str(exc)[:80])
        return None
    ports = data.get("alive_ports") if isinstance(data, dict) else None
    if not isinstance(ports, list):
        log.warning("nodes endpoint: no alive_ports in the answer")
        return None
    return [p for p in dict.fromkeys(ports) if isinstance(p, int) and 0 < p < 65536]


async def _probe(proxy: str, max_latency: float, sem: asyncio.Semaphore) -> float | None:
    """Latency if the node reaches YouTube (tiny generate_204 request), else None."""
    async with sem:
        started = time.monotonic()
        try:
            async with new_session(proxy, timeout=max_latency * 2.5) as session:
                r = await session.get(PROBE_URL, headers=HEADERS)
            latency = time.monotonic() - started
            return latency if r.status_code in (200, 204) and latency <= max_latency else None
        except Exception:  # noqa: BLE001 - dead or blocked node
            return None


def _stop(proc: asyncio.subprocess.Process | None) -> None:
    if proc is not None and proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass


async def harvest(cfg: dict, base_port: int) -> tuple[list[str], asyncio.subprocess.Process | None, dict[str, float]]:
    """(working local proxies, the sing-box process serving them, latency per proxy). Empty list when nothing worked."""
    v, binary = cfg["vpn"], cfg["env"]["singbox_bin"]
    own = [u for u in re.split(r"[\s,]+", os.getenv("VPN_SOURCES_EXTRA", "")) if u.startswith("http")]  # secret: .env only
    only_own = os.getenv("VPN_ONLY", "").lower() in ("1", "true", "yes") and bool(own)
    sources = [] if only_own else [s for s in (v.get("sources") or []) if isinstance(s, str) and s.strip()]
    if not (sources or own) or not binary:
        log.error("vpn: no sources or no sing-box binary")
        return [], None, {}
    own_links = await _download(own) if own else []
    own_set = set(own_links)
    free_links = [l for l in (await _download(sources) if sources else []) if l not in own_set]
    random.shuffle(free_links)
    links = own_links + free_links
    candidates, is_own = [], []
    for link in own_links + free_links[: int(v.get("candidates", 800))]:
        try:
            candidates.append(parse_link(link))
            is_own.append(link in own_set)
        except ValueError:
            continue
    log.info("vpn: %d links (%d own), %d parsed as supported (%s)", len(links), len(own_links), len(candidates),
             ", ".join(f"{k} {c}" for k, c in sorted(Counter(n["proto"] for n in candidates).items())))
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = Path(tmp_s)
        if candidates and not await _config_ok(binary, candidates, tmp, base_port, "all"):  # slow path: find the bad nodes one by one
            sem = asyncio.Semaphore(8)
            ok = await asyncio.gather(*(_config_ok(binary, [n], tmp, base_port, f"t{i}", sem) for i, n in enumerate(candidates)))
            candidates, is_own = [n for n, g in zip(candidates, ok) if g], [o for o, g in zip(is_own, ok) if g]
        acc = list(zip(candidates, is_own))
        n_own = sum(o for _, o in acc)
        acc = [x for x in acc if x[1]] + [x for x in acc if not x[1]][: max(0, int(v.get("nodes", 700)) - n_own)]
        nodes, own_flags = [n for n, _ in acc], [o for _, o in acc]
        if not nodes:
            log.warning("vpn: %d links, no node accepted by sing-box", len(links))
            return [], None, {}
        path = tmp / "singbox.json"
        path.write_text(json.dumps(build_singbox_config(nodes, base_port)), encoding="utf-8")
        log_file = open(SB_LOG, "wb")  # noqa: SIM115 - handed to the child process
        proc = await asyncio.create_subprocess_exec(binary, "run", "-c", str(path), stdout=log_file, stderr=log_file)
        log_file.close()
        await asyncio.sleep(3)  # sing-box has read the config by now
        if proc.returncode is not None:
            log.warning("vpn: sing-box exited with %s: %s", proc.returncode, NodePool.tail())
            return [], None, {}
    max_latency = float(v.get("max_latency_seconds", 8.0))
    sem = asyncio.Semaphore(int(v.get("probe_concurrency", 60)))
    lat = await asyncio.gather(*(_probe(f"{LOCAL_PREFIX}{base_port + i}", max_latency, sem) for i in range(len(nodes))))
    ok_all = [(l, base_port + i, own_flags[i]) for i, l in enumerate(lat) if l is not None]
    mine = sorted((l, p) for l, p, o in ok_all if o)
    free = sorted((l, p) for l, p, o in ok_all if not o)
    good = sorted(mine + free[: max(0, int(v.get("keep", 700)) - len(mine))])  # own nodes are always kept
    if not good:
        log.warning("vpn: %d nodes started, none reached YouTube", len(nodes))
        _stop(proc)
        return [], None, {}
    log.info("vpn: %d links, %d accepted by sing-box, %d reach YouTube (<=%.0fs), median %.1fs, fastest %.2fs", len(links), len(nodes), len(good), max_latency, statistics.median(l for l, _ in good), good[0][0])
    return [f"{LOCAL_PREFIX}{p}" for _, p in good], proc, {f"{LOCAL_PREFIX}{p}": l for l, p in good}


class NodePool:
    def __init__(self, cfg: dict, stats) -> None:
        self.cfg, self.stats = cfg, stats
        self.proxies: list[str] = []
        self._set: set[str] = set()
        self.lat: dict[str, float] = {}
        self._rr: deque[str] = deque()  # round-robin order: the least recently used node is next
        self.banned: dict[str, float] = {}
        self.fails: Counter[str] = Counter()
        self.proc: asyncio.subprocess.Process | None = None
        self.quarantine = float(cfg["vpn"].get("quarantine_seconds", 120))
        self._gen = 0

    def _set_nodes(self, proxies: list[str], lat: dict[str, float]) -> None:
        self.proxies, self._set, self.lat = proxies, set(proxies), lat
        keep = [p for p in self._rr if p in self._set]
        have = set(keep)
        new = [p for p in proxies if p not in have]
        random.shuffle(new)
        self._rr = deque(keep + new)

    async def refresh_local(self) -> bool:
        """Nodes of the local nodes service: its alive ports, minus those that do not answer YouTube. Known ones are kept."""
        env, v = self.cfg["env"], self.cfg["vpn"]
        ports = await fetch_alive(env["nodes_endpoint"])
        if ports is None:
            return False  # service unreachable: keep the current list
        urls = [f"{env['nodes_scheme']}://{env['nodes_host']}:{p}" for p in ports]
        new = [u for u in urls if u not in self._set]
        sem = asyncio.Semaphore(int(v.get("probe_concurrency", 30)))
        lat = await asyncio.gather(*(_probe(u, float(v.get("max_latency_seconds", 8.0)), sem) for u in new))
        lat_map = {u: self.lat.get(u, 9e9) for u in urls if u in self._set}
        lat_map.update({u: l for u, l in zip(new, lat) if l is not None})
        proxies = [u for u in urls if u in self._set] + [u for u, l in zip(new, lat) if l is not None]
        if set(proxies) != self._set:
            log.info("nodes: %d alive in the service, %d usable (+%d new, %d new failed YouTube)", len(urls), len(proxies), len(proxies) - len(urls) + len(new), len(urls) - len(proxies))
        self._set_nodes(proxies, lat_map)
        self.banned = {k: t for k, t in self.banned.items() if k in self._set}
        self.fails = Counter({k: n for k, n in self.fails.items() if k in self._set})
        return True

    async def refresh(self) -> bool:
        if self.cfg["env"].get("nodes_endpoint"):
            return await self.refresh_local()
        proxies, proc, lat = await harvest(self.cfg, PORT_BASES[self._gen % 2])
        if not proxies:
            return False
        old, self.proc = self.proc, proc
        self._set_nodes(proxies, lat)
        self.banned.clear()
        self.fails.clear()
        self._gen += 1
        if old is not None:
            asyncio.get_running_loop().call_later(120, _stop, old)  # grace: workers move to the new list
        return True

    async def run(self) -> None:
        refresh_s = float(self.cfg["vpn"].get("refresh_minutes", 60)) * 60
        if self.cfg["env"].get("nodes_endpoint"):
            refresh_s = float(self.cfg["vpn"].get("local_refresh_seconds", 60))
        next_at = 0.0
        while True:
            dead = self.proc is not None and self.proc.returncode is not None
            if dead or time.monotonic() >= next_at:
                if dead:
                    log.warning("sing-box died (%s): %s", self.proc.returncode, self.tail())
                ok = await self.refresh()
                next_at = time.monotonic() + (refresh_s if ok else 60)
            await asyncio.sleep(10)

    def usable(self, proxy: str | None) -> bool:
        return proxy in self._set and self.banned.get(proxy, 0) <= time.monotonic()

    def pick(self) -> str | None:
        """Round-robin over live nodes: a node is reused only after all the others had their turn."""
        now = time.monotonic()
        for _ in range(len(self._rr)):
            p = self._rr[0]
            self._rr.rotate(-1)
            if self.banned.get(p, 0) <= now:
                return p
        return None

    def fastest(self, n: int = 2) -> list[str]:
        """The n lowest-latency nodes (quarantine ignored: it is about YouTube, not about url-check)."""
        return sorted(self._set, key=lambda p: self.lat.get(p, 9e9))[:n]

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
            return " | ".join(Path(SB_LOG).read_text(errors="ignore").splitlines()[-lines:])[:600]
        except OSError:
            return ""

    def close(self) -> None:
        _stop(self.proc)
