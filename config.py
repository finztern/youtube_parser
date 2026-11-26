"""Config: config.yaml + environment (.env). No secrets in the yaml."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
from pathlib import Path

import yaml

log = logging.getLogger("config")


def _path() -> Path:
    return Path(os.getenv("CONFIG_PATH", "config.yaml"))


def _read() -> dict:
    cfg = yaml.safe_load(_path().read_text(encoding="utf-8")) or {}
    cfg["youtube"] = cfg.get("youtube") or {}
    cfg["vpn"] = cfg.get("vpn") or {}
    if cfg["youtube"].get("parent_toplevel_mode", "self") not in ("self", "null"):
        raise ValueError("youtube.parent_toplevel_mode must be self|null")
    for k in ("target_rate", "buffer_size"):
        if float(cfg["youtube"].get(k, 0)) < 0:
            raise ValueError(f"youtube.{k} must be >= 0")
    return cfg


async def watch(cfg: dict, callbacks: list, interval: float = 2.0) -> None:
    """Hot reload: when config.yaml changes, update cfg['youtube'] / cfg['vpn'] in place and call the callbacks."""
    last = None
    try:
        last = _path().stat().st_mtime_ns
    except OSError:
        pass
    while True:
        await asyncio.sleep(interval)
        try:
            m = _path().stat().st_mtime_ns
            if m == last:
                continue
            last = m
            new = _read()
        except Exception as exc:  # noqa: BLE001 - keep the old config on a broken edit
            log.warning("config reload failed, keeping old: %s", str(exc)[:120])
            continue
        for sec in ("youtube", "vpn"):
            cfg[sec].clear()
            cfg[sec].update(new[sec])
        for cb in callbacks:
            try:
                cb()
            except Exception as exc:  # noqa: BLE001
                log.warning("config callback failed: %s", str(exc)[:120])
        log.info("config reloaded: target_rate=%s buffer_size=%s", cfg["youtube"].get("target_rate", 0), cfg["youtube"].get("buffer_size"))


def load() -> dict:
    try:
        cfg = _read()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    env = {
        "store_endpoint": os.getenv("STORE_ENDPOINT", "").strip(),
        "url_check_endpoint": os.getenv("URL_CHECK_ENDPOINT", "https://url-check.exorde.network/v1/check").strip(),
        "url_check_proxy": os.getenv("URL_CHECK_PROXY", "").strip(),
        "nodes_endpoint": os.getenv("NODES_ENDPOINT", "").strip(),  # local nodes service (alive_ports); empty = free vless lists
        "nodes_host": os.getenv("NODES_HOST", "host.docker.internal").strip(),
        "nodes_scheme": os.getenv("NODES_SCHEME", "socks5h").strip(),
        "singbox_bin": os.getenv("SINGBOX_BIN") or shutil.which("sing-box") or "",
        "log_level": os.getenv("LOG_LEVEL", "INFO").upper(),
        "dash_host": os.getenv("DASH_HOST", "0.0.0.0").strip(),
        "dash_port": int(os.getenv("DASH_PORT", "8090") or 0),
    }
    if not env["store_endpoint"]:
        raise SystemExit("STORE_ENDPOINT is not set (.env)")
    if "upload.exorde.network" in env["store_endpoint"]:
        raise SystemExit("STORE_ENDPOINT must be the local collector, not upload.exorde.network")
    cfg["env"] = env
    return cfg
