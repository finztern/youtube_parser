"""Config: config.yaml + environment (.env). No secrets in the yaml."""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import yaml


def load() -> dict:
    cfg = yaml.safe_load(Path(os.getenv("CONFIG_PATH", "config.yaml")).read_text(encoding="utf-8")) or {}
    cfg.setdefault("youtube", {})
    cfg.setdefault("vpn", {})
    env = {
        "store_endpoint": os.getenv("STORE_ENDPOINT", "").strip(),
        "url_check_endpoint": os.getenv("URL_CHECK_ENDPOINT", "https://url-check.exorde.network/v1/check").strip(),
        "url_check_proxy": os.getenv("URL_CHECK_PROXY", "").strip(),
        "xray_bin": os.getenv("XRAY_BIN") or shutil.which("xray") or "",
        "log_level": os.getenv("LOG_LEVEL", "INFO").upper(),
        "dash_host": os.getenv("DASH_HOST", "0.0.0.0").strip(),
        "dash_port": int(os.getenv("DASH_PORT", "8090") or 0),
    }
    if not env["store_endpoint"]:
        raise SystemExit("STORE_ENDPOINT is not set (.env)")
    if "upload.exorde.network" in env["store_endpoint"]:
        raise SystemExit("STORE_ENDPOINT must be the local collector, not upload.exorde.network")
    if cfg["youtube"].get("parent_toplevel_mode", "self") not in ("self", "null"):
        raise SystemExit("youtube.parent_toplevel_mode must be self|null")
    cfg["env"] = env
    return cfg
