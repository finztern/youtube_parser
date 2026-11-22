# youtube_parser (Exorde)

Standalone YouTube comment collector. Config: `config.yaml` + `.env` (see `.env.example`).

Run: `cp .env.example .env && docker compose up -d --build`
Probe (TZ p.0): `PROXY=socks5h://127.0.0.1:PORT python3 tools/yt_time_probe.py` -> `docs/yt_time_probe_report.md`
Tests: `pip install pytest && pytest -q`
