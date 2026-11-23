# youtube_parser (Exorde)

Standalone YouTube comment collector. Config: `config.yaml` + `.env` (see `.env.example`).

Run: `cp .env.example .env && docker compose up -d --build`
Probe (TZ p.0): `docker compose run --rm -e PROXY=pool youtube-parser python3 tools/yt_time_probe.py` -> `docs/yt_time_probe_report.md`
Tests: `pip install pytest && pytest -q`
Hot reload: edit `config.yaml` on the host (saved in place) - `target_rate`, `buffer_size`, batch/url-check/hot params apply within ~2 s; workers, vpn, seed need a restart.
Dashboard: `http://<host>:8090` (live charts, `/api` JSON; `DASH_PORT=0` turns it off, no auth)
Metrics: `http://<host>:8090/metrics` (Prometheus format). Grafana + Prometheus (optional): `docker compose --profile monitoring up -d --build` -> Grafana `http://<host>:3000` (admin / `GRAFANA_PASSWORD`, default `admin`), Prometheus `127.0.0.1:9090`. Needs `DASH_PORT` != 0.
