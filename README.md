# Dave's Stock Tools

Interactive FastAPI app for analyzing stock option Vanna/GEX profiles and a multi-ticker gamma scanner. Built as a personal experiment for fast prototyping and “vibe-coding” mainly with ChatGPT 5.1/5.2 Codex-Max and VS Code. Current version: **v0.1.0** (semantic versioning; bump in `core/__init__.py`).

The main intention was a personalized tool for running locally to aid in stock monitoring, supplementing other information sources. Sharing to Github as a reference for others to what you can do with trendy tools. If desiring a test drive without using the Massive API, go to the home page and select demo mode to go for a test drive.

## Main Features

- Vanna and GEX heatmaps per ticker using Massive API (formerly Polygon) snapshots. Tunable expiry scope (0DTE/1DTE/weeklies/monthlies), strike windows, caching, auto-refresh, sorting, and other UX niceties.
- Gamma scanner across a watchlist with flip detection, per-ticker ± windows, GEX trend deltas, OI trend sparklines, and column sorting (click to cycle asc/desc/natural + drag to reorder when “natural”). Jobs are parallelized and cached, but large lists can still take a few minutes.
- News/events pages (Massive news + Investing.com RSS).
- Unified debug console to watch server/client logs.

## Gamma Scanner Highlights

- Add tickers with individual ±% windows; watchlist is persisted locally (browser storage) and supports quick clear/re-add flows.
- Sorting: click a column header to toggle asc/desc/natural order; “natural” turns on drag handles so you can pin your own ordering. Default “Score” sorts by farthest flip distance across expirations.
- Columns: weekly/monthly/all flip strikes with distances, Top Gamma Strike with % change, GEX Trend (% change in net GEX above the top call strike vs prior snapshot), and OI Trend sparklines for context.
- Runs show progress + ETA; you can stop a scan mid-run. GEX links on each row open the ticker’s GEX view with the matching window.

## Intentional Design Tradeoffs

- Local-only persistence via browser storage for favorites; no auth/identity since this was built for personal use. Add your own multi-user auth later if needed.
- Cache targets frequent Massive (Polygon) calls without a full database. GEX data is shared across tickers/window sizes and kept over time for pseudo-historical views (not part of the Massive API tier used here).
- Unified Debug Console helps debug stuck jobs/progress and can be extended for other logging needs.
- TradingView workflow: copied/pasted generated Pine overlays directly into TradingView instead of publishing/auto-updating to avoid license issues and hosting overhead.
- Job worker performance: built for a premium Massive options plan, so rate limits weren’t a concern. Adjust workers/parallel requests for other tiers via **SCANNER_CONCURRENCY** (scanner fan-out) and **POLYGON_CONCURRENCY** (Massive request fan-out).

## Comments and Objectives of Dave's Stock Tools

1. **Vibe Coding** - I explored the use case of AI assist for coding a set of requirements, whereby I started with an initial set of firm requirements but iterated through the rest as I tried to use the tool/output along the way. The codex started in GPT 5, but as 5.1 Max came along, I could see a large increase in the capability of avoiding "*2 step forward, 1 step back*" situations with causing errors or nailing the requirement on the first prompt. I suspect this improves tremendously in the future as the AI giants iterate.
2. **Dogfood a Tool from Scratch** - I built a very capable and successful tool for my stock trading habits, where I start with GEX exposure analysis for 0DTE, 1DTE, weekly and monthly analysis for taking positions. Shoutout to Geeks of Finance and TanukiTrade for getting me started on this financial path.
3. **FastAPI and python** - I selected this tech stack since I'm comfortable in python myself. Obviously, this can be done in many other languages as necessary for the backend, and is probably very portable on the templated front end abstraction architecture.

## Quickstart

```bash
# create a project-local venv (IDE-friendly)
python3 -m venv .venv
source .venv/bin/activate

# install deps (dev includes lint/test tools)
pip install -r requirements-dev.txt

# configure secrets (MASSIVE_API_KEY)
cp .env.example .env
# defaults: scanner starts with SPY/QQQ/AAPL/MSFT/NVDA at a 10% window; change in the UI or tweak `SCANNER_DEFAULTS` in `core/web.py` if you want a different starting list.
```

Populate `.env` with `DEMO_MODE=1` to use the built-in sample tickers (AAPL/MSFT/SPY/QQQ/NVDA) and click around without any API keys. Set `DEMO_MODE=0` and fill in `MASSIVE_API_KEY` (or legacy `POLYGON_API_KEY`) for live data. For production or shared demos, set a unique `SESSION_SIGNER_SECRET` (explained below). Then:

```bash
uvicorn app:app --reload --host 0.0.0.0 --port 8000
# or
./start_server.sh
```

If you prefer a shared venv elsewhere, set `VENV_PATH` before running `start_server.sh` and activate that env manually in your shell/IDE.

## Secrets & Environment

- `.env` and other env files are gitignored; `.env.example` is the safe template to commit.
- `DEMO_MODE`: when `1`, all ticker endpoints serve bundled demo data (no Massive calls). Set to `0` + provide `MASSIVE_API_KEY`/`POLYGON_API_KEY` to use live data.
- `SESSION_SIGNER_SECRET`: used to sign the session cookie so it can’t be forged/tampered with. For solo local use, the default is fine; for any shared deployment or if you care about session integrity, set this to a long random string.
- `CORS_ALLOW_ORIGINS`: comma-separated allowlist (e.g., `http://localhost:8000,http://127.0.0.1:8000`). If unset, CORS is disabled. Set `*` to allow all origins (credentials will be disabled).
- `CORS_ALLOW_CREDENTIALS`: `1` to allow cookies/credentials for cross-origin requests (requires explicit origins).
- Keep API keys (`MASSIVE_API_KEY`/`POLYGON_API_KEY`) and `SESSION_SIGNER_SECRET` only in `.env` or shell environment when deploying/publishing.

### Session Signing Secret (what/why)

- Every browser session gets a signed cookie so the server can associate background jobs (scanner/Vanna/GEX) to you and guard cached results. Signing stops anyone from forging another user’s cookie.
- Local-only usage can rely on the default; any shared or deployed environment should set `SESSION_SIGNER_SECRET` to a unique, long random string.

## Demo vs Live Data

- Demo mode (`DEMO_MODE=1`) ships with example scanner/GEX/Vanna payloads so the UI renders fully without an API key. Great for screenshots, local demos, or onboarding.
- Live mode (`DEMO_MODE=0`) fetches real data from Massive (Polygon) and requires `MASSIVE_API_KEY` (or `POLYGON_API_KEY`).
- Switch modes by toggling `DEMO_MODE` in `.env` and restarting the server, or click the Demo Mode button on the home page (next to the Unified Debug Console link) to set it via cookie.

## Screenshots

See `screenshots/` folder for examples:

- GEX Ticker Example
- Vanna Ticker Example
- Gamma Scanner Example

## Tests & Quality

- Unit tests: `pytest` (requires dev deps installed)
- Lint: `ruff check .`
- Format: `black .`

`pyproject.toml` contains shared configuration. Add more coverage in `tests/` as features evolve.

## Project Layout

- `app.py` – FastAPI entrypoint (imports composed router from `routes/`).
- `routes/` – modular routers (debug console, events/news).
- `views.py` – main page + API routes for Vanna/GEX/scanner.
- `engine.py` – core Vanna/GEX computation helpers and Massive (Polygon) fetchers.
- `start_server.sh` – convenience launcher that loads `.env`.

## Releases & Versioning

- Semantic version lives in `core/__init__.py` as `__version__`; bump MAJOR.MINOR.PATCH when shipping changes.
- Tag releases with git for GitHub: `git tag -a v0.1.0 -m "Release scanner sorting + GEX trend"` then `git push origin main --tags`.

## Notes

- Massive (Polygon) powers live data; keep `DEMO_MODE=1` if you want a credential-free demo.
- All runtime state (jobs, logs, caches) is in-memory; multi-process deployments need a shared backend.

## License

MIT — see `LICENSE`. Copyright 2026 Dave Selinger (<https://github.com/dselinger>).
