# Dave's Stock Tools

FastAPI application for local options-flow analysis across three workflows:

- `GEX / Zero Gamma` ticker analysis with spot-space zero-gamma solving and strike-space charts
- `Gamma Scanner` for multi-ticker net GEX and regime monitoring
- `Vanna Ticker` for strike-by-strike vanna profiles and cumulative context

**NOT Investment Advice** for using this tool at your own discretion.

## Main Features

`v0.2.0` is the first post-release architecture pass after `v0.1.0`. The project now centers on a shared gamma math layer, faster bulk-snapshot query paths, clearer diagnostics, and a more consistent UI for ticker, scanner, and demo-mode exploration.

## What's New In v0.2.0

- Replaced the old Macro/Micro flip-centric workflow with a canonical Zero Gamma and Gamma Regime model shared across the GEX ticker and scanner.
- Refactored gamma computation into `core/gamma_math.py`, including solver presets, reduced-universe refinement, and explicit diagnostics.
- Moved single-expiry GEX runs onto bulk expiry-snapshot fetches, improving parity with aggregate and scanner paths while preserving a fallback when provider responses are incomplete.
- Corrected net GEX unit handling and separated the page headline `Net GEX (Spot-Scaled)` metric from the internal solver's raw signed gamma values.
- Modernized the GEX and Vanna pages with stronger information hierarchy, advanced settings panels, full-details views, and copy-to-clipboard debug JSON.
- Simplified the scanner around the current gamma model: Zero Gamma, Gamma Regime, Net GEX, and solver confidence.
- Refreshed demo mode so first-run screenshots and click-through exploration render with current-looking expirations and populated states.

## Main Capabilities

### GEX / Zero Gamma

- Strike-space GEX charts for calls, puts, and accumulated net GEX
- Spot-space Zero Gamma solving with configurable horizons, strike bands, tail handling, and refinement modes
- Gamma Regime and solver-confidence context for parity/debugging work
- Export helpers for TradingView pairs and Pine snippets

### Gamma Scanner

- Watchlist scanning across weekly, monthly, or aggregate expiry scopes
- Per-ticker windows, 0DTE removal toggle, sortable results, and direct drill-down into the GEX ticker
- Shared zero-gamma math with the single-ticker GEX page so scanner and ticker stay aligned

### Vanna Ticker

- Per-strike and cumulative vanna charts
- Weighting controls, expiry/scope controls, spot overrides, and full diagnostic details
- Same job-management and demo-mode workflow as the gamma pages

## Screenshots

All screenshots below are captured in demo mode with bundled sample data.

### Home

![Home demo](screenshots/home-demo.png)

### GEX Ticker

![GEX ticker demo](screenshots/gex-ticker-demo-nvda.png)

### Gamma Scanner

![Gamma scanner demo](screenshots/gamma-scanner-demo.png)

### Vanna Ticker

![Vanna ticker demo](screenshots/vanna-ticker-demo-nvda.png)

## Architecture

The `v0.2.0` release re-centers the project around a shared gamma math layer, a bulk-snapshot query engine, and a more explicit diagnostics model.

The full engineering overview lives in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Demo Mode

Demo mode is intended for screenshots, onboarding, and drive-by repo inspection.

- Set `DEMO_MODE=1` in `.env` to use bundled sample payloads without any API keys.
- The home page also exposes a Demo Mode toggle that persists in a cookie.
- Demo mode now returns synthetic expiry lists as well, so GEX and Vanna pages load like a realistic first-run experience.
- The screenshots in this README represent demo-mode data, not live market output.

## Running Locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env

# Demo mode
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

For live data, set `DEMO_MODE=0` and provide `MASSIVE_API_KEY`. `POLYGON_API_KEY` is still accepted as a compatibility alias for older local setups.

## Main Pages

- `/` home page for mode selection, favorites, and demo-mode toggling
- `/gexticker/{symbol}` GEX / Zero Gamma analysis
- `/scanner` multi-ticker gamma scanner
- `/ticker/{symbol}` Vanna analysis
- `/debug` unified debug console
- `/events/{symbol}` and `/econ` supporting event/news pages

## Diagnostics And Transparency

`v0.2.0` adds more explicit debugging surfaces so results can be inspected instead of trusted blindly.

- GEX exposes solver previews, confidence labels, included-expiration context, and a full-details panel.
- Vanna exposes its active query state plus copyable debug JSON.
- Scanner rows inherit the same gamma regime and confidence framing used by the single-ticker views.
- Cache keys now track the solver profile, expiry filters, 0DTE handling, and calc-version inputs so old payloads do not masquerade as current results.

## Data And Model Assumptions

- Zero Gamma is solved in spot space from total signed gamma as a function of spot. It is intentionally not the same thing as the older static strike-flip approximations.
- `Net GEX (Spot-Scaled)` is the headline page metric. The solver still works from raw signed gamma internally, and the UI now makes that distinction explicit.
- Provider IV/OI freshness, expiry filtering, and vendor-specific weighting differences can still create drift versus external dashboards.
- Runtime state is in-memory. Multi-process or hosted deployments would need a shared cache/job backend.

## Quality

- Tests: `.venv/bin/python -m pytest -q`
- Lint: `.venv/bin/python -m ruff check .`
- Format: `.venv/bin/python -m black .`

Shared tooling config lives in `pyproject.toml`.

## Release Context

- Current release target: `v0.2.0`
- Last GitHub release/tag baseline: `v0.1.0`
- Full release notes: `RELEASE_NOTES.md`
- Detailed changelog: `CHANGELOG.md`

## License

MIT. See `LICENSE`.
