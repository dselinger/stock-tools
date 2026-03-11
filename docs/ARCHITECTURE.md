# Architecture

## Overview

Dave's Stock Tools is a local-first analytical dashboard for exploring options market structure through:

- Gamma Exposure (GEX)
- Zero Gamma / Gamma Flip levels
- Vanna exposure

The application emphasizes transparent calculations, fast interactive workflows, and inspectable diagnostics rather than opaque indicator outputs.

The system is intentionally lightweight and designed primarily for local analytical exploration rather than distributed deployment.

Core technologies include:

- FastAPI for HTTP endpoints and page delivery
- Jinja2 templates for UI rendering
- httpx for upstream market-data requests
- pandas / numpy for chain normalization and vectorized math
- in-memory job management with lightweight disk caching

At a high level the system is organized into four layers.

                ┌───────────────────────────┐
                │        Browser UI         │
                │ (Jinja Templates + JS)   │
                └──────────────┬────────────┘
                               │
                               ▼
                ┌───────────────────────────┐
                │      FastAPI Routes       │
                │     views.py / routes     │
                └──────────────┬────────────┘
                               │
                               ▼
                ┌───────────────────────────┐
                │      Query / Job Engine   │
                │          engine.py        │
                └──────────────┬────────────┘
                               │
                               ▼
                ┌───────────────────────────┐
                │   Math + Chain Normalizer │
                │      core/gamma_math.py   │
                └──────────────┬────────────┘
                               │
                               ▼
                ┌───────────────────────────┐
                │   Market Data Provider    │
                │ Polygon / Massive APIs   │
                └───────────────────────────┘

The design intentionally separates financial logic from UI logic, ensuring that the math layer remains reusable and consistent across pages.

---

## Key Modules

Module | Responsibility
------ | --------------
engine.py | Query orchestration, market data acquisition, job lifecycle
core/gamma_math.py | Canonical gamma exposure calculations and solver
core/cache.py | Short-lived disk caching and canonical payload reuse
views.py | FastAPI API endpoints used by the UI
routes/ | Page routing and template entry points
templates/ | Jinja HTML templates for the UI
core/web.py | UI helpers and context utilities
core/demo_data.py | Demo mode payloads for screenshots and local exploration

---

## Data Flow

The typical request lifecycle is:

1. A page loads (`/`, `/gexticker/{symbol}`, `/scanner`, `/ticker/{symbol}`).
2. The browser initiates a background job through an API endpoint.
3. `engine.py` fetches market data from Massive (Polygon) or demo payloads.
4. The raw option chain is normalized into a canonical contract schema.
5. The canonical chain is passed into the gamma or vanna calculation pipeline.
6. Results are written into the job manager and returned as JSON.
7. The UI renders metrics, charts, diagnostics, and debug information.

This architecture ensures:

- UI components never perform financial calculations
- all analytical logic lives in shared engine/math modules
- scanner and ticker workflows use identical computation paths

---

## Polygon / Massive Data Acquisition

Live mode retrieves market data from Massive (formerly Polygon).

The application consumes several categories of endpoints:

- underlying spot and previous close
- option contract listings
- option snapshot chains by underlying
- supporting metadata

The acquisition strategy changed significantly in v0.2.0.

### Listing-First Chain Construction

For scanner and aggregate workflows the engine first enumerates the full contract universe.

This avoids truncation when chains exceed the provider’s first page (typically 2000 contracts).

### Bulk Expiry Snapshots

For single-ticker GEX runs the preferred path is:

Underlying → Expiry Snapshot → Chain Assembly

instead of:

Underlying → Contract List → Per-Contract Snapshot Fan-Out

This reduces API fan-out and dramatically improves runtime.

### Fallback Behavior

Certain fallback paths remain intentionally:

- contract-level snapshots when expiry snapshots return unusable rows
- alternate spot lookup providers when upstream values are missing

These paths improve robustness against inconsistent upstream responses.

---

## Gamma Calculation Model

The canonical gamma model lives in:

core/gamma_math.py

The module performs three responsibilities:

1. normalize and filter option-chain rows
2. compute signed gamma exposure
3. derive higher-level outputs (Zero Gamma, regimes, diagnostics)

Normalized contracts include:

- strike
- option type
- open interest
- implied volatility
- time to expiry
- expiry identifier
- contract multiplier

The same normalized chain powers:

- GEX ticker outputs
- scanner summaries
- demo payload normalization
- cache canonicalization

This centralization is the main architectural shift from v0.1.0.

---

## Algorithms and Numerical Methods

The analytical components rely on a small set of numerical techniques optimized for stability and interactive performance.

### Gamma Calculation

Option gamma is computed using the Black–Scholes model.

For each contract:

γ = φ(d1) / (S σ √T)

Where:

- φ = standard normal probability density
- S = underlying spot price
- σ = implied volatility
- T = time to expiry

Signed exposure is then calculated as:

signed_gamma = sign * γ(S) * open_interest * contract_multiplier

### Exposure Scaling

Two related metrics are derived:

Net GEX = signed_gamma * S  
Raw Signed Gamma = signed_gamma * S²

Purpose:

Metric | Usage
------ | ------
Net GEX | headline UI metric
Raw Signed Gamma | solver root-finding

Separating the two improves clarity and aligns with external dashboards.

### Zero Gamma Root Finding

Zero Gamma is treated as a spot-space root-finding problem.

Solver process:

1. Construct a spot grid around the current price
2. Evaluate total gamma at each grid point
3. Detect sign-change intervals
4. Interpolate candidate roots
5. Optionally refine using a reduced contract universe

Interpolation is linear between adjacent grid points.

### Solver Complexity

Let:

N = number of contracts  
G = grid points

Solver complexity is approximately:

O(N × G)

Typical values:

- N ≈ 200–500 contracts after filtering
- G ≈ 50–100 grid points

This keeps computation comfortably below one second for most chains.

---

## Query Engine Architecture

The query engine resides primarily in engine.py.

Responsibilities include:

- upstream data acquisition
- background job orchestration
- result caching
- canonical chain assembly

### Job Model

The system uses an in-memory JobManager keyed by session and job ID.

Features include:

- asynchronous job execution
- progress polling
- cancellation support
- per-job logs and diagnostics

This design is appropriate for local single-process analysis tools.

### Cache Model

Two levels of caching exist.

In-Memory

- active job results
- short-lived ticker outputs

Disk Cache

- selected GEX payloads
- history/trend helpers

Cache keys include calculation context such as:

- expiry scope
- 0DTE inclusion
- solver configuration
- calculation version token

This prevents stale results when solver settings change.

---

## Performance Improvements in v0.2.0

Most improvements are architectural rather than micro-optimizations.

Key changes include:

Bulk Snapshot Preference  
Expiry-level snapshots replace contract fan-out in most workflows.

Paginated Contract Universes  
Chains now fetch beyond the provider’s first page.

Shared Normalization Pipeline  
Ticker and scanner now reuse identical normalization logic.

Reduced Solver Universes  
The solver may refine roots using smaller relevant subsets.

These changes produced roughly:

~10× improvement in ticker and scanner latency

in typical usage.

---

## Application Pages

### GEX Ticker

The most detailed workflow.

Provides:

- strike-space GEX charts
- Net GEX and Zero Gamma summaries
- solver configuration
- full diagnostics and debug JSON

### Gamma Scanner

A watchlist-oriented monitoring page.

Outputs:

- spot
- zero gamma
- gamma regime
- net GEX
- solver confidence

The scanner intentionally mirrors ticker semantics.

### Vanna Ticker

A separate calculation path for volatility sensitivity.

Outputs:

- per-strike vanna
- accumulated net vanna
- weighting modes
- expiry filters
- debug diagnostics

The UI framework is shared with the gamma pages.

---

## Diagnostics and Debugging

Diagnostics are treated as a first-class product feature.

Exposed data includes:

- solver diagnostics
- contract inclusion filters
- cache metrics
- upstream fetch metadata
- full copyable debug JSON

This transparency helps explain differences versus vendor dashboards.

---

## Demo Mode

The application includes a fully supported demo mode.

When DEMO_MODE=1:

- bundled payloads replace live market data
- expiry endpoints return synthetic expirations
- ticker and scanner pages render populated examples

This allows GitHub reviewers to explore the UI without API keys.

---

## Design Philosophy

The system follows several guiding principles.

One canonical math layer  
Financial logic should be implemented once and reused.

Transparent analytics  
Users should understand how outputs are produced.

Robust but minimal fallbacks  
Fallback logic exists only when it meaningfully improves reliability.

Optimize the common path  
Bulk snapshots and solver refinement accelerate normal usage.

Make the repository reviewable  
Architecture, diagnostics, and documentation are structured so external engineers can understand the system quickly.