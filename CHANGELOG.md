# Changelog

All notable changes to this project are documented in this file.

## [0.2.0] - 2026-03-11

### Added

- Added a canonical gamma math layer in `core/gamma_math.py` to normalize option-chain inputs, compute spot-space zero gamma, classify gamma regime, and surface solver diagnostics consistently across the GEX ticker, scanner, demo data, and cache outputs.
- Added solver presets and advanced controls for horizon, strike band, 0DTE handling, tail handling, and refinement behavior.
- Added a formal diagnostics surface for GEX and Vanna, including full-details panels and copyable debug JSON.
- Added solver-preview support and richer metadata so the UI can explain active query context instead of only showing chart output.
- Added demo-mode expiry generation so ticker pages render realistic populated states without live API credentials.
- Added regression coverage for canonical gamma math, contract-universe pagination, aggregate-vs-scanner parity, cache-key versioning, and demo-mode expiry behavior.

### Changed

- Reoriented the gamma product story around Zero Gamma and Gamma Regime instead of legacy Macro/Micro flip terminology.
- Refactored the GEX ticker, scanner, and supporting views to share a common gamma vocabulary, inclusion rules, and cache semantics.
- Simplified scanner output to focus on the current architecture: spot, zero gamma, regime, net GEX, and confidence.
- Updated README, release documentation, and visible application versioning for the `v0.2.0` release target.
- Refreshed demo data and screenshots so drive-by exploration matches the current UI and query model.

### Fixed

- Fixed net GEX handling so headline `Net GEX (Spot-Scaled)` remains aligned to the selected page universe while the solver continues to operate on raw signed gamma internally.
- Fixed stale flip-era behavior that could fabricate synthetic roots when no crossing existed in range; the current architecture reports the absence of a tested zero-gamma crossing instead of inventing one.
- Fixed cache-key drift by including solver, expiry-filter, and 0DTE inputs in cache-key generation.
- Fixed selected-expiry ticker parity by normalizing single-expiry and aggregate/scanner paths onto the same canonical chain model.
- Fixed demo-mode first-run behavior by serving bundled expirations instead of relying on live expiry lookups.

### Removed

- Removed obsolete flip recomputation helpers from `core/cache.py`.
- Removed unused template/router artifacts that no longer reflect the active application structure: `templates/raw.html`, `templates/partials/ticker_header.html`, and the dead composed-router logic in `routes/__init__.py`.
- Removed stale release-prep notes as standalone source-of-truth documents in favor of formal README, release notes, and changelog content.
- Removed an outdated scanner empty-state label and an old home-page layout hack that no longer matched the polished UI.

### Performance

- Reworked single-expiry GEX runs to prefer bulk expiry snapshots over per-contract OI/IV fan-out.
- Extended aggregate contract fetching beyond the provider's first listing page so large chains are not silently capped at the first 2,000 contracts.
- Reused the same normalized gamma-chain flow across ticker and scanner paths to reduce redundant transformations and improve parity.
- Added reduced-universe solver refinement with guarded fallback to the stable first-pass root when refinement diverges.

### UX / UI

- Modernized the GEX and Vanna pages with clearer information hierarchy, improved metric cards, better settings surfaces, and stronger chart framing.
- Tightened the scanner table around the metrics that matter for the current gamma model and improved the explanatory copy around scopes and 0DTE handling.
- Refined the home page, theming, and screenshot states so the project presents cleanly to first-time GitHub visitors.
- Standardized labels around Zero Gamma, Gamma Regime, confidence, and demo mode.

### Diagnostics / Developer Experience

- Expanded automated coverage to protect the new gamma solver, aggregate fetch behavior, and demo-mode UX.
- Improved documentation so the release story, architecture direction, run flow, and model assumptions are explicit for reviewers.
- Kept intentional robustness fallbacks documented rather than hidden, especially around provider snapshot failure paths and zero-OI Vanna behavior.

## [0.1.0] - 2026-02-02

- Initial public release.
