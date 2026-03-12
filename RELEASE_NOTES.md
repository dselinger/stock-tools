# Release Notes

## v0.2.1 GitHub Release Summary

`v0.2.1` is the release that closes the loop on the new expiration-scope architecture. The GEX ticker, Gamma Scanner, Vanna ticker, demo payloads, and expiry metadata endpoints now resolve `Selected`, `0DTE`, `1DTE`, `Weekly`, `Monthly`, `M1`, `M2`, and `All` through the same shared logic, so the UI and solver operate on the same explicit expiration universe.

The scanner is the most visible product change in this cut. It now exposes a richer monitoring surface with W1/M1/M2 term-shape analytics, spot-density context, explicit unsupported-scope handling, monthly-expiry tagging, and a persistent context strip that keeps scope, 0DTE handling, ticker count, and freshness visible during a run. GEX and Vanna were updated in parallel so their page-level scope controls and full-details panels explain the same resolved expiration set instead of leaving that mapping implicit.

Under the hood, this release hardens expiry discovery and cache behavior. Expiry listing now pages contract references more defensively, partial or timed-out results no longer clobber good cached expiration sets, and the active GEX calculation token moves to `gamma-v5` so older cache entries cannot masquerade as the current scope model. Demo mode and screenshots were refreshed at the same time so the repository shows the app as it now behaves.

## Highlights

- Shared expiration-scope model across GEX, scanner, Vanna, demo mode, and expiry APIs
- Scanner term-shape, spot-density, exclusion-state, and monthly-expiry context
- More reliable GEX scope resolution and confidence labeling based on the actual solver universe
- Safer expiry caching and a new `gamma-v5` calc token for cache separation
- Refreshed docs and screenshots for the `v0.2.1` release target

## Short "What Changed" Summary

`v0.2.1` takes the `v0.2.0` architecture pass and makes the expiration model coherent end to end: scope resolution is shared, unsupported cases are explicit, scanner context is materially richer, and the docs/demo assets now match the shipped UI.
