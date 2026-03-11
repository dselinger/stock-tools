# Release Notes

## v0.2.0 GitHub Release Summary

`v0.2.0` is the first major cleanup and architecture pass after the initial `v0.1.0` public release. The project now treats Zero Gamma as the canonical gamma workflow, replaces flip-era assumptions with a shared spot-space solver, and aligns the GEX ticker, scanner, and demo mode around the same normalized chain model.

This release also redesigns the query engine beneath the UI. Single-expiry GEX runs now prefer bulk expiry snapshots instead of slow per-contract fan-out, aggregate fetches paginate past the provider's first contract page, and cache semantics now include solver/filter inputs so diagnostic work is reproducible. The net result is better parity with external dashboards, faster selected-expiry runs, and fewer hidden mismatches between scanner and ticker output.

On the product side, the GEX and Vanna pages received a substantial information-architecture refresh: stronger metric summaries, modernized layouts, advanced options panels, full diagnostic views, and copyable JSON for deeper inspection. Demo mode was refreshed as well so first-time GitHub visitors can open the app, click through populated states, and understand the intended workflow without live credentials.

## Highlights

- Zero Gamma / GammaFlip modernization centered on a shared spot-space solver
- Faster and more consistent ticker/scanner query paths using bulk snapshot architecture
- Corrected net GEX unit handling and clearer separation between headline metrics and solver internals
- Better diagnostics, solver confidence reporting, and transparent full-details views
- Modernized GEX/Vanna layouts, cleaner scanner workflow, and refreshed demo screenshots

## Short "What Changed" Summary

`v0.2.0` turns the project into a cleaner, more reviewable open-source release: Zero Gamma is now the primary gamma model, query performance is materially better, ticker/scanner outputs are more consistent, diagnostics are much more transparent, and the README/demo flow now represent the current architecture accurately.
