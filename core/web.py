from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi.templating import Jinja2Templates

# --- UI assets ---
FAVICON_SVG = """
<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 128 128'>
  <defs>
    <linearGradient id='g' x1='0' y1='0' x2='1' y2='1'>
      <stop offset='0%' stop-color='#0b0f14'/>
      <stop offset='100%' stop-color='#1f2937'/>
    </linearGradient>
    <linearGradient id='a' x1='0' y1='1' x2='1' y2='0'>
      <stop offset='0%' stop-color='#10b981'/>
      <stop offset='100%' stop-color='#22d3ee'/>
    </linearGradient>
    <filter id='s' x='-20%' y='-20%' width='140%' height='140%'>
      <feDropShadow dx='0' dy='2' stdDeviation='2' flood-color='#000' flood-opacity='0.45'/>
    </filter>
  </defs>
  <rect x='6' y='6' width='116' height='116' rx='24' fill='url(#g)'/>
  <!-- Candles -->
  <g opacity='0.9'>
    <rect x='26' y='66' width='8' height='22' rx='2' fill='#f59e0b'/>
    <rect x='46' y='54' width='8' height='34' rx='2' fill='#10b981'/>
    <rect x='66' y='60' width='8' height='28' rx='2' fill='#f43f5e'/>
    <rect x='86' y='40' width='8' height='48' rx='2' fill='#22d3ee'/>
  </g>
  <!-- Up arrow / line -->
  <path d='M22 86 L48 68 L62 72 L88 52 L106 58' fill='none' stroke='url(#a)' stroke-width='7' stroke-linecap='round' stroke-linejoin='round' filter='url(#s)'/>
  <polygon points='98,46 112,60 96,62' fill='url(#a)' filter='url(#s)'/>
  <!-- Confetti dots -->
  <circle cx='30' cy='34' r='4' fill='#e879f9'/>
  <circle cx='50' cy='26' r='3' fill='#60a5fa'/>
  <circle cx='92' cy='28' r='3' fill='#fde047'/>
  <circle cx='24' cy='98' r='2.5' fill='#34d399'/>
  <circle cx='106' cy='92' r='2.5' fill='#a78bfa'/>
</svg>
"""

DARK_CSS = """
:root{--bg:#0b0f14;--card:#111827;--muted:#9ca3af;--text:#e5e7eb;--accent:#10b981;--btn:#1f2937;--link:#93c5fd;--link-hover:#bfdbfe;--link-visited:#c4b5fd;--chip:#0c1220;}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,Arial;margin:24px}
.row{display:grid;grid-template-columns:1fr 1fr;gap:18px}
.card{background:var(--card);border:1px solid #1f2937;border-radius:14px;padding:20px;box-shadow:0 1px 2px rgba(0,0,0,.5)}
.muted{color:var(--muted);font-size:.9rem}
.logs a, .card a, body a{color:var(--link)}
.logs a:hover, .card a:hover, body a:hover{color:var(--link-hover)}
.logs a:visited, .card a:visited, body a:visited{color:var(--link-visited)}
.btn{background:var(--btn);color:var(--text);border:none;padding:10px 14px;border-radius:10px;cursor:pointer;transition:background .15s ease, box-shadow .15s ease, transform .15s ease, border-color .15s ease}
.btn.primary{background:var(--accent);color:#062d22;box-shadow:0 6px 20px rgba(16,185,129,0.25);font-weight:700}
.btn.primary:not(:disabled):hover{background:#22d3ee;box-shadow:0 8px 24px rgba(34,211,238,0.35);transform:translateY(-1px)}
.btn.secondary{background:#374151}
.btn.secondary:not(:disabled):hover{background:#475569;box-shadow:0 6px 18px rgba(71,85,105,0.3);transform:translateY(-1px)}
.btn.ghost{background:transparent;border:1px solid #1f2937;color:var(--muted)}
.btn.ghost:not(:disabled):hover{border-color:#334155;color:var(--text);box-shadow:0 6px 18px rgba(51,65,85,0.25);transform:translateY(-1px)}
.btn:disabled{opacity:0.6;cursor:not-allowed}
input[type=text]{padding:10px;border:1px solid #374151;background:#0f172a;color:var(--text);border-radius:10px}
input[type=number]{padding:10px;border:1px solid #374151;background:#0f172a;color:var(--text);border-radius:10px;width:90px}
input[type=number]::-webkit-outer-spin-button,
input[type=number]::-webkit-inner-spin-button{-webkit-appearance:none;margin:0}
input[type=number]{-moz-appearance:textfield}
.num{padding:10px;border:1px solid #374151;background:#0f172a;color:var(--text);border-radius:10px;width:90px}
input:disabled, input.disabled{opacity:0.55;background:#0b1324;color:#94a3b8;border-color:#263243;-webkit-text-fill-color:#94a3b8}
.field-spot input:disabled,
.field-spot input.disabled{opacity:0.7;background:#0b1324;color:#8a98ad;border-color:#263243;-webkit-text-fill-color:#8a98ad}
.field-spot input:disabled::placeholder,
.field-spot input.disabled::placeholder{color:#8a98ad;opacity:0.9}
.progress{height:8px;background:#111827;border-radius:999px;overflow:hidden}
.bar{height:100%;background:var(--accent);width:0%;transition:width .2s ease}
.progress-sm{height:6px;background:#111827;border:1px solid #334155;border-radius:999px;overflow:hidden;display:inline-block;width:140px;margin-left:10px;vertical-align:middle}
.bar-sm{height:100%;background:var(--accent);width:0%;transition:width .2s ease}
.logs{max-height:140px;overflow:auto;font-family:ui-monospace,Menlo,Consolas,monospace;background:#0f172a;padding:8px;border-radius:8px}
.logs.full{max-height:none;overflow:visible}
.badge{display:inline-block;padding:6px 10px;border-radius:12px;background:#0f172a;border:1px solid #1f2937;margin:0 8px 8px 0;cursor:pointer}
.badge:hover{border-color:#334155}
.alert{background:#7f1d1d;border:1px solid #b91c1c;color:#fee2e2;padding:10px;border-radius:10px}
.page-head{display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:12px}
.head-titles{display:flex;flex-direction:column;gap:4px}
.head-desc{color:var(--muted)}
.home-link{display:inline-flex;align-items:center;justify-content:center;width:40px;height:40px;border-radius:12px;border:1px solid #1f2937;background:#0f172a;color:var(--link);text-decoration:none;font-size:24px;line-height:1}
.home-link:hover{border-color:#334155;color:var(--link-hover);box-shadow:0 0 0 4px rgba(147,197,253,0.12)}
.home-link:focus-visible{outline:2px solid var(--link);outline-offset:3px}
.head-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-left:auto}
.drag-handle{cursor:grab;user-select:none;display:inline-flex;align-items:center;margin-right:8px;font-size:18px}
.drag-handle:active{cursor:grabbing}
.dragging{opacity:0.6}
.drag-over{outline:1px dashed #334155}
.scan-row{user-select:none}
.scan-controls{display:flex;flex-direction:column;gap:10px}
.scan-top{display:flex;gap:12px;align-items:flex-start;flex-wrap:wrap}
.scan-top-left{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
.scan-top-note{margin-top:-6px}
.scan-actions{margin-left:auto;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.chip-scroll{overflow-x:auto;padding:4px 2px}
.scan-chip{display:inline-flex;align-items:center;gap:6px;padding:8px 12px;border-radius:14px;background:var(--chip);border:1px solid #162032;margin:0 8px 8px 0;cursor:pointer}
.scan-chip:hover{border-color:#334155;box-shadow:0 4px 12px rgba(0,0,0,0.35)}
.scan-chip button{background:none;border:none;color:#9ca3af;cursor:pointer;font-size:14px;}
.scan-chip button:hover{color:#f87171;}
.chip-footer{display:flex;justify-content:flex-end;margin-top:4px}
.muted-small{color:#b4c5d6;font-size:0.85rem;}
.empty-hint{color:#cbd5e1}
.scan-table{width:100%;border-collapse:collapse;}
.scan-table tbody tr:nth-child(even){background:#0f172a;}
.scan-table tbody tr:hover{background:#132038;}
.ticker-form{display:flex;flex-direction:column;gap:8px}
.ticker-form .form-row{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
.meta-card{margin-top:10px}
.meta-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px;align-items:stretch}
.meta-tile{background:var(--chip);border:1px solid #162032;border-radius:12px;padding:12px 14px;display:flex;flex-direction:column;gap:6px;min-height:78px}
.meta-label{color:var(--muted);font-size:0.85rem;letter-spacing:0.01em}
.meta-value{font-weight:700;font-size:1.05rem;color:var(--text)}
.meta-subvalue{color:#b4c5d6;font-size:0.9rem}
.head-chip{display:inline-flex;align-items:center;padding:6px 10px;border-radius:999px;background:var(--chip);border:1px solid #162032;font-size:0.9rem;color:#cbd5e1}
:root{--radius-sm:10px;--radius:12px;--radius-lg:14px;--border-soft:#1f2937;--border-strong:#334155;--input-bg:#0f172a;--input-border:#374151;--focus-ring:0 0 0 3px rgba(148,163,184,0.18)}
body{line-height:1.5}
h1,h2,h3{margin:0;font-weight:700;letter-spacing:0.01em}
h1{font-size:1.6rem}
h2{font-size:1.35rem}
h3{font-size:1.05rem}
.card h3{margin-bottom:12px}
.card-title{font-weight:700;font-size:1.08rem;margin:0 0 14px 0}
.card-subtext{margin:0 0 16px 0}
.pill-input{border-radius:var(--radius-sm);border:1px solid var(--input-border);background:var(--input-bg);padding:10px 12px;color:var(--text);width:100%;min-height:44px;transition:border-color .15s ease, box-shadow .15s ease}
.pill-input:focus{outline:none;border-color:#64748b;box-shadow:var(--focus-ring)}
.pill-input.disabled{opacity:0.55}
.select-input{appearance:none;-moz-appearance:none;-webkit-appearance:none;background:var(--input-bg);color:var(--text);border:1px solid var(--input-border);border-radius:var(--radius-sm);padding:8px 34px 8px 12px;min-height:40px;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 10 6'%3E%3Cpath fill='%2394a3b8' d='M0 0l5 6 5-6z'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 10px center;background-size:10px 7px}
.select-input:focus{outline:none;border-color:#64748b;box-shadow:var(--focus-ring)}
.select-input:disabled{opacity:0.5;cursor:not-allowed;border-color:var(--border-soft);color:#94a3b8}
.select-input.select-sm{min-height:32px;padding:4px 28px 4px 10px;font-size:0.85rem}
.suffix-input{position:relative;display:inline-flex;align-items:center}
.suffix-input input{padding-right:26px;height:44px}
.suffix-input .suffix{position:absolute;right:8px;top:50%;transform:translateY(-50%);color:#94a3b8;font-size:0.9rem;pointer-events:none}
.control-head{display:flex;flex-direction:column;gap:6px;margin-bottom:6px;min-width:0}
.control-head .muted{margin:0}
.control-subrow{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.control-row{display:flex;flex-wrap:wrap;gap:10px;align-items:center}
.field-ticker{flex:0 0 140px;min-width:140px;max-width:140px}
.field-ticker input{width:100%}
.field-window{flex:0 0 100px;min-width:100px;max-width:100px}
.field-window .suffix-input{width:100%}
.field-window .num{width:100%}
.field-spot{flex:0 0 120px;min-width:120px;max-width:120px}
.field-spot input{width:100%}
.spot-toggle{display:flex;align-items:center;gap:8px;margin:0;white-space:nowrap}
.control-row input[type=text], .control-row input[type=number]{height:44px}
.spot-row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:6px}
.field-group{display:flex;flex-direction:column;gap:6px}
.flip-mode{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:4px}
.flip-mode .btn{min-height:44px}
.flip-mode .btn.flip-active{background:#006d75;color:#e6f7fb;border:1px solid #0095a4;box-shadow:0 6px 18px rgba(0,109,117,0.35)}
.flip-mode .btn.flip-active:not(:disabled):hover{background:#00838e;box-shadow:0 8px 22px rgba(0,131,142,0.4)}
.options-grid{display:flex;gap:18px;align-items:stretch;flex-wrap:wrap;width:100%}
.options-grid > .card{flex:1 1 280px;min-width:240px;min-height:0}
.options-grid .options-card{flex:1 1 320px}
.options-grid .view-card{flex:0.85 1 240px}
.options-grid .ticker-details{flex:1.2 1 360px}
.options-card,.view-card,.ticker-details{display:flex;flex-direction:column;gap:10px}
.options-card .card-title,.view-card .card-title,.ticker-details .card-title{margin-bottom:0}
.options-card .card-subtext{margin:0}
.options-card .control-subrow{margin:0}
.options-card .control-row{margin:0}
.options-card .spot-row{margin:0}
.options-card .form-row{margin:0}
.action-row{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:0}
.view-card .field-group{margin:0}
.view-card .form-row{margin:0}
.action-row .progress-sm{margin-left:0}
@media(max-width:900px){.options-grid{flex-direction:column}}
@media(max-width:900px){.control-row{flex-wrap:wrap}}
@media(max-width:700px){.control-subrow{flex-wrap:wrap}}
"""


# Jinja2 templates
BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def render_template(name: str, request, context: dict | None = None, status_code: int = 200) -> Any:
    ctx = {"request": request, "title": "Dave's Stock Tools", "dark_css": DARK_CSS}
    if context:
        ctx.update(context)
    return templates.TemplateResponse(name, ctx, status_code=status_code)


# Defaults for client-side favorites/scanner (also exposed in JS)
FALLBACK_FAVS = ["SPX", "SPY", "QQQ", "NVDA", "AAPL"]
SCANNER_DEFAULTS = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA"]
D_SCANNER_CONCURRENCY = 8  # default fan-out for scanner; override via env SCANNER_CONCURRENCY


def scanner_max_workers(total_symbols: int) -> int:
    try:
        conf = int(os.getenv("SCANNER_CONCURRENCY", str(D_SCANNER_CONCURRENCY)))
    except Exception:
        conf = D_SCANNER_CONCURRENCY
    return max(1, min(total_symbols, conf))


def sort_scanner_results(
    results, tickers=None, sort_mode: str = "score", sort_key: str | None = None, sort_dir: int = 1
):
    """
    Shared helper to mirror the client-side sorting rules for scanner rows.

    Args:
        results: Iterable of result dicts with at least a "symbol".
        tickers: Optional sequence describing the requested symbol order.
        sort_mode: One of "natural", "asc", "desc", or "score" (default abs(score) desc).
        sort_key: Field name to sort on when mode is asc/desc.
        sort_dir: Direction for numeric sorts (1 asc, -1 desc).
    """
    items = list(results or [])
    mode = (sort_mode or "score").lower()
    # Natural order follows the user-specified ticker list
    if mode == "natural":
        order = {}
        for idx, t in enumerate(tickers or []):
            sym = t
            if isinstance(t, (list, tuple)) and t:
                sym = t[0]
            if isinstance(t, dict) and "symbol" in t:
                sym = t.get("symbol")
            key = str(sym or "").upper()
            if key and key not in order:
                order[key] = idx
        return sorted(
            items,
            key=lambda r: order.get(str(r.get("symbol") or "").upper(), 10**9),
        )

    # Asc/desc numeric sorts on a particular field
    if sort_key:

        def _coerce(val):
            try:
                return float(val)
            except Exception:
                return None

        numeric = []
        missing = []
        for it in items:
            v = _coerce(it.get(sort_key))
            if v is None:
                missing.append(it)
            else:
                numeric.append((v, it))
        reverse = mode == "desc" or (sort_dir or 1) < 0
        numeric.sort(key=lambda x: x[0], reverse=reverse)
        ordered = [it for _, it in numeric]
        ordered.extend(missing)
        return ordered

    # Default: score sort by absolute value, descending
    def _score(item):
        try:
            return abs(float(item.get("score", 0)))
        except Exception:
            return 0.0

    return sorted(items, key=_score, reverse=True)
