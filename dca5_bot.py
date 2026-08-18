#!/usr/bin/env python3
"""
DCA5-Bot — Multi-Symbol 90-Day DCA Short Bot, priced and sized at rolling 9-day high

Single-process, single-machine bot for Fly.io.

Behavior:

On startup: run a one-time TEST order (limit short at market+10%,
held 60s, then cancelled if unfilled) against EVERY symbol in
SYMBOLS, one at a time, sequentially, to validate the signing and
order-placement/cancellation path for each symbol's own specs
(tick size, min size, max leverage) before the real engine loop
begins. Each symbol's test is independent — one symbol failing its
test does not stop the others from being tested or stop the engine
loop from starting afterward. This adds ~len(SYMBOLS) x
TEST_ORDER_WAIT_SEC to startup time (currently ~17 minutes for 17
symbols at 60s each) — unavoidable if each symbol's fill/cancel
path is to be genuinely validated rather than assumed.

The test order is sized at each symbol's own minimum order volume
(not a fixed USD amount) — this is the smallest possible live
order for that symbol, chosen to minimize capital exposure and
market impact from what is purely a validation trade.

Every hour on the hour: refresh mark prices + rolling 9d highs for
all symbols and refresh the in-memory SVG status table.

Only at hour == 00 UTC: for each symbol, if today's calendar date
falls within that symbol's 90-day DCA window (per-symbol start
dates hardcoded below) AND that symbol has not already fired today
(per persisted fire-history), add that day's slice (budget / 90)
to a persisted per-symbol USD accumulator. If the accumulator is
now large enough to meet that symbol's minimum contract size at
the current trailing 9-day high, a limit SHORT is placed for the
FULL accumulated amount and the accumulator resets to zero. If not
yet large enough, no order is placed today, but the day is still
recorded as fired (its slice has been queued into the
accumulator) so the daily budget is never silently dropped.

Sizing uses the 9d-high price (the limit price itself), NOT mark.
Reasoning: a resting limit short fills at its limit price or
better, essentially never at mark (mark is only relevant to
orders that trade immediately) — the 9d-high is the ONLY price
this order will actually transact at if it fills at all. Sizing
off mark while pricing off the 9d-high would guarantee every fill
is worth more than the target slice (by exactly the ratio of
9d-high/mark), which defeats the purpose of a fixed daily dollar
slice. Sizing off the 9d-high itself makes the notional-at-fill
exactly the accumulated slice amount, which is the correct target
for a DCA program. Mark is still fetched and logged for
visibility/context, just not used for sizing.

The order is left open with no timeout — if a previous day's
order for that symbol is still unfilled, it is left resting and a
new order is placed on top of it (orders stack; nothing is ever
cancelled by the daily engine).

Pricing every day's DCA slice at the rolling 9d high (rather than
at mark) is a deliberate choice: it reaches for a better-than-
market short price every day, and naturally scales with each
symbol's own volatility (a choppier symbol's 9d high sits further
above mark without any separate volatility calculation needed).
The tradeoff is that in a sustained uptrend the 9d high chases
price upward and fills may not be much better than mark, and
resting orders may take a long time (or never) to fill.

If a symbol's daily klines can't be fetched or return fewer than
ROLL_DAYS closed bars on a given midnight wake, that symbol is
skipped for the day entirely (not fired, not marked as fired, and
its slice is NOT added to the accumulator) rather than falling
back to any placeholder price. It will be retried at the next
midnight wake.

A restart cannot double-fire (double-queue) a given symbol on a
given UTC date: fire history (symbol -> list of ISO dates already
fired) is persisted to a local JSON file and checked before every
fire. The per-symbol pending_usd accumulator is persisted
alongside it. This is the persisted state in this script; a DCA
schedule is not self-healing from exchange state alone the way a
pure rolling-high trigger is, so this file is required for
correctness across restarts.

Every placed order is logged (id, symbol, price, usd, contracts)
and recorded into the same local JSON state file alongside fire
history, so open orders can be cross-checked against MEXC's
open-orders API at any time (see / status page, /orders.json, and
logs).

LEVERAGE HANDLING: a single default leverage (LEVERAGE_DEFAULT) is
requested for every symbol UNLESS that symbol's own exchange-
reported maxLeverage (from /api/v1/contract/detail, loaded into
specs by load_specs) is lower than the default, in which case the
symbol's maxLeverage is used instead. This is resolved once per
symbol at startup (see load_specs / effective_leverage) and applied
identically to both the startup test order and every daily DCA
order — no order is ever submitted requesting more leverage than
the exchange allows for that symbol.

No CLI arguments. No config files beyond the state file (which stores
history/records, not config). No web UI for configuration. All
parameters are hardcoded constants below. A second thread runs a
small public HTTP server that exposes the current status table as an
SVG and a minimal HTML wrapper page.

Environment (secrets only, not behavior):
MEXC - MEXC API key
MEXCSECRET - MEXC API secret

CONFIRMED VIA LIVE DIAGNOSTIC RUNS (carried over from the original
SP9H-Bot's diagnostics — same account/endpoints, so still applicable):

Kline endpoint returns data as a dict of parallel arrays, keyed by
field name, with 'real*' fields for actual traded OHLC, timestamps
in Unix seconds (confirmed via fetch_mexc_klines.py).

Open-orders endpoint's symbol query param is NOT a reliable
server-side filter — it returns orders across ALL symbols regardless
of the param. This script always filters open orders by symbol
client-side before matching/counting (confirmed via
fetch_mexc_open_orders.py and test_order_flow.py).

The order-create response's "data" field is a DICT shaped like
{"orderId": "...", "ts": ...} — NOT a bare order id. Every place
that extracts an order id from a create-order response must read
data["orderId"], not data itself (confirmed via test_order_flow.py,
where the earlier bug was traced to this exact mismatch).

CONFIRMED VIA LIVE SYMBOL-SNIFF (standalone diagnostic script run
against /api/v1/contract/detail, results reviewed manually):

All eight new equity-proxy symbols use a "STOCK" suffix on the base
coin: BABASTOCK_USDT, BIDUSTOCK_USDT, JDSTOCK_USDT (NOT JD_USDT —
this was corrected after JD_USDT returned "not found"),
XIAOMISTOCK_USDT, ZHONGJISTOCK_USDT, ZHIPUSTOCK_USDT,
ENFLAMESTOCK_USDT, and CXMTSTOCK_USDT. Their maxLeverage values are
NOT uniform: BABASTOCK_USDT and JDSTOCK_USDT allow up to 100x,
CXMTSTOCK_USDT up to 50x, and BIDUSTOCK_USDT, XIAOMISTOCK_USDT,
ZHIPUSTOCK_USDT, ZHONGJISTOCK_USDT, and ENFLAMESTOCK_USDT are
capped at 20x — well below this script's LEVERAGE_DEFAULT of 30x,
which is why per-symbol leverage clamping (see above) is required
for those five symbols specifically.

NOT YET CONFIRMED FOR THIS VERSION:

Contract specs (tick size, min size, contract size) for BTC_USDT,
ETH_USDT, SOL_USDT, XRP_USDT, NAS100_USDT, COPPER_USDT,
SILVER_USDT, and XAU_USDT are fetched the same way as SPX500_USDT
via load_specs(), and the startup test order now exercises the
full place/cancel path for each of them individually (see
run_startup_test_orders below).

The daily-kline shape (real* fields, Unix-seconds timestamps) is
assumed to hold across all symbols; only directly confirmed for
SPX500_USDT in the original script's diagnostics.

The minimum-contract-size accumulation logic (see run_daily_dca
and the module docstring) is new in this version and has not been
exercised against live fills; verify accumulator behavior against
/orders.json and the status page after the first few accumulation
cycles, particularly for the lowest-priced-per-contract symbols
where multi-day accumulation is most likely to occur.
"""

import datetime
import hashlib
import hmac
import http.server
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── constants (all hardcoded — no argparse, no config files) ─────────────────

UTC = datetime.timezone.utc

MEXC_KEY = os.getenv("MEXC")
MEXC_SECRET = os.getenv("MEXCSECRET")
MEXC_BASE = "https://api.mexc.co"

# Symbols in this DCA program. The startup test order validates EVERY one.
SYMBOLS = [
    "SPX500_USDT", "BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT",
    "NAS100_USDT", "SILVER_USDT", "XAU_USDT",
]

# Default leverage requested for every symbol UNLESS the exchange's own
# maxLeverage for that symbol (loaded into specs at startup) is lower, in
# which case the symbol's maxLeverage is used instead. See
# effective_leverage() below. Resolved once per symbol after load_specs()
# runs, then applied to every order (test and DCA) for that symbol.
LEVERAGE_DEFAULT = 30

ROLL_DAYS = 9  # trailing window for the 9-day-high price target, including today

# ── DCA schedule ───────────────────────────────────────────────────────────

# Each symbol gets its own total budget split evenly across DCA_DAYS daily
# slices. Aug 1 2026 -> Oct 29 2026 inclusive is 90 UTC calendar days.
DCA_DAYS = 90

# Per-symbol total budget in USD. The original nine symbols share the
# original $1000 budget; the eight new equity-proxy symbols are $125 each
# per the latest spec.
SYMBOL_BUDGET_USD: Dict[str, float] = {
    "SPX500_USDT": 1000.0,
    "BTC_USDT": 1000.0,
    "ETH_USDT": 1000.0,
    "SOL_USDT": 1000.0,
    "XRP_USDT": 1000.0,
    "NAS100_USDT": 1000.0,
    "COPPER_USDT": 1000.0,
    "SILVER_USDT": 1000.0,
    "XAU_USDT": 1000.0,
    "BABASTOCK_USDT": 125.0,
    "BIDUSTOCK_USDT": 125.0,
    "JDSTOCK_USDT": 125.0,
    "XIAOMISTOCK_USDT": 125.0,
    "ZHONGJISTOCK_USDT": 125.0,
    "ZHIPUSTOCK_USDT": 125.0,
    "ENFLAMESTOCK_USDT": 125.0,
    "CXMTSTOCK_USDT": 125.0,
}

def daily_slice_usd(sym: str) -> float:
    return SYMBOL_BUDGET_USD[sym] / DCA_DAYS

# Per-symbol start date (UTC calendar date). All symbols share the same
# 90-day window here (Aug/Sep/Oct 2026) per spec, but this is per-symbol
# so schedules can be staggered later without restructuring the code.
DCA_START_DATE: Dict[str, datetime.date] = {
    sym: datetime.date(2026, 8, 1) for sym in SYMBOLS
}

def in_dca_window(sym: str, d: datetime.date) -> bool:
    start = DCA_START_DATE[sym]
    end = start + datetime.timedelta(days=DCA_DAYS - 1)
    return start <= d <= end

HOURLY_SLEEP_FLOOR_SEC = 5  # small buffer after top-of-hour before waking

TEST_ORDER_PREMIUM = 1.10  # market + 10%
TEST_ORDER_WAIT_SEC = 60
# NOTE: test orders are sized at each symbol's minimum contract size
# (see run_startup_test_order_for), not a fixed USD amount.

HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(os.getenv("PORT", "8080"))  # Fly.io injects PORT; not a behavior param

# Persistence: fire history, per-symbol pending-USD accumulator, AND a
# record of every placed order, so resting/stacked orders can be
# cross-checked against MEXC's own open-orders API without relying on
# memory alone across restarts.
STATE_FILE = os.getenv("DCA_STATE_FILE", "/data/dca_fire_history.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()

specs: Dict[str, Dict] = {}

# Resolved per-symbol leverage (min(LEVERAGE_DEFAULT, symbol maxLeverage)),
# populated once by load_specs(). Read via effective_leverage(sym).
_leverage_by_symbol: Dict[str, int] = {}

# ── shared, lock-guarded state between engine thread and server thread ───────

class SharedState:
    def __init__(self):
        self._lock = threading.Lock()
        self._svg = "<svg xmlns='http://www.w3.org/2000/svg'><text x='10' y='20'>Initializing...</text></svg>"
        self._status = "initializing"

    def set_svg(self, svg: str):
        with self._lock:
            self._svg = svg

    def get_svg(self) -> str:
        with self._lock:
            return self._svg

    def set_status(self, status: str):
        with self._lock:
            self._status = status

    def get_status(self) -> str:
        with self._lock:
            return self._status

STATE = SharedState()

# ── persisted state: fire history + pending accumulator + order records ─────

def _default_state() -> Dict:
    return {"fired": {}, "pending_usd": {}, "orders": []}

def load_state() -> Dict:
    """Load {"fired": {symbol: [iso_date, ...]}, "pending_usd":
    {symbol: float}, "orders": [record, ...]} from STATE_FILE. Missing
    or corrupt file -> fresh empty state (never crashes startup over
    this)."""
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("state file did not contain a dict")
        data.setdefault("fired", {})
        data.setdefault("pending_usd", {})
        data.setdefault("orders", [])
        return data
    except FileNotFoundError:
        log.info(f"no state file at {STATE_FILE} — starting fresh")
        return _default_state()
    except Exception as e:
        log.error(f"state file at {STATE_FILE} unreadable ({e}) — starting fresh")
        return _default_state()

def save_state(state: Dict):
    try:
        os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.error(f"failed to persist state to {STATE_FILE}: {e}")

STATE_DATA: Dict = load_state()

def has_fired_today(sym: str, d: datetime.date) -> bool:
    return d.isoformat() in STATE_DATA["fired"].get(sym, [])

def get_pending_usd(sym: str) -> float:
    return float(STATE_DATA["pending_usd"].get(sym, 0.0))

def set_pending_usd(sym: str, value: float):
    STATE_DATA["pending_usd"][sym] = value

def mark_queued(sym: str, d: datetime.date):
    """Record that today's slice has been queued into the accumulator
    for sym, without necessarily placing an order (used whether or not
    the accumulator was large enough to fire this cycle)."""
    STATE_DATA["fired"].setdefault(sym, []).append(d.isoformat())
    save_state(STATE_DATA)

def record_order(order_record: Dict):
    STATE_DATA["orders"].append(order_record)
    save_state(STATE_DATA)

def fired_count(sym: str) -> int:
    return len(STATE_DATA["fired"].get(sym, []))

# ── http helpers ──────────────────────────────────────────────────────────────

def _http(method, url, headers=None, data=None, params=None):
    if params:
        url += "?" + urllib.parse.urlencode(sorted(params.items()))
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
    except urllib.error.HTTPError as e:
        body = e.read()
    return json.loads(body) if body.strip() else {}

def _get(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())

# ── mexc signed requests (futures/contract, used for specs + orders) ─────────

def mexc(method, endpoint, params=None, body=None):
    params = params or {}
    ts = str(int(time.time() * 1000))
    sp = ("&".join(f"{k}={v}" for k, v in sorted(params.items()))
          if method == "GET"
          else (json.dumps(body, separators=(",", ":"), sort_keys=True) if body else ""))
    sig = hmac.new(MEXC_SECRET.encode(), (MEXC_KEY + ts + sp).encode(), hashlib.sha256).hexdigest()
    hdr = {"ApiKey": MEXC_KEY, "Request-Time": ts, "Signature": sig,
           "Content-Type": "application/json", "Accept": "application/json"}
    raw = (json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
           if body and method not in ("GET", "DELETE") else None)
    try:
        return _http(method, MEXC_BASE + endpoint, headers=hdr, data=raw,
                      params=params if method in ("GET", "DELETE") else None)
    except Exception as e:
        log.error(f"mexc {method} {endpoint}: {e}")
        return {}

# ── specs / sizing ─────────────────────────────────────────────────────────────

def load_specs():
    """Fetch the contract list once and populate specs for every symbol
    in SYMBOLS. Exits the process if ANY symbol is not found — no
    silent fallback, since mis-sizing a real order is worse than not
    starting. Also resolves each symbol's effective leverage as
    min(LEVERAGE_DEFAULT, that symbol's exchange-reported maxLeverage)
    into _leverage_by_symbol, so no order is ever built requesting more
    leverage than MEXC allows for that specific contract."""
    rows = mexc("GET", "/api/v1/contract/detail").get("data") or []
    if not rows:
        log.error("empty contract detail response from MEXC")
        raise SystemExit(1)

    by_sym = {c.get("symbol", "").upper(): c for c in rows}
    missing = [s for s in SYMBOLS if s not in by_sym]
    if missing:
        log.error(f"symbols not found in MEXC contract detail: {missing}")
        raise SystemExit(1)

    for sym in SYMBOLS:
        match = by_sym[sym]
        vu = float(match.get("volUnit", 1))
        pu = float(match.get("priceUnit", 0.5))
        cs = float(match.get("contractSize", vu))
        max_lev = match.get("maxLeverage")
        raw = f"{vu:.10f}".rstrip("0")
        p = len(raw.split(".")[1]) if "." in raw else 0
        specs[sym] = {"p": p, "t": pu, "vu": vu, "cs": cs, "max_lev": max_lev}

        if max_lev is not None and max_lev < LEVERAGE_DEFAULT:
            resolved = int(max_lev)
            log.warning(f"[{sym}] exchange maxLeverage={max_lev} is below "
                        f"LEVERAGE_DEFAULT={LEVERAGE_DEFAULT} — clamping this "
                        f"symbol's orders to {resolved}x")
        else:
            resolved = LEVERAGE_DEFAULT
        _leverage_by_symbol[sym] = resolved

        log.info(f"loaded specs for {sym}: {specs[sym]} effective_leverage={resolved}")

def effective_leverage(sym: str) -> int:
    """The leverage to actually request for sym: LEVERAGE_DEFAULT, or
    the symbol's exchange-reported maxLeverage if that is lower.
    Resolved once in load_specs(); falls back to LEVERAGE_DEFAULT if
    called before load_specs() has run (should not happen in normal
    operation)."""
    return _leverage_by_symbol.get(sym, LEVERAGE_DEFAULT)

def _tick(sym):
    return specs.get(sym, {}).get("t", 0.5)

def _prec(sym):
    return specs.get(sym, {}).get("p", 0)

def _rfmt_price(sym, v):
    t = _tick(sym)
    r = round(v / t) * t
    s = f"{t:.10f}".rstrip("0")
    dec = len(s.split(".")[1]) if "." in s else 0
    return f"{r:.{dec}f}"

def _rfmt_vol(sym, v):
    p = _prec(sym)
    if p >= 0:
        return f"{round(v, p):.{p}f}"
    d = 10 ** abs(p)
    return str(int(round(v / d) * d))

def _contracts(sym, usd, price):
    """Contract count so that usd dollars of notional trades at
    price. Caller must pass the price the order will actually
    transact at (the limit price for a resting limit order) — NOT
    mark — since notional-at-fill = contracts x contract_size x
    (fill price), and a limit order fills at its limit price."""
    cs = specs.get(sym, {}).get("cs", 1.0)
    return float(_rfmt_vol(sym, max(0, usd / (cs * price))))

def _mos(sym):
    return specs.get(sym, {}).get("vu", 1.0)

def _min_usd_for_min_size(sym: str, price: float) -> float:
    """The minimum USD notional required to reach this symbol's
    minimum order size at the given price. Used to decide whether
    the accumulated pending_usd is large enough to fire yet."""
    cs = specs.get(sym, {}).get("cs", 1.0)
    return _mos(sym) * cs * price

# ── orders (short/sell-to-open) ───────────────────────────────────────────────

def _open_orders_for_sym(sym: str) -> List[Dict]:
    """Fetch the open-orders list and filter to sym client-side.
    The 'symbol' query param is NOT a reliable server-side filter on
    this MEXC endpoint — confirmed to return open orders across ALL
    symbols regardless of the param — so filtering here is mandatory."""
    data = mexc("GET", "/api/v1/private/order/list/open_orders",
                 params={"symbol": sym, "page_num": 1, "page_size": 100}).get("data") or []
    if isinstance(data, dict):
        data = data.get("resultList", [])
    return [o for o in data if o.get("symbol", "").upper() == sym]

def _open_ids(sym: str) -> set:
    return {str(o.get("orderId", "")) for o in _open_orders_for_sym(sym)}

def place_short(sym: str, limit_price: float, sizing_price: float, usd_amount: float) -> Optional[str]:
    """Place a limit SHORT (sell-to-open) order for usd_amount dollars
    on sym at limit_price, sized using sizing_price (the price
    used to compute contract count — pass the price the order will
    actually transact at, e.g. limit_price itself for a resting DCA
    order, or mark for the startup test order which is deliberately
    priced away from mark and not expected to fill). Leverage is
    resolved per-symbol via effective_leverage(sym) — never a flat
    LEVERAGE_DEFAULT — so this never requests more leverage than the
    exchange allows for sym. Left open indefinitely (no timeout, never
    auto-cancelled by the daily engine — it stacks with any prior
    unfilled order for the same symbol). Returns order id, 'SKIP' if
    below minimum size, or None on rejection.

    NOTE: side=3 assumes MEXC's contract convention 1=open-long,
    2=close-short, 3=open-short, 4=close-long — confirmed correct via
    live test_order_flow.py run (original SP9H-Bot diagnostics). The
    create-order response's "data" field is a DICT shaped like
    {"orderId": "...", "ts": ...} — the order id must be extracted via
    data["orderId"]."""
    vol = _contracts(sym, usd_amount, sizing_price)
    if vol < _mos(sym):
        log.warning(f"[{sym}] size {vol} < min {_mos(sym)} (${usd_amount:.2f}) — order skipped")
        return "SKIP"
    lev = effective_leverage(sym)
    body = {
        "leverage": lev,
        "openType": 2,
        "positionMode": 1,
        "price": _rfmt_price(sym, limit_price),
        "side": 3,
        "symbol": sym,
        "type": 1,
        "vol": _rfmt_vol(sym, vol),
    }
    r = mexc("POST", "/api/v1/private/order/create", body=body)
    if not r.get("success"):
        log.error(f"[{sym}] short order rejected: {r}")
        return None
    data = r.get("data") or {}
    if not isinstance(data, dict):
        log.error(f"[{sym}] unexpected 'data' shape from order/create: {data!r}")
        return None
    oid = data.get("orderId")
    if not oid:
        log.error(f"[{sym}] order/create succeeded but no 'orderId' in data: {data!r}")
        return None
    oid = str(oid)
    log.info(f"[{sym}] limit SHORT {_rfmt_vol(sym, vol)} @ {_rfmt_price(sym, limit_price)} "
              f"id={oid} usd={usd_amount:.2f} sizing_price={sizing_price:.4f} leverage={lev}x")
    return oid

def cancel_order(sym: str, oid: str) -> bool:
    """Cancel a single open order by ID, scoped to sym only. Used
    only by the startup test orders — the daily DCA engine never
    cancels its own orders."""
    body = [oid]  # MEXC cancel endpoint accepts a list of order IDs
    r = mexc("POST", "/api/v1/private/order/cancel", body=body)
    ok = bool(r.get("success"))
    if ok:
        log.info(f"[{sym}] cancelled order id={oid}")
    else:
        log.error(f"[{sym}] cancel failed for id={oid}: {r}")
    return ok

def is_filled(sym: str, oid: str) -> bool:
    return oid not in _open_ids(sym)

def get_mark(sym: str) -> float:
    d = mexc("GET", "/api/v1/contract/ticker", params={"symbol": sym}).get("data") or {}
    return float(d.get("fairPrice", d.get("lastPrice", 0)) or 0)

# ── daily klines + rolling 9d high ────────────────────────────────────────────

def fetch_daily_bars(sym: str, lookback_days: int) -> List[Dict]:
    """Fetch closed daily candles from MEXC's public futures kline
    endpoint for sym. Response 'data' is a dict of parallel arrays
    keyed by field name. Uses the 'real*' fields (realHigh etc.), which
    reflect actual traded/index price rather than mark-price OHLC.
    Excludes the still-open, unclosed current daily bar."""
    now_s = int(time.time())
    start_s = now_s - (lookback_days + 2) * 86400
    url = (f"{MEXC_BASE}/api/v1/contract/kline/{sym}"
           f"?interval=Day1&start={start_s}&end={now_s}")
    try:
        raw = _get(url)
    except Exception as e:
        log.error(f"[{sym}] daily kline fetch failed: {e}")
        return []

    if not raw.get("success"):
        log.error(f"[{sym}] daily kline fetch unsuccessful: {raw}")
        return []
    d = raw.get("data") or {}
    times = d.get("time") or []
    highs = d.get("realHigh") or d.get("high") or []
    bars = []
    for i in range(len(times)):
        t_s = int(times[i])
        if t_s + 86400 > now_s:
            continue  # exclude still-open current daily bar
        bars.append({"t": t_s * 1000, "h": float(highs[i])})
    bars.sort(key=lambda b: b["t"])
    return bars

def rolling_9d_high(sym: str) -> Optional[float]:
    """Max high of the trailing ROLL_DAYS closed daily bars for sym,
    including the most recently closed bar. Returns None if fewer than
    ROLL_DAYS closed bars are available — caller must skip the symbol
    for the day rather than substitute a placeholder."""
    bars = fetch_daily_bars(sym, ROLL_DAYS + 3)
    if len(bars) < ROLL_DAYS:
        log.error(f"[{sym}] only {len(bars)} closed daily bars available, need {ROLL_DAYS} — cannot compute 9d high")
        return None
    window = bars[-ROLL_DAYS:]
    return max(b["h"] for b in window)

# ── startup test orders (ALL symbols) ─────────────────────────────────────────

def run_startup_test_order_for(sym: str):
    """One-time validation for a single symbol: places a limit SHORT at
    mark price + 10% (chosen to be unlikely to fill immediately), sized
    at this symbol's minimum order size (the smallest possible live
    order, to minimize capital exposure from what is purely a
    validation trade), at this symbol's effective (leverage-clamped)
    leverage, waits TEST_ORDER_WAIT_SEC, then cancels it if still open.
    This test validates the signing/place/cancel path and this
    symbol's specs (tick size, min size, max leverage), not trading
    logic."""
    log.info(f"── startup test order [{sym}]: begin ──────────────────")
    try:
        mark = get_mark(sym)
        if mark <= 0:
            log.error(f"[{sym}] test order aborted: invalid mark price ({mark})")
            return

        test_price = mark * TEST_ORDER_PREMIUM
        min_vol = _mos(sym)
        cs = specs.get(sym, {}).get("cs", 1.0)
        test_usd = min_vol * cs * test_price
        lev = effective_leverage(sym)
        log.info(f"[{sym}] test order: mark={mark:.4f} limit={test_price:.4f} "
                  f"(+{(TEST_ORDER_PREMIUM-1)*100:.0f}%) min_vol={min_vol} "
                  f"est_usd={test_usd:.2f} leverage={lev}x")
        oid = place_short(sym, test_price, mark, test_usd)
        if oid == "SKIP":
            log.warning(f"[{sym}] test order skipped — computed size still below minimum; "
                        f"sizing/signing path could not be fully validated")
            return
        if oid is None:
            log.error(f"[{sym}] test order was rejected by MEXC — see above for details")
            return
        log.info(f"[{sym}] test order placed id={oid} — waiting {TEST_ORDER_WAIT_SEC}s before checking fill/cancel")
        time.sleep(TEST_ORDER_WAIT_SEC)
        if is_filled(sym, oid):
            log.warning(f"[{sym}] test order id={oid} FILLED during the {TEST_ORDER_WAIT_SEC}s wait — "
                        f"this is now a real open short position, not a no-op. "
                        f"No cancellation is possible for a filled order; review your "
                        f"MEXC position manually if this was unexpected.")
        else:
            cancelled = cancel_order(sym, oid)
            if cancelled:
                log.info(f"[{sym}] test order id={oid} cancelled successfully — order-placement "
                          f"and cancellation path validated")
            else:
                log.error(f"[{sym}] test order id={oid} could not be cancelled — it may still be "
                          f"open; check MEXC manually")
    except Exception as e:
        log.error(f"[{sym}] startup test order failed: {e}", exc_info=True)
    log.info(f"── startup test order [{sym}]: end ─────────────────────")

def run_startup_test_orders():
    """Run the startup test order for every symbol in SYMBOLS,
    sequentially. Each symbol's test is fully independent — a failure
    or exception for one symbol is logged and does not prevent the
    remaining symbols from being tested, nor does it prevent the
    engine loop from starting once all tests have run."""
    log.info(f"══ startup test orders: {len(SYMBOLS)} symbols, ~{TEST_ORDER_WAIT_SEC}s each ══")
    for sym in SYMBOLS:
        run_startup_test_order_for(sym)
    log.info("══ startup test orders: all symbols done ══")

# ── DCA trigger logic ─────────────────────────────────────────────────────────

def run_daily_dca(now_utc: datetime.datetime):
    """For each symbol whose 90-day window includes today and which
    hasn't already fired (queued) today (per persisted fire history):
    compute the current trailing 9-day high, add today's slice
    (budget/90) to that symbol's persisted pending_usd accumulator,
    and mark today as fired (the day's obligation has been queued
    regardless of whether an order fires this cycle). If the
    accumulator is now large enough to reach the symbol's minimum
    contract size at the 9d-high price, place a limit SHORT for the
    FULL accumulated amount (sized off the SAME 9d-high price, at the
    symbol's effective/clamped leverage — see place_short docstring /
    module docstring for why) and reset the accumulator to zero. Never
    cancels anything — a prior day's unfilled order is left resting
    and any new order stacks on top of it."""
    today = now_utc.date()
    for sym in SYMBOLS:
        if not in_dca_window(sym, today):
            continue
        if has_fired_today(sym, today):
            log.info(f"[{sym}] DCA: already fired for {today.isoformat()} — skipping")
            continue

        mark = get_mark(sym)
        if mark <= 0:
            log.error(f"[{sym}] DCA: invalid mark price ({mark}) — skipping today, will retry next midnight wake only")
            continue

        target = rolling_9d_high(sym)
        if target is None:
            log.error(f"[{sym}] DCA: could not compute 9d high — skipping today, will retry next midnight wake only")
            continue

        slice_usd = daily_slice_usd(sym)
        accumulated = get_pending_usd(sym) + slice_usd
        min_usd_needed = _min_usd_for_min_size(sym, target)

        log.info(f"[{sym}] DCA queue {today.isoformat()}: slice=${slice_usd:.2f} "
                  f"accumulated=${accumulated:.2f} min_needed=${min_usd_needed:.2f} "
                  f"9dHigh={target:.4f} mark={mark:.4f} leverage={effective_leverage(sym)}x")

        if accumulated < min_usd_needed:
            # Not yet enough to place an order — queue the slice and
            # mark today as fired so it is never double-added, but
            # place no order this cycle.
            set_pending_usd(sym, accumulated)
            mark_queued(sym, today)
            log.info(f"[{sym}] DCA: accumulated (${accumulated:.2f}) still below minimum "
                      f"(${min_usd_needed:.2f}) — no order placed today, will keep accumulating")
            continue

        oid = place_short(sym, target, target, accumulated)
        if oid == "SKIP":
            # Should be rare given the min_usd_needed check above, but
            # handled defensively: keep accumulating rather than lose
            # the slice.
            set_pending_usd(sym, accumulated)
            mark_queued(sym, today)
            log.warning(f"[{sym}] DCA fire unexpectedly skipped by place_short despite meeting "
                        f"estimated minimum — accumulator preserved at ${accumulated:.2f}, will retry next midnight wake")
            continue
        if oid is None:
            # Order rejected — keep the accumulator intact (do not lose
            # the queued budget) but still mark today as fired so the
            # daily slice isn't re-added on top of itself tomorrow;
            # the shortfall is retried as part of the existing
            # accumulator, not by re-queuing today's slice again.
            set_pending_usd(sym, accumulated)
            mark_queued(sym, today)
            log.error(f"[{sym}] DCA fire rejected by MEXC; accumulator preserved at ${accumulated:.2f}, will retry next midnight wake")
            continue

        # Success: order placed for the full accumulated amount, reset accumulator.
        set_pending_usd(sym, 0.0)
        mark_queued(sym, today)
        record_order({
            "symbol": sym,
            "date": today.isoformat(),
            "order_id": oid,
            "limit_price": target,
            "sizing_price": target,
            "mark_at_fire": mark,
            "usd": accumulated,
            "slice_usd": slice_usd,
            "leverage": effective_leverage(sym),
        })

# ── SVG rendering (multi-symbol status table) ─────────────────────────────────

def render_svg(marks: Dict[str, float], highs: Dict[str, Optional[float]], today: datetime.date) -> str:
    W, H = 1080, 60 + 26 * len(SYMBOLS)
    now_str = datetime.datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        f'<text x="10" y="20" fill="#111" font-family="monospace" font-size="14">'
        f'DCA5-Bot (priced + sized at 9d high) {now_str}</text>',
    ]
    y = 50
    for sym in SYMBOLS:
        mark = marks.get(sym, 0.0)
        high = highs.get(sym)
        high_str = f"{high:,.4f}" if high is not None else "n/a"
        start = DCA_START_DATE[sym]
        end = start + datetime.timedelta(days=DCA_DAYS - 1)
        n_fired = fired_count(sym)
        active = in_dca_window(sym, today)
        fired_today = has_fired_today(sym, today)
        budget = SYMBOL_BUDGET_USD[sym]
        pending = get_pending_usd(sym)
        lev = effective_leverage(sym)
        remaining_usd = max(0.0, budget - (n_fired * daily_slice_usd(sym)))

        if today < start:
            phase = f"not started (begins {start.isoformat()})"
        elif today > end:
            phase = f"window complete ({end.isoformat()})"
        else:
            phase = f"day {(today - start).days + 1}/{DCA_DAYS}" + (
                " — queued today" if fired_today else " — pending today" if active else ""
            )

        clr = "#1a8a1a" if fired_today else ("#aa1111" if active else "#999")
        line = (f"{sym:<18} mark={mark:>12,.4f} 9dHigh={high_str:>12} "
                f"queued={n_fired:>3}/{DCA_DAYS} pending=${pending:>7,.2f} "
                f"lev={lev:>3}x remaining=${remaining_usd:>8,.2f} {phase}")
        svg.append(f'<text x="10" y="{y}" fill="{clr}" font-family="monospace" font-size="12">{line}</text>')
        y += 26
    svg.append("</svg>")
    return "\n".join(svg)

# ── engine loop ────────────────────────────────────────────────────────────────

def _seconds_until_next_hour() -> float:
    now = time.time()
    return (int(now) // 3600 + 1) * 3600 + HOURLY_SLEEP_FLOOR_SEC - now

def engine_cycle():
    now_utc = datetime.datetime.now(UTC)
    marks = {sym: get_mark(sym) for sym in SYMBOLS}
    highs = {sym: rolling_9d_high(sym) for sym in SYMBOLS}

    if now_utc.hour == 0:
        run_daily_dca(now_utc)

    svg = render_svg(marks, highs, now_utc.date())
    STATE.set_svg(svg)
    n_fired_total = sum(len(v) for v in STATE_DATA["fired"].values())
    STATE.set_status(f"ok {now_utc.strftime('%Y-%m-%d %H:%M UTC')} total_fires={n_fired_total}")

def run_engine():
    load_specs()
    run_startup_test_orders()

    log.info("engine starting — running initial cycle")
    try:
        engine_cycle()
    except Exception as e:
        log.error(f"initial engine cycle failed: {e}", exc_info=True)
        STATE.set_status(f"error: {e}")

    while True:
        wait_s = _seconds_until_next_hour()
        time.sleep(max(0, wait_s))
        try:
            engine_cycle()
        except Exception as e:
            log.error(f"engine cycle failed: {e}", exc_info=True)
            STATE.set_status(f"error: {e}")

# ── http server (second thread) ───────────────────────────────────────────────

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default per-request access logging

    def do_GET(self):
        if self.path == "/chart.svg":
            svg = STATE.get_svg().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Content-Length", str(len(svg)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(svg)
        elif self.path == "/orders.json":
            body = json.dumps(STATE_DATA["orders"], indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/" or self.path == "":
            status = STATE.get_status()
            html = (
                "<html><head><title>DCA5-Bot Overview</title></head><body>"
                "<h3>DCA5-Bot — 90-Day DCA Short Bot (priced + sized at 9d high)</h3>"
                f"<p>status: {status}</p>"
                "<img src='/chart.svg' alt='status table'/>"
                "<p><a href='/orders.json'>order records (JSON)</a></p>"
                "</body></html>"
            )
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

def run_server():
    server = http.server.ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Handler)
    log.info(f"server listening on {HTTP_HOST}:{HTTP_PORT}")
    server.serve_forever()

# ── entrypoint ─────────────────────────────────────────────────────────────────

def main():
    if not MEXC_KEY or not MEXC_SECRET:
        log.error("MEXC / MEXCSECRET not set")
        raise SystemExit(1)

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    run_engine()  # main thread — blocks forever

if __name__ == "__main__":
    main()
