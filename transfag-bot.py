abcdefg#!/usr/bin/env python3
"""
MultiBiDCA-Bot — Multi-Symbol Bidirectional Daily DCA Bot.

Every symbol is priced and sized independently. Once per UTC day,
at UTC midnight, the bot fires exactly one limit order per symbol,
at that moment's mark price, sized at the GREATER of:
  - a fixed USD floor (ORDER_MIN_USD, currently $0.10), and
  - the exchange's own reported minimum order size for that
    contract (volUnit, converted to USD at the current mark price).

Direction is fixed per symbol by a static classification supplied
at the top of this file:

  - "up"   list -> SELL (short) once per day
  - "down" list -> BUY  (long)  once per day

This is intentionally a bidirectional, stacking DCA protocol: there
is no budget, no accumulator, and no automatic close. Each day's
order simply adds to (or against) that symbol's existing isolated
position. Positions are expected to accumulate over time; unwinding
them is a manual, out-of-band decision.

Single-process, single-machine bot for Fly.io.

Symbols are configured in one place — UP_BASES / DOWN_BASES, near
the top of this file. Each base is resolved to BASE + "_USDT" and
validated live against MEXC's contract detail at startup.

═══════════════════════════════════════════════════════════════════
ORDER SIZING
═══════════════════════════════════════════════════════════════════

For every order this bot places (startup test orders AND daily
fires alike):

  1. Compute the exchange's own minimum order size in USD:
       exch_min_usd = volUnit * contractSize * price
  2. target_usd = max(ORDER_MIN_USD, exch_min_usd)
  3. Convert target_usd to contracts at `price`, rounded to the
     exchange's volume precision.
  4. If the ROUNDED contract count still falls below volUnit (can
     happen after rounding down, e.g. for high-priced/low-precision
     contracts), bump up to exactly volUnit contracts, since MEXC
     will otherwise reject the order outright.

This sizing function (`_target_vol`) is the single place order size
is decided; both startup test orders and daily fires call it, so
sizing logic can never drift between the two.

═══════════════════════════════════════════════════════════════════
DAILY EXECUTION ENGINE
═══════════════════════════════════════════════════════════════════

Once per UTC calendar day, at or after UTC midnight (00:00:00), for
EVERY non-failed symbol:
  1. Fetch the current mark price.
  2. Compute order size per ORDER SIZING above.
  3. Place ONE limit order at exactly that mark price:
       - side = SELL if the symbol's base is in UP_BASES
       - side = BUY  if the symbol's base is in DOWN_BASES
  4. Record the fill attempt (success or failure) for charting and
     for the 14:00 UTC report.

A guard (mirroring the original bot's report guard) ensures the
daily fire cannot double-execute on restart near midnight: it
requires both (a) current time is at/after UTC midnight, AND (b) at
least DAILY_FIRE_MIN_INTERVAL_HOURS have passed since the last
successful daily-fire pass for that symbol.

FAILED symbols (see below) are skipped entirely at fire time — no
order is ever attempted for them.

═══════════════════════════════════════════════════════════════════
FAILED SYMBOLS
═══════════════════════════════════════════════════════════════════

A symbol that fails its startup test order is flagged FAILED for
the remainder of the process's lifetime. This excludes it from
TRADING ONLY:
  - No daily fire is ever attempted.

It does NOT exclude the symbol from CHARTING:
  - Its daily-candle buffer keeps refreshing every 30 minutes, same
    as any other symbol.
  - Its chart keeps rendering every 30 minutes and is linked from
    the main overview page exactly like a healthy symbol, marked
    plainly as failed.

═══════════════════════════════════════════════════════════════════
CHARTS
═══════════════════════════════════════════════════════════════════

Each symbol — failed or not — gets its own SVG candlestick chart:
the trailing 30 UTC-daily candles. The chart marks:
  - Every REAL daily order successfully placed, as a filled circle
    (blue = buy, red = sell) at its fire time/price.
  - Every MISSED/FAILED daily attempt (order rejected by the
    exchange, for a non-failed symbol) as a small X tick marker at
    the attempted time/price.

Charts are re-rendered every 30 minutes, independent of the daily
fire cycle, using the daily-candle buffer (which is itself also
refreshed every 30 minutes from MEXC's daily klines).

Served at /chart/<SYMBOL>.svg and linked from the main overview
page for every symbol, failed or not.

═══════════════════════════════════════════════════════════════════
STATUS REPORT (ntfy) — 14:00 UTC
═══════════════════════════════════════════════════════════════════

Once per UTC calendar day, at or after 14:00 UTC, a plain-text
status report is pushed to the ntfy.sh topic configured below, one
line per symbol, containing:
  - side (BUY / SELL, per the static classification)
  - current open position size, converted to USD at mark price
  - unrealized PnL in USD (mark-to-market against MEXC's own
    reported average open price for the position; MEXC's own
    unrealized-PnL field is used directly where available)

This bot never closes positions automatically, so PnL reported here
is ALWAYS unrealized / mark-to-market.

The same duplicate-send guard as the original bot applies: requires
both (a) current time at/after 14:00 UTC, AND (b) at least
REPORT_MIN_INTERVAL_HOURS since the last successful send.

Failed symbols are still listed in the report so the report
reflects that they're being watched but not traded further.

═══════════════════════════════════════════════════════════════════
STARTUP TEST ORDERS
═══════════════════════════════════════════════════════════════════

  - On startup: run a one-time TEST order (limit, sized per ORDER
    SIZING above, in the symbol's OWN daily direction — sell for
    up-list, buy for down-list — at a price offset from mark so it
    does not immediately fill) against EVERY symbol, in three flat
    batch phases (no threads):
      1. OPEN  — send a test limit order for every symbol.
      2. WAIT  — sleep once, for TEST_ORDER_WAIT_SEC seconds.
      3. CLOSE — check fill status and cancel/confirm for every
         symbol that opened.

    ANY failure at any phase flags that symbol FAILED for the
    remainder of this process's lifetime. A test order that fills
    during the wait is NOT a failure (it is a real position and is
    recorded as such).

Environment (secrets only, not behavior):
  MEXC        - MEXC API key
  MEXCSECRET  - MEXC API secret

IMPORTANT:
  Contract specifications are fetched live from MEXC at startup.
  The bot does not hardcode priceUnit, volUnit, or contractSize.
"""

import collections
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
import xml.sax.saxutils as _saxutils
from typing import Deque, Dict, List, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── constants ─────────────────────────────────────────────────────────────────

UTC = datetime.timezone.utc

MEXC_KEY    = os.getenv("MEXC")
MEXC_SECRET = os.getenv("MEXCSECRET")
MEXC_BASE   = "https://api.mexc.co"


# ── symbol configuration ──────────────────────────────────────────────────────
#
# Bases only — each is resolved to BASE + "_USDT" and validated live
# against MEXC's contract detail at startup (see load_specs). A base
# that cannot be resolved is flagged FAILED, same as any other
# startup validation failure.
#
# UP   -> SELL once per day (short)
# DOWN -> BUY  once per day (long)

UP_BASES: List[str] = [
    "ADA",
    "TRX",
    "PYTH",
    "POL",
    "CRV",
]

DOWN_BASES: List[str] = [
    "WAVES",
    "HFT",
    "HOT",
    "RARE",
    "RPL",
    "SKL",
    "SNX",
    "C98",
    "DYDX",
    "BICO",
    "BLUR",
    "BEAMX",
    "MINA",
    "DOT",
    "CYBER",
    "SCRT",
]

QUOTE_SUFFIX = "_USDT"

SYMBOLS: List[str] = [
    b + QUOTE_SUFFIX for b in UP_BASES + DOWN_BASES
]

# side per symbol, fixed for the process lifetime
SIDE_OF: Dict[str, str] = {}

for _b in UP_BASES:
    SIDE_OF[_b + QUOTE_SUFFIX] = "sell"

for _b in DOWN_BASES:
    SIDE_OF[_b + QUOTE_SUFFIX] = "buy"

LEVERAGE = 10
OPEN_TYPE_ISOLATED = 1

# MEXC contract order side codes:
#   1 = open long, 2 = close short, 3 = open short, 4 = close long
MEXC_SIDE_OPEN_LONG  = 1
MEXC_SIDE_OPEN_SHORT = 3


# ── order sizing constants ────────────────────────────────────────────────────

# Fixed USD floor for every order this bot places (startup test
# orders and daily fires alike). The exchange's own minimum order
# size (volUnit, converted to USD at the order price) is always
# also respected — the bot uses whichever of the two is larger.
ORDER_MIN_USD = 0.10


# ── daily fire engine constants ───────────────────────────────────────────────

# Minimum time that must pass since the last successful daily fire
# pass for a symbol before another can be attempted, even if we're
# past UTC midnight again — guards against double-fire on restart
# near midnight.
DAILY_FIRE_MIN_INTERVAL_HOURS = 20

DAILY_FIRE_HOUR_UTC   = 0
DAILY_FIRE_MINUTE_UTC = 0


# ── data refresh constants ────────────────────────────────────────────────────

DATA_REFRESH_INTERVAL_SEC = 30 * 60   # 30 minutes


# ── chart constants ────────────────────────────────────────────────────────────

CHART_DAYS = 30                       # 30 daily candles
DAILY_BUFFER_MAX_DAYS = CHART_DAYS + 5  # small safety margin

CHART_W = 1200
CHART_H = 420
CHART_MARGIN_L = 60
CHART_MARGIN_R = 20
CHART_MARGIN_T = 40
CHART_MARGIN_B = 40


# ── status report / ntfy constants ────────────────────────────────────────────

NTFY_TOPIC     = "1618091301200506091401140305"
NTFY_URL       = f"https://ntfy.sh/{NTFY_TOPIC}"
REPORT_HOUR_UTC   = 14
REPORT_MINUTE_UTC = 0

# Minimum time that must pass since the last successful send before
# another can go out, even if we're past REPORT_HOUR_UTC again —
# guards against double-send on restart near 14:00 UTC.
REPORT_MIN_INTERVAL_HOURS = 20


# ── timing ────────────────────────────────────────────────────────────────────

MAIN_LOOP_SLEEP_FLOOR_SEC = 5


# ── startup test order ────────────────────────────────────────────────────────

TEST_ORDER_DISCOUNT_BUY  = 0.90   # buy test placed 10% below mark (won't fill)
TEST_ORDER_PREMIUM_SELL  = 1.10   # sell test placed 10% above mark (won't fill)
TEST_ORDER_WAIT_SEC      = 20


# ── failed-symbol tracking ────────────────────────────────────────────────────
#
# FAILED excludes a symbol from TRADING ONLY (daily fire attempts).
# It does NOT exclude charting or buffer refresh.

FAILED_SYMBOLS: set = set()
_FAILED_LOCK = threading.Lock()


def _xml_escape(s: str) -> str:
    """
    Escapes text for safe embedding inside SVG/XML text content.
    Handles &, <, > (and quotes, harmless extra safety) so that any
    dynamic string — symbol names, formatted numbers, log-derived
    text — can never break XML parsing.
    """
    return _saxutils.escape(str(s))


def flag_failed(sym: str, reason: str):

    with _FAILED_LOCK:
        FAILED_SYMBOLS.add(sym)

    log.error(
        f"[{sym}] FLAGGED FAILED — {reason} — "
        "excluded from trading (buffer/chart continue)"
    )


def is_failed(sym: str) -> bool:

    with _FAILED_LOCK:
        return sym in FAILED_SYMBOLS


# ── HTTP server ───────────────────────────────────────────────────────────────

HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(os.getenv("PORT", "8080"))


# ── persistence ──────────────────────────────────────────────────────────────

STATE_FILE = os.getenv(
    "DCA_STATE_FILE",
    "/data/multi_bidca_fire_history.json"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s"
)

log = logging.getLogger()


specs: Dict[str, Dict] = {}


# ── shared state ──────────────────────────────────────────────────────────────

class SharedState:

    def __init__(self):
        self._lock = threading.Lock()

        self._svg = (
            "<svg xmlns='http://www.w3.org/2000/svg' "
            "width='600' height='100'>"
            "<text x='10' y='50'>Initializing...</text>"
            "</svg>"
        )

        self._status = "initializing"

        self._chart_svgs: Dict[str, str] = {}

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

    def set_chart_svg(self, sym: str, svg: str):
        with self._lock:
            self._chart_svgs[sym] = svg

    def get_chart_svg(self, sym: str) -> str:
        with self._lock:
            return self._chart_svgs.get(
                sym,
                "<svg xmlns='http://www.w3.org/2000/svg' width='400' "
                "height='60'><text x='10' y='30' "
                "font-family='Courier New'>Loading chart...</text></svg>"
            )


STATE = SharedState()


# ── persisted state ──────────────────────────────────────────────────────────
#
# {
#   "orders": [...],
#   "missed": [
#       {"symbol": "...", "time": "...", "price": ..., "side": "buy"},
#       ...
#   ],
#   "last_fire_date": {"ADA_USDT": "2026-08-22", ...},
#   "last_fire_at": {"ADA_USDT": "2026-08-22T00:00:03+00:00", ...},
#   "last_report_sent_at": "2026-08-19T14:00:07+00:00"
# }
#
# "missed" is a lightweight rolling log (trimmed to the chart
# window) used purely to render missed-attempt markers on charts —
# separate from "orders", which only ever contains real fills.

def _default_state() -> Dict:
    return {
        "orders": [],
        "missed": [],
        "last_fire_date": {},
        "last_fire_at": {},
        "last_report_sent_at": None,
    }


def load_state() -> Dict:

    try:

        with open(STATE_FILE, "r") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("state file did not contain a dict")

        defaults = _default_state()

        for k, v in defaults.items():
            data.setdefault(k, v)

        return data

    except FileNotFoundError:

        log.info(
            f"no state file at {STATE_FILE} — starting fresh"
        )

        return _default_state()

    except Exception as e:

        log.error(
            f"state file at {STATE_FILE} unreadable ({e}) "
            f"— starting fresh"
        )

        return _default_state()


def save_state(state: Dict):

    try:

        os.makedirs(
            os.path.dirname(STATE_FILE) or ".",
            exist_ok=True
        )

        tmp = STATE_FILE + ".tmp"

        with open(tmp, "w") as f:
            json.dump(state, f)

        os.replace(tmp, STATE_FILE)

    except Exception as e:

        log.error(
            f"failed to persist state to {STATE_FILE}: {e}"
        )


STATE_DATA: Dict = load_state()
_STATE_DATA_LOCK = threading.Lock()


def get_last_fire_date(sym: str) -> Optional[datetime.date]:

    s = STATE_DATA["last_fire_date"].get(sym)

    if not s:
        return None

    try:
        return datetime.date.fromisoformat(s)
    except Exception:
        return None


def get_last_fire_at(sym: str) -> Optional[datetime.datetime]:

    s = STATE_DATA["last_fire_at"].get(sym)

    if not s:
        return None

    try:
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None


def _persist():

    save_state(STATE_DATA)


def mark_fired(sym: str, when_utc: datetime.datetime):

    with _STATE_DATA_LOCK:

        STATE_DATA["last_fire_date"][sym] = when_utc.date().isoformat()
        STATE_DATA["last_fire_at"][sym] = when_utc.isoformat()

        _persist()


def record_order(order_record: Dict):

    with _STATE_DATA_LOCK:

        STATE_DATA["orders"].append(order_record)

        _persist()


def record_missed(
    sym: str,
    when_utc: datetime.datetime,
    price: float,
    side: str
):

    """
    Lightweight log entry used purely for chart missed-attempt
    markers. Trimmed to roughly the chart window on write to bound
    growth.
    """

    with _STATE_DATA_LOCK:

        STATE_DATA["missed"].append({
            "symbol": sym,
            "time": when_utc.isoformat(),
            "price": price,
            "side": side,
        })

        cutoff = time.time() - CHART_DAYS * 86400 - 3600

        STATE_DATA["missed"] = [
            m for m in STATE_DATA["missed"]
            if _safe_ts(m.get("time")) is None
            or _safe_ts(m.get("time")) >= cutoff
        ]

        _persist()


def _safe_ts(iso_str: Optional[str]) -> Optional[float]:

    if not iso_str:
        return None

    try:
        return datetime.datetime.fromisoformat(iso_str).timestamp()
    except Exception:
        return None


def total_orders_count() -> int:

    return len(STATE_DATA["orders"])


def get_last_report_sent_at() -> Optional[datetime.datetime]:

    s = STATE_DATA.get("last_report_sent_at")

    if not s:
        return None

    try:
        return datetime.datetime.fromisoformat(s)
    except Exception:
        return None


def set_last_report_sent_at(dt: datetime.datetime):

    with _STATE_DATA_LOCK:

        STATE_DATA["last_report_sent_at"] = dt.isoformat()

        _persist()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _http(
    method,
    url,
    headers=None,
    data=None,
    params=None
):

    if params:

        url += "?" + urllib.parse.urlencode(
            sorted(params.items())
        )

    req = urllib.request.Request(
        url,
        data=data,
        headers=headers or {},
        method=method
    )

    try:

        with urllib.request.urlopen(
            req,
            timeout=10
        ) as r:

            body = r.read()

    except urllib.error.HTTPError as e:

        body = e.read()

    return (
        json.loads(body)
        if body.strip()
        else {}
    )


def _get(url):

    with urllib.request.urlopen(
        url,
        timeout=10
    ) as r:

        return json.loads(r.read())


# ── MEXC signed requests ─────────────────────────────────────────────────────

def mexc(
    method,
    endpoint,
    params=None,
    body=None
):

    params = params or {}

    ts = str(
        int(time.time() * 1000)
    )

    sp = (
        "&".join(
            f"{k}={v}"
            for k, v in sorted(params.items())
        )
        if method == "GET"
        else (
            json.dumps(
                body,
                separators=(",", ":"),
                sort_keys=True
            )
            if body
            else ""
        )
    )

    sig = hmac.new(
        MEXC_SECRET.encode(),
        (MEXC_KEY + ts + sp).encode(),
        hashlib.sha256
    ).hexdigest()

    hdr = {
        "ApiKey": MEXC_KEY,
        "Request-Time": ts,
        "Signature": sig,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    raw = (
        json.dumps(
            body,
            separators=(",", ":"),
            sort_keys=True
        ).encode()
        if body and method not in ("GET", "DELETE")
        else None
    )

    try:

        return _http(
            method,
            MEXC_BASE + endpoint,
            headers=hdr,
            data=raw,
            params=params
            if method in ("GET", "DELETE")
            else None
        )

    except Exception as e:

        log.error(
            f"mexc {method} {endpoint}: {e}"
        )

        return {}


# ── ntfy ──────────────────────────────────────────────────────────────────────

def ntfy_send(message: str, title: Optional[str] = None) -> bool:

    """
    Pushes a plain-text message to the configured ntfy.sh topic.
    Returns True on apparent success, False on failure. Never
    raises — a failed notification must not affect trading.
    """

    headers = {"Content-Type": "text/plain; charset=utf-8"}

    if title:
        headers["Title"] = title

    try:

        req = urllib.request.Request(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers=headers,
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=10) as r:

            r.read()

        log.info(f"ntfy: report sent to {NTFY_URL}")

        return True

    except Exception as e:

        log.error(f"ntfy: failed to send report: {e}")

        return False


# ── contract specifications ───────────────────────────────────────────────────

def load_specs():

    """
    Fetch contract specifications once for every symbol.

    A symbol whose specs cannot be loaded is flagged failed rather
    than aborting the whole process, so other symbols can still
    trade (and this symbol can still chart).
    """

    rows = (
        mexc(
            "GET",
            "/api/v1/contract/detail"
        ).get("data") or []
    )

    if not rows:

        log.error(
            "empty contract detail response from MEXC — "
            "flagging all symbols failed"
        )

        for sym in SYMBOLS:

            flag_failed(
                sym,
                "empty contract detail response from MEXC"
            )

        return

    by_sym = {
        c.get("symbol", "").upper(): c
        for c in rows
    }

    for sym in SYMBOLS:

        match = by_sym.get(sym)

        if match is None:

            flag_failed(
                sym,
                "symbol not found in MEXC contract detail "
                f"(resolved as {sym} — check USDT-quoted contract "
                "exists for this base)"
            )

            continue

        vu = float(
            match.get("volUnit", 1)
        )

        pu = float(
            match.get("priceUnit", 0.01)
        )

        cs = float(
            match.get("contractSize", vu)
        )

        raw = (
            f"{vu:.10f}"
            .rstrip("0")
        )

        p = (
            len(raw.split(".")[1])
            if "." in raw
            else 0
        )

        specs[sym] = {
            "p": p,
            "t": pu,
            "vu": vu,
            "cs": cs,
        }

        log.info(
            f"loaded specs for {sym}: "
            f"{specs[sym]}"
        )


def _tick(sym):

    return specs.get(
        sym,
        {}
    ).get("t", 0.01)


def _prec(sym):

    return specs.get(
        sym,
        {}
    ).get("p", 0)


def _rfmt_price(sym, v):

    t = _tick(sym)

    r = round(v / t) * t

    s = (
        f"{t:.10f}"
        .rstrip("0")
    )

    dec = (
        len(s.split(".")[1])
        if "." in s
        else 0
    )

    return f"{r:.{dec}f}"


def _rfmt_vol(sym, v):

    p = _prec(sym)

    if p >= 0:

        return (
            f"{round(v, p):.{p}f}"
        )

    d = 10 ** abs(p)

    return str(
        int(round(v / d) * d)
    )


def _contract_size(sym) -> float:

    return specs.get(
        sym,
        {}
    ).get("cs", 1.0)


def _mos(sym) -> float:

    """Exchange-reported minimum order size, in contracts."""

    return specs.get(
        sym,
        {}
    ).get("vu", 1.0)


def _usd_of_contracts(sym, contracts: float, price: float) -> float:

    return contracts * _contract_size(sym) * price


def _target_vol(sym: str, price: float) -> float:

    """
    Single source of truth for order sizing, used by BOTH startup
    test orders and daily fires.

    Sizing rule: max(ORDER_MIN_USD, exchange minimum order value in
    USD), converted to contracts at `price`, rounded to the
    exchange's volume precision, then bumped up to the exchange's
    raw volUnit if rounding dropped it back below that floor (which
    can happen for low-precision / high-priced contracts).
    """

    min_vol = _mos(sym)

    exch_min_usd = _usd_of_contracts(sym, min_vol, price)

    target_usd = max(ORDER_MIN_USD, exch_min_usd)

    raw_contracts = (
        target_usd / (_contract_size(sym) * price)
        if price > 0
        else min_vol
    )

    rounded_str = _rfmt_vol(sym, raw_contracts)

    rounded_contracts = float(rounded_str)

    if rounded_contracts < min_vol:

        log.warning(
            f"[{sym}] target size {rounded_contracts} contracts "
            f"(${target_usd:.4f}) rounded below exchange minimum "
            f"{min_vol} contracts — bumping up to exchange minimum"
        )

        rounded_contracts = min_vol

    return rounded_contracts


# ── open orders ───────────────────────────────────────────────────────────────

def _open_orders_for_sym(
    sym: str
) -> List[Dict]:

    data = (
        mexc(
            "GET",
            "/api/v1/private/order/list/open_orders",
            params={
                "symbol": sym,
                "page_num": 1,
                "page_size": 100,
            }
        ).get("data") or []
    )

    if isinstance(data, dict):

        data = data.get(
            "resultList",
            []
        )

    return [
        o for o in data
        if o.get(
            "symbol",
            ""
        ).upper() == sym
    ]


def _open_ids(sym: str) -> set:

    return {
        str(o.get("orderId", ""))
        for o in _open_orders_for_sym(sym)
    }


# ── position query ────────────────────────────────────────────────────────────

def get_open_position(sym: str) -> Optional[Dict]:

    """
    Returns the current open position for sym, or None if flat.
    Uses MEXC's own reported avg open price and unrealized PnL
    where available, since these already account for the exchange's
    own accounting rules (funding, fees on open, etc.) more
    accurately than a locally-derived recomputation would.
    """

    data = (
        mexc(
            "GET",
            "/api/v1/private/position/open_positions",
            params={"symbol": sym}
        ).get("data") or []
    )

    if not isinstance(data, list) or not data:
        return None

    for p in data:

        if p.get("symbol", "").upper() != sym:
            continue

        hold_vol = float(p.get("holdVol", 0) or 0)

        if hold_vol == 0:
            continue

        return p

    return None


# ── order placement ──────────────────────────────────────────────────────────

def place_order(
    sym: str,
    side: str,
    limit_price: float,
    vol_contracts: float
) -> Optional[str]:

    """
    side: "buy" -> open long, "sell" -> open short.
    Always an OPEN order (this bot never closes), isolated margin,
    fixed leverage.
    """

    mexc_side = (
        MEXC_SIDE_OPEN_LONG
        if side == "buy"
        else MEXC_SIDE_OPEN_SHORT
    )

    body = {
        "leverage": LEVERAGE,
        "openType": OPEN_TYPE_ISOLATED,
        "positionMode": 1,
        "price": _rfmt_price(
            sym,
            limit_price
        ),
        "side": mexc_side,
        "symbol": sym,
        "type": 1,
        "vol": _rfmt_vol(
            sym,
            vol_contracts
        ),
    }

    r = mexc(
        "POST",
        "/api/v1/private/order/create",
        body=body
    )

    if not r.get("success"):

        log.error(
            f"[{sym}] {side} order rejected: {r}"
        )

        return None

    data = r.get("data") or {}

    if not isinstance(data, dict):

        log.error(
            f"[{sym}] unexpected 'data' shape "
            f"from order/create: {data!r}"
        )

        return None

    oid = data.get("orderId")

    if not oid:

        log.error(
            f"[{sym}] order/create succeeded "
            f"but no 'orderId' in data: {data!r}"
        )

        return None

    oid = str(oid)

    log.info(
        f"[{sym}] limit {side.upper()} "
        f"{_rfmt_vol(sym, vol_contracts)} "
        f"@ {_rfmt_price(sym, limit_price)} "
        f"id={oid}"
    )

    return oid


# ── cancel order ──────────────────────────────────────────────────────────────

def cancel_order(
    sym: str,
    oid: str
) -> bool:

    body = [oid]

    r = mexc(
        "POST",
        "/api/v1/private/order/cancel",
        body=body
    )

    ok = bool(
        r.get("success")
    )

    if ok:

        log.info(
            f"[{sym}] cancelled order "
            f"id={oid}"
        )

    else:

        log.error(
            f"[{sym}] cancel failed "
            f"for id={oid}: {r}"
        )

    return ok


def is_filled(
    sym: str,
    oid: str
) -> bool:

    return oid not in _open_ids(sym)


# ── mark price ────────────────────────────────────────────────────────────────

def get_mark(
    sym: str
) -> float:

    d = (
        mexc(
            "GET",
            "/api/v1/contract/ticker",
            params={"symbol": sym}
        ).get("data") or {}
    )

    return float(
        d.get(
            "fairPrice",
            d.get("lastPrice", 0)
        ) or 0
    )


# ── daily klines ──────────────────────────────────────────────────────────────

def fetch_daily_bars(
    sym: str,
    start_s: int,
    end_s: int
) -> List[Dict]:

    now_s = int(time.time())

    url = (
        f"{MEXC_BASE}/api/v1/contract/kline/{sym}"
        f"?interval=Day1"
        f"&start={start_s}"
        f"&end={end_s}"
    )

    try:

        raw = _get(url)

    except Exception as e:

        log.error(
            f"[{sym}] daily kline fetch failed: {e}"
        )

        return []

    if not raw.get("success"):

        log.error(
            f"[{sym}] daily kline fetch "
            f"unsuccessful: {raw}"
        )

        return []

    d = raw.get("data") or {}

    times  = d.get("time") or []
    opens  = d.get("realOpen")  or d.get("open")  or []
    highs  = d.get("realHigh")  or d.get("high")  or []
    lows   = d.get("realLow")   or d.get("low")   or []
    closes = d.get("realClose") or d.get("close") or []

    n = min(
        len(times), len(opens), len(highs), len(lows), len(closes)
    )

    bars = []

    for i in range(n):

        t_s = int(times[i])

        if t_s > now_s + 86400:
            # drop bars that would represent the future
            continue

        try:
            o = float(opens[i])
            h = float(highs[i])
            l = float(lows[i])
            c = float(closes[i])
        except Exception:
            continue

        if l <= 0:
            continue

        bars.append({"t": t_s, "o": o, "h": h, "l": l, "c": c})

    bars.sort(key=lambda b: b["t"])

    return bars


# ── per-symbol rolling daily OHLC buffer ──────────────────────────────────────

class DailyBuffer:

    def __init__(self):
        self.bars: Deque[Dict] = collections.deque()
        self.lock = threading.Lock()

    def seed(self, bars: List[Dict]):

        with self.lock:
            self.bars = collections.deque(bars)
            self._trim_locked()

    def append_new(self, bars: List[Dict]):

        with self.lock:

            existing_ts = {
                b["t"] for b in self.bars
            }

            for b in bars:

                if b["t"] in existing_ts:
                    # daily bar for "today" may update intraday —
                    # replace rather than skip
                    self.bars = collections.deque(
                        bb for bb in self.bars if bb["t"] != b["t"]
                    )

                self.bars.append(b)
                existing_ts.add(b["t"])

            self._sort_and_trim_locked()

    def _sort_and_trim_locked(self):

        self.bars = collections.deque(
            sorted(self.bars, key=lambda b: b["t"])
        )

        self._trim_locked()

    def _trim_locked(self):

        cutoff = int(time.time()) - DAILY_BUFFER_MAX_DAYS * 86400

        while self.bars and self.bars[0]["t"] < cutoff:
            self.bars.popleft()

    def snapshot(self) -> List[Dict]:

        with self.lock:
            return list(self.bars)

    def size(self) -> int:

        with self.lock:
            return len(self.bars)


DAILY_BUFFERS: Dict[str, DailyBuffer] = {
    sym: DailyBuffer() for sym in SYMBOLS
}


def seed_daily_buffer(sym: str):

    """
    One-time startup seed of ~35 days of daily history for sym.
    Called for EVERY symbol regardless of failed status, so failed
    symbols still get full-history charts.
    """

    now_s = int(time.time())

    start_s = now_s - DAILY_BUFFER_MAX_DAYS * 86400

    bars = fetch_daily_bars(sym, start_s, now_s)

    DAILY_BUFFERS[sym].seed(bars)

    log.info(
        f"[{sym}] daily buffer seeded: "
        f"{DAILY_BUFFERS[sym].size()} bars"
    )

    if not bars:

        log.warning(
            f"[{sym}] seeded daily buffer is EMPTY — "
            "chart will be blank until the next refresh cycle "
            "picks up data"
        )


def refresh_daily_buffer(sym: str):

    """
    Periodic (every DATA_REFRESH_INTERVAL_SEC) incremental update.
    Called for EVERY symbol regardless of failed status. Re-fetches
    the last few days so the in-progress "today" candle stays
    current.
    """

    now_s = int(time.time())

    start_s = now_s - 5 * 86400

    bars = fetch_daily_bars(sym, start_s, now_s)

    if bars:
        DAILY_BUFFERS[sym].append_new(bars)


# ── daily fire engine ─────────────────────────────────────────────────────────

def _due_for_daily_fire(sym: str, now_utc: datetime.datetime) -> bool:

    at_or_after_fire_time = (
        (now_utc.hour, now_utc.minute)
        >= (DAILY_FIRE_HOUR_UTC, DAILY_FIRE_MINUTE_UTC)
    )

    if not at_or_after_fire_time:
        return False

    last_at = get_last_fire_at(sym)

    if last_at is not None:

        hours_since = (now_utc - last_at).total_seconds() / 3600.0

        if hours_since < DAILY_FIRE_MIN_INTERVAL_HOURS:
            return False

    return True


def process_symbol_daily_fire(sym: str, now_utc: datetime.datetime):

    """
    Attempts exactly one daily order for sym, if due. FAILED
    symbols are skipped entirely (checked by the caller before this
    is invoked — see run_daily_fire_checks).
    """

    side = SIDE_OF[sym]

    mark = get_mark(sym)

    if mark <= 0:

        log.error(
            f"[{sym}] invalid mark price ({mark}) at daily fire — "
            "skipping this attempt, will retry next cycle"
        )

        return

    vol = _target_vol(sym, mark)

    log.info(
        f"[{sym}] daily fire due — side={side} "
        f"mark={mark:.6f} vol={vol} "
        f"(target ${_usd_of_contracts(sym, vol, mark):.4f})"
    )

    oid = place_order(sym, side, mark, vol)

    if oid is None:

        record_missed(sym, now_utc, mark, side)

        log.error(
            f"[{sym}] daily fire order rejected — recorded as "
            "missed, will retry next cycle (not marked fired)"
        )

        return

    # Only mark fired on a genuinely accepted order, so a rejected
    # attempt is retried on the very next engine cycle rather than
    # waiting a full day.
    mark_fired(sym, now_utc)

    usd_size = _usd_of_contracts(sym, vol, mark)

    record_order({
        "symbol": sym,
        "timestamp": now_utc.isoformat(),
        "order_id": oid,
        "side": side,
        "limit_price": mark,
        "vol_contracts": vol,
        "usd": usd_size,
    })


def run_daily_fire_checks(now_utc: datetime.datetime):

    """
    Runs for every NON-FAILED symbol that is due. Failed symbols
    are never attempted.
    """

    for sym in SYMBOLS:

        if is_failed(sym):
            continue

        try:

            if _due_for_daily_fire(sym, now_utc):

                process_symbol_daily_fire(sym, now_utc)

        except Exception as e:

            log.error(
                f"[{sym}] daily fire check failed: {e}",
                exc_info=True
            )


# ── status report ─────────────────────────────────────────────────────────────

def build_status_report_text(now_utc: datetime.datetime) -> str:

    header = (
        f"Status Report — {now_utc.strftime('%Y-%m-%d %H:%M')} UTC"
    )

    lines = [header, ""]

    for sym in SYMBOLS:

        side = SIDE_OF[sym]

        excluded_note = " [EXCLUDED — not traded]" if is_failed(sym) else ""

        try:

            pos = get_open_position(sym)

        except Exception as e:

            lines.append(
                f"{sym}: side={side}  ERROR fetching position ({e})"
                f"{excluded_note}"
            )

            continue

        if pos is None:

            lines.append(
                f"{sym}: side={side}  position=flat  "
                f"size_usd=$0.00  upnl=$0.00{excluded_note}"
            )

            continue

        hold_vol = float(pos.get("holdVol", 0) or 0)
        avg_price = float(pos.get("holdAvgPrice", pos.get("openAvgPrice", 0)) or 0)
        upnl = pos.get("unrealizedPnl") or pos.get("unrealised", None)

        mark = get_mark(sym)

        size_usd = _usd_of_contracts(sym, hold_vol, mark if mark > 0 else avg_price)

        if upnl is not None:

            try:
                upnl_usd = float(upnl)
            except Exception:
                upnl_usd = None

        else:

            upnl_usd = None

        if upnl_usd is None and mark > 0 and avg_price > 0:

            # Fallback local mark-to-market if MEXC didn't report it
            # directly: long gains as price rises, short gains as
            # price falls.
            direction = 1 if side == "buy" else -1

            upnl_usd = (
                direction
                * (mark - avg_price)
                * hold_vol
                * _contract_size(sym)
            )

        upnl_str = (
            f"${upnl_usd:,.2f}" if upnl_usd is not None else "n/a"
        )

        lines.append(
            f"{sym}: side={side}  "
            f"avg_price={avg_price:,.6f}  "
            f"size_usd=${size_usd:,.2f}  "
            f"upnl={upnl_str}"
            f"{excluded_note}"
        )

    return "\n".join(lines)


def maybe_send_status_report(now_utc: datetime.datetime):

    """
    Sends the status report at/after REPORT_HOUR_UTC:REPORT_MINUTE_UTC,
    but only if at least REPORT_MIN_INTERVAL_HOURS have passed since
    the last successful send.
    """

    at_or_after_report_time = (
        (now_utc.hour, now_utc.minute)
        >= (REPORT_HOUR_UTC, REPORT_MINUTE_UTC)
    )

    if not at_or_after_report_time:
        return

    last_sent = get_last_report_sent_at()

    if last_sent is not None:

        hours_since = (now_utc - last_sent).total_seconds() / 3600.0

        if hours_since < REPORT_MIN_INTERVAL_HOURS:
            return

    report_text = build_status_report_text(now_utc)

    log.info(f"sending status report:\n{report_text}")

    sent_ok = ntfy_send(
        report_text,
        title=f"BiDCA Bot Status Report {now_utc.date().isoformat()}"
    )

    if sent_ok:

        set_last_report_sent_at(now_utc)

    else:

        log.error(
            "status report send failed — will retry next cycle"
        )


# ── startup test orders ───────────────────────────────────────────────────────

def _open_test_order(sym: str) -> Optional[Dict]:

    if sym not in specs:

        return None

    side = SIDE_OF[sym]

    try:

        mark = get_mark(sym)

        if mark <= 0:

            flag_failed(
                sym,
                f"invalid mark price ({mark}) at startup test"
            )

            return None

        test_price = (
            mark * TEST_ORDER_DISCOUNT_BUY
            if side == "buy"
            else mark * TEST_ORDER_PREMIUM_SELL
        )

        vol = _target_vol(sym, test_price)

        log.info(
            f"[{sym}] test order OPEN: "
            f"side={side} mark={mark:.6f} "
            f"limit={test_price:.6f} "
            f"vol={vol} (target "
            f"${_usd_of_contracts(sym, vol, test_price):.4f})"
        )

        oid = place_order(
            sym,
            side,
            test_price,
            vol
        )

        if oid is None:

            flag_failed(
                sym,
                "test order rejected by MEXC"
            )

            return None

        log.info(
            f"[{sym}] test order placed id={oid}"
        )

        return {
            "sym": sym,
            "oid": oid,
            "side": side,
            "limit_price": test_price,
            "vol": vol,
        }

    except Exception as e:

        flag_failed(
            sym,
            f"exception during test order open: {e}"
        )

        log.error(
            f"[{sym}] test order open failed: {e}",
            exc_info=True
        )

        return None


def _close_test_order(pending: Dict):

    sym = pending["sym"]
    oid = pending["oid"]

    try:

        if is_filled(sym, oid):

            log.warning(
                f"[{sym}] test order id={oid} "
                f"FILLED during the "
                f"{TEST_ORDER_WAIT_SEC}s wait. "
                "This is now a real open position. "
                "Symbol remains validated."
            )

            record_order({
                "symbol": sym,
                "timestamp": datetime.datetime.now(UTC).isoformat(),
                "order_id": oid,
                "side": pending["side"],
                "kind": "startup_test_filled",
                "limit_price": pending["limit_price"],
                "vol_contracts": pending["vol"],
                "usd": _usd_of_contracts(
                    sym, pending["vol"], pending["limit_price"]
                ),
            })

            return

        cancelled = cancel_order(
            sym,
            oid
        )

        if cancelled:

            log.info(
                f"[{sym}] test order id={oid} "
                "cancelled successfully — symbol validated"
            )

        else:

            flag_failed(
                sym,
                f"test order id={oid} could not be cancelled"
            )

    except Exception as e:

        flag_failed(
            sym,
            f"exception during test order close: {e}"
        )

        log.error(
            f"[{sym}] test order close failed: {e}",
            exc_info=True
        )


def run_startup_test_orders():

    log.info(
        f"══ startup test orders: {len(SYMBOLS)} symbols — "
        f"phase 1/3: opening ══"
    )

    pending = []

    for sym in SYMBOLS:

        result = _open_test_order(sym)

        if result is not None:

            pending.append(result)

    log.info(
        f"══ startup test orders: {len(pending)}/{len(SYMBOLS)} "
        f"opened — phase 2/3: waiting {TEST_ORDER_WAIT_SEC}s ══"
    )

    time.sleep(TEST_ORDER_WAIT_SEC)

    log.info(
        "══ startup test orders: phase 3/3: closing ══"
    )

    for p in pending:

        _close_test_order(p)

    ok = [s for s in SYMBOLS if not is_failed(s)]
    failed = [s for s in SYMBOLS if is_failed(s)]

    log.info(
        "══ startup test orders: all symbols done — "
        f"{len(ok)} ok, {len(failed)} failed "
        f"{failed if failed else ''} ══"
    )


# ── main overview SVG ─────────────────────────────────────────────────────────

def render_svg(now_utc: datetime.datetime) -> str:

    W = 1200
    H = 60 + 30 * len(SYMBOLS)

    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    title_text = _xml_escape(
        f'MultiBiDCA-Bot — {len(SYMBOLS)} symbols — '
        f'daily bidirectional engine — {now_str}'
    )

    svg = [

        '<?xml version="1.0" encoding="UTF-8"?>',

        (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {W} {H}" '
            f'width="100%" '
            f'style="max-width:{W}px;display:block">'
        ),

        f'<rect width="{W}" height="{H}" fill="#fafafa"/>',

        (
            f'<text x="20" y="24" '
            f'font-family="Courier New" '
            f'font-size="13" '
            f'fill="#333" '
            f'font-weight="bold">'
            f'{title_text}'
            f'</text>'
        ),
    ]

    y = 50

    for sym in SYMBOLS:

        failed = is_failed(sym)
        side = SIDE_OF[sym]

        buf = DAILY_BUFFERS[sym]

        n_orders = sum(
            1 for o in STATE_DATA["orders"]
            if o.get("symbol") == sym
        )

        last_fire = get_last_fire_date(sym)
        last_fire_str = last_fire.isoformat() if last_fire else "never"

        if failed:

            clr = "#cc0000"

            line = (
                f"{sym:<16} "
                "*** FAILED — EXCLUDED FROM TRADING "
                "(chart still tracked) ***  "
                f"side={side:<4}  buf={buf.size():>3}d"
            )

        else:

            clr = "#cc2200" if side == "sell" else "#1a8a1a"

            line = (
                f"{sym:<16} "
                f"side={side:<4}  "
                f"fires={n_orders:>4}  "
                f"last_fire={last_fire_str:<10}  "
                f"buf={buf.size():>3}d"
            )

        svg.append(
            f'<text x="20" y="{y}" '
            f'font-family="Courier New" '
            f'font-size="11" '
            f'fill="{clr}">'
            f'{_xml_escape(line)}'
            f'</text>'
        )

        y += 30

    svg.append("</svg>")

    return "\n".join(svg)


# ── per-symbol chart SVG ───────────────────────────────────────────────────────

def render_symbol_chart_svg(sym: str) -> str:

    """
    Rendered for EVERY symbol, failed or not. Failed symbols get
    the same candlesticks, but only ever show missed-attempt
    markers (never order circles, since none are ever attempted
    after failure — any pre-failure fills, including a startup test
    fill, still show as order circles).
    """

    buf = DAILY_BUFFERS[sym]

    candles = buf.snapshot()

    now_s = int(time.time())
    cutoff = now_s - CHART_DAYS * 86400

    candles = [c for c in candles if c["t"] >= cutoff]

    if not candles:

        return (
            '<?xml version="1.0" encoding="UTF-8"?>'
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'width="{CHART_W}" height="{CHART_H}">'
            f'<rect width="{CHART_W}" height="{CHART_H}" fill="#fafafa"/>'
            f'<text x="20" y="40" font-family="Courier New" '
            f'font-size="14" fill="#888">'
            f'{sym}: no chart data yet</text></svg>'
        )

    failed = is_failed(sym)
    side = SIDE_OF[sym]

    lo = min(c["l"] for c in candles)
    hi = max(c["h"] for c in candles)

    span = (hi - lo) or 1.0

    lo -= span * 0.05
    hi += span * 0.05
    span = hi - lo

    plot_w = CHART_W - CHART_MARGIN_L - CHART_MARGIN_R
    plot_h = CHART_H - CHART_MARGIN_T - CHART_MARGIN_B

    t0 = candles[0]["t"]
    t1 = candles[-1]["t"] + 86400
    t_span = (t1 - t0) or 1

    def x_of(t: int) -> float:
        return CHART_MARGIN_L + (t - t0) / t_span * plot_w

    def y_of(price: float) -> float:
        return CHART_MARGIN_T + (hi - price) / span * plot_h

    now_str = datetime.datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    title_suffix = _xml_escape(
        " [FAILED — excluded from trading]" if failed else ""
    )

    svg = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {CHART_W} {CHART_H}" '
            f'width="100%" style="max-width:{CHART_W}px;display:block">'
        ),
        f'<rect width="{CHART_W}" height="{CHART_H}" fill="#fafafa"/>',
        (
            f'<text x="{CHART_MARGIN_L}" y="20" '
            f'font-family="Courier New" font-size="13" '
            f'fill="{"#cc0000" if failed else "#333"}" font-weight="bold">'
            f'{sym} — 30d daily candles — side={side} — '
            f'{now_str}{title_suffix}</text>'
        ),
    ]

    for i in range(6):

        price = lo + span * i / 5
        y = y_of(price)

        svg.append(
            f'<line x1="{CHART_MARGIN_L}" y1="{y:.1f}" '
            f'x2="{CHART_W - CHART_MARGIN_R}" y2="{y:.1f}" '
            f'stroke="#e0e0e0" stroke-width="1"/>'
        )

        svg.append(
            f'<text x="4" y="{y + 4:.1f}" '
            f'font-family="Courier New" font-size="9" '
            f'fill="#888">{_xml_escape(f"{price:,.4f}")}</text>'
        )

    candle_px_w = max(2.0, plot_w / len(candles) * 0.6)

    for c in candles:

        x = x_of(c["t"]) + (plot_w / len(candles)) / 2

        up = c["c"] >= c["o"]
        color = "#1a8a1a" if up else "#cc2200"

        y_high = y_of(c["h"])
        y_low = y_of(c["l"])

        y_open = y_of(c["o"])
        y_close = y_of(c["c"])

        body_top = min(y_open, y_close)
        body_h = max(1.0, abs(y_close - y_open))

        svg.append(
            f'<line x1="{x:.1f}" y1="{y_high:.1f}" '
            f'x2="{x:.1f}" y2="{y_low:.1f}" '
            f'stroke="{color}" stroke-width="1"/>'
        )

        svg.append(
            f'<rect x="{x - candle_px_w / 2:.1f}" y="{body_top:.1f}" '
            f'width="{candle_px_w:.1f}" height="{body_h:.1f}" '
            f'fill="{color}"/>'
        )

    # Order markers — filled circles, at every real fill for this
    # symbol (blue = buy/long, red = sell/short).
    orders = [
        o for o in STATE_DATA["orders"]
        if o.get("symbol") == sym
        and "limit_price" in o
        and "timestamp" in o
    ]

    for o in orders:

        ts = _safe_ts(o.get("timestamp"))

        if ts is None or ts < t0 or ts > t1:
            continue

        ox = x_of(int(ts))
        oy = y_of(o["limit_price"])

        marker_color = "#0044cc" if o.get("side") == "buy" else "#cc2200"

        svg.append(
            f'<circle cx="{ox:.1f}" cy="{oy:.1f}" r="4" '
            f'fill="{marker_color}" stroke="#fff" stroke-width="1"/>'
        )

    # Missed/failed attempt markers — small X ticks.
    missed = [
        m for m in STATE_DATA["missed"]
        if m.get("symbol") == sym
    ]

    for m in missed:

        ts = _safe_ts(m.get("time"))

        if ts is None or ts < t0 or ts > t1:
            continue

        mx = x_of(int(ts))
        my = y_of(m["price"])

        sz = 3.5

        svg.append(
            f'<line x1="{mx - sz:.1f}" y1="{my - sz:.1f}" '
            f'x2="{mx + sz:.1f}" y2="{my + sz:.1f}" '
            f'stroke="#7a3fb8" stroke-width="1.3"/>'
        )

        svg.append(
            f'<line x1="{mx - sz:.1f}" y1="{my + sz:.1f}" '
            f'x2="{mx + sz:.1f}" y2="{my - sz:.1f}" '
            f'stroke="#7a3fb8" stroke-width="1.3"/>'
        )

    # Legend
    legend_y = CHART_H - 8

    svg.append(
        f'<circle cx="{CHART_MARGIN_L + 4}" cy="{legend_y}" r="4" '
        f'fill="#0044cc" stroke="#fff" stroke-width="1"/>'
    )
    svg.append(
        f'<text x="{CHART_MARGIN_L + 14}" y="{legend_y + 4}" '
        f'font-family="Courier New" font-size="10" fill="#555">'
        f'buy fill</text>'
    )

    svg.append(
        f'<circle cx="{CHART_MARGIN_L + 90}" cy="{legend_y}" r="4" '
        f'fill="#cc2200" stroke="#fff" stroke-width="1"/>'
    )
    svg.append(
        f'<text x="{CHART_MARGIN_L + 100}" y="{legend_y + 4}" '
        f'font-family="Courier New" font-size="10" fill="#555">'
        f'sell fill</text>'
    )

    svg.append(
        f'<line x1="{CHART_MARGIN_L + 175}" y1="{legend_y - 4}" '
        f'x2="{CHART_MARGIN_L + 183}" y2="{legend_y + 4}" '
        f'stroke="#7a3fb8" stroke-width="1.3"/>'
    )
    svg.append(
        f'<line x1="{CHART_MARGIN_L + 175}" y1="{legend_y + 4}" '
        f'x2="{CHART_MARGIN_L + 183}" y2="{legend_y - 4}" '
        f'stroke="#7a3fb8" stroke-width="1.3"/>'
    )
    svg.append(
        f'<text x="{CHART_MARGIN_L + 189}" y="{legend_y + 4}" '
        f'font-family="Courier New" font-size="10" fill="#555">'
        f'missed</text>'
    )

    svg.append(
        f'<rect x="{CHART_MARGIN_L}" y="{CHART_MARGIN_T}" '
        f'width="{plot_w}" height="{plot_h}" '
        f'fill="none" stroke="#999" stroke-width="1"/>'
    )

    svg.append("</svg>")

    return "\n".join(svg)


# ── engine timing ─────────────────────────────────────────────────────────────

def _seconds_until_next_refresh_mark() -> float:

    now = time.time()

    next_mark = (
        (int(now) // DATA_REFRESH_INTERVAL_SEC + 1)
        * DATA_REFRESH_INTERVAL_SEC
    )

    return next_mark - now


# ── engine cycle ─────────────────────────────────────────────────────────────

def engine_cycle():

    now_utc = datetime.datetime.now(UTC)

    # Daily fire check first (only actually fires once/day per
    # symbol, guarded) — never delayed by chart rendering or report
    # sending.
    run_daily_fire_checks(now_utc)

    # Refresh daily buffers for EVERY symbol, failed or not.
    for sym in SYMBOLS:

        try:

            refresh_daily_buffer(sym)

        except Exception as e:

            log.error(
                f"[{sym}] daily buffer refresh failed: {e}",
                exc_info=True
            )

    svg = render_svg(now_utc)
    STATE.set_svg(svg)

    # Charts rendered AFTER trading logic, for EVERY symbol —
    # failed symbols get charts too.
    for sym in SYMBOLS:

        try:

            chart_svg = render_symbol_chart_svg(sym)
            STATE.set_chart_svg(sym, chart_svg)

        except Exception as e:

            log.error(
                f"[{sym}] chart render failed: {e}",
                exc_info=True
            )

    try:

        maybe_send_status_report(now_utc)

    except Exception as e:

        log.error(
            f"status report check failed: {e}",
            exc_info=True
        )

    n_orders = total_orders_count()
    n_failed = len(FAILED_SYMBOLS)

    STATE.set_status(
        f"ok  "
        f"{now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}  "
        f"total_orders={n_orders}  "
        f"failed_symbols={n_failed}"
    )


# ── engine ────────────────────────────────────────────────────────────────────

def run_engine():

    load_specs()

    run_startup_test_orders()

    log.info(
        "seeding daily candle buffers for ALL symbols "
        f"(including failed) (~{DAILY_BUFFER_MAX_DAYS} days each)"
    )

    for sym in SYMBOLS:

        try:

            seed_daily_buffer(sym)

        except Exception as e:

            log.error(
                f"[{sym}] failed to seed daily buffer: {e}",
                exc_info=True
            )

    log.info(
        "engine starting — running initial cycle"
    )

    try:

        engine_cycle()

    except Exception as e:

        log.error(
            f"initial engine cycle failed: {e}",
            exc_info=True
        )

        STATE.set_status(f"error: {e}")

    while True:

        wait_s = _seconds_until_next_refresh_mark()

        time.sleep(max(0, wait_s))

        try:

            engine_cycle()

        except Exception as e:

            log.error(
                f"engine cycle failed: {e}",
                exc_info=True
            )

            STATE.set_status(f"error: {e}")


# ── HTTP server ───────────────────────────────────────────────────────────────

class Handler(
    http.server.BaseHTTPRequestHandler
):

    def log_message(
        self,
        fmt,
        *args
    ):

        pass

    def do_GET(self):

        if self.path == "/chart.svg":

            svg = STATE.get_svg().encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Content-Length", str(len(svg)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(svg)

        elif self.path.startswith("/chart/") and self.path.endswith(".svg"):

            sym = self.path[len("/chart/"):-len(".svg")]

            if sym not in SYMBOLS:

                self.send_response(404)
                self.end_headers()
                return

            svg = STATE.get_chart_svg(sym).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "image/svg+xml")
            self.send_header("Content-Length", str(len(svg)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(svg)

        elif self.path == "/orders.json":

            body = json.dumps(
                STATE_DATA["orders"], indent=2
            ).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/missed.json":

            body = json.dumps(
                STATE_DATA["missed"], indent=2
            ).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/failed.json":

            body = json.dumps(
                sorted(FAILED_SYMBOLS), indent=2
            ).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif self.path == "/positions.json":

            out = {}

            for sym in SYMBOLS:

                try:
                    pos = get_open_position(sym)
                except Exception as e:
                    pos = {"error": str(e)}

                out[sym] = pos

            body = json.dumps(out, indent=2).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif (
            self.path == "/"
            or self.path == ""
        ):

            status = STATE.get_status()

            chart_links = " · ".join(
                (
                    f'<a href="/chart/{sym}.svg" target="_blank">'
                    f'{sym} ({SIDE_OF[sym]})'
                    f'{" [failed]" if is_failed(sym) else ""}'
                    f'</a>'
                )
                for sym in SYMBOLS
            )

            html = (
                "<!doctype html>"
                "<html>"
                "<head>"
                "<meta charset='utf-8'>"
                "<meta http-equiv='refresh' content='300'>"
                "<title>MultiBiDCA-Bot Overview</title>"
                "<style>"
                "body{font-family:monospace;"
                "background:#fafafa;margin:24px}"
                "img{max-width:100%;height:auto;"
                "border:1px solid #ccc}"
                "</style>"
                "</head>"
                "<body>"
                "<h3>"
                "MultiBiDCA-Bot — "
                "Multi-Symbol Bidirectional Daily DCA Bot"
                "</h3>"
                f"<p>status: {status}</p>"
                "<img src='/chart.svg' "
                "alt='overview table'/>"
                f"<p>charts: {chart_links}</p>"
                "<p>"
                "<a href='/orders.json'>order records</a>"
                " · "
                "<a href='/positions.json'>live positions</a>"
                " · "
                "<a href='/missed.json'>missed attempts</a>"
                " · "
                "<a href='/failed.json'>failed symbols</a>"
                "</p>"
                "</body>"
                "</html>"
            )

            body = html.encode("utf-8")

            self.send_response(200)
            self.send_header(
                "Content-Type", "text/html; charset=utf-8"
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        else:

            self.send_response(404)
            self.end_headers()


# ── HTTP server thread ────────────────────────────────────────────────────────

def run_server():

    server = http.server.ThreadingHTTPServer(
        (HTTP_HOST, HTTP_PORT),
        Handler
    )

    log.info(
        f"server listening on {HTTP_HOST}:{HTTP_PORT}"
    )

    server.serve_forever()


# ── entrypoint ────────────────────────────────────────────────────────────────

def main():

    if not MEXC_KEY or not MEXC_SECRET:

        log.error("MEXC / MEXCSECRET not set")

        raise SystemExit(1)

    server_thread = threading.Thread(
        target=run_server,
        daemon=True
    )

    server_thread.start()

    run_engine()


if __name__ == "__main__":

    main()
