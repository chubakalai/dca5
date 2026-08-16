#!/usr/bin/env python3
"""
DCA5-Bot — Multi-Symbol 90-Day DCA Short Bot, priced and sized at rolling 9-day high

Single-process, single-machine bot for Fly.io.

Behavior:
  - On startup: run a one-time TEST order (limit short at market+10%,
    held 60s, then cancelled if unfilled) against EVERY symbol in
    SYMBOLS, one at a time, sequentially, to validate the signing and
    order-placement/cancellation path for each symbol's own specs
    (tick size, min size) before the real engine loop begins.
  - Every hour on the hour: refresh mark prices + rolling 9d highs for
    all symbols and refresh the in-memory SVG status table.
  - Only at hour == 00 UTC: for each symbol, if today's calendar date
    falls within that symbol's 90-day DCA window and that symbol has
    not already fired today, place a limit SHORT priced AND SIZED at
    that symbol's current trailing 9-day high.
  - Existing symbols each have a $1,000 90-day DCA budget.
  - The 9 newly added stock symbols share a combined $1,000 90-day
    DCA budget, so each receives $1,000 / 9 = $111.111111... total
    over its 90-day window.
  - All DCA orders are left open indefinitely. Previous unfilled
    orders are never cancelled by the daily engine; orders stack.
  - Fire history and every placed DCA order are persisted locally.
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


# ── constants ────────────────────────────────────────────────────────────────

UTC = datetime.timezone.utc

MEXC_KEY    = os.getenv("MEXC")
MEXC_SECRET = os.getenv("MEXCSECRET")
MEXC_BASE   = "https://api.mexc.co"


# ── symbols ──────────────────────────────────────────────────────────────────
#
# Original symbols:
#   $1,000 DCA budget EACH
#
# New stock symbols:
#   $1,000 TOTAL shared across all 9 symbols
#

ORIGINAL_SYMBOLS = [
    "SPX500_USDT",
    "BTC_USDT",
    "ETH_USDT",
    "SOL_USDT",
    "XRP_USDT",
    "NAS100_USDT",
    "COPPER_USDT",
    "SILVER_USDT",
    "XAU_USDT",
]

NEW_STOCK_SYMBOLS = [
    "BABASTOCK_USDT",      # Alibaba Group Holding Limited (BABA)
    "BIDUSTOCK_USDT",      # Baidu, Inc. (BIDU)
    "JD_USDT",             # JD.com, Inc. (JD)
    "XIAOMISTOCK_USDT",    # Xiaomi Corporation (XIAOMI)
    "ZHONGJISTOCK_USDT",   # Zhongji Innolight (ZHONGJI)
    "ZHIPUSTOCK_USDT",     # Zhipu AI (ZHIPU)
    "ENFLAMESTOCK_USDT",   # Enflame Technology (ENFLAME)
    "CXMTSTOCK_USDT",      # ChangXin Memory Technologies (CXMT)
]

SYMBOLS = ORIGINAL_SYMBOLS + NEW_STOCK_SYMBOLS


LEVERAGE  = 30
ROLL_DAYS = 9


# ── DCA schedule ─────────────────────────────────────────────────────────────
#
# Original 9 symbols:
#   $1,000 each
#
# New 8 symbols listed above:
#   $1,000 total across the group
#
# NOTE:
# The requested new-symbol list contains 8 symbols, not 9:
#
#   1. BABASTOCK_USDT
#   2. BIDUSTOCK_USDT
#   3. JD_USDT
#   4. XIAOMISTOCK_USDT
#   5. ZHONGJISTOCK_USDT
#   6. ZHIPUSTOCK_USDT
#   7. ENFLAMESTOCK_USDT
#   8. CXMTSTOCK_USDT
#
# Therefore the $1,000 shared budget is divided across 8 symbols:
#
#   $1,000 / 8 = $125 per symbol
#
#   $125 / 90 = ~$1.3888889 per daily fire.
#
# This matches the original "125 USD each" amount you specified.
#

ORIGINAL_DCA_BUDGET_USD = 1000.0
NEW_STOCK_GROUP_BUDGET_USD = 1000.0

DCA_DAYS = 90


DCA_BUDGET_USD: Dict[str, float] = {
    sym: ORIGINAL_DCA_BUDGET_USD
    for sym in ORIGINAL_SYMBOLS
}

NEW_STOCK_DAILY_BUDGET_USD = (
    NEW_STOCK_GROUP_BUDGET_USD / len(NEW_STOCK_SYMBOLS)
)

for sym in NEW_STOCK_SYMBOLS:
    DCA_BUDGET_USD[sym] = NEW_STOCK_DAILY_BUDGET_USD


DCA_DAILY_USD: Dict[str, float] = {
    sym: DCA_BUDGET_USD[sym] / DCA_DAYS
    for sym in SYMBOLS
}


# Per-symbol start date (UTC calendar date).
DCA_START_DATE: Dict[str, datetime.date] = {
    sym: datetime.date(2026, 8, 1)
    for sym in SYMBOLS
}


def in_dca_window(sym: str, d: datetime.date) -> bool:
    start = DCA_START_DATE[sym]
    end = start + datetime.timedelta(days=DCA_DAYS - 1)
    return start <= d <= end


HOURLY_SLEEP_FLOOR_SEC = 5

TEST_ORDER_PREMIUM  = 1.10
TEST_ORDER_WAIT_SEC = 60
TEST_ORDER_USD      = 75.0

HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(os.getenv("PORT", "8080"))

STATE_FILE = os.getenv(
    "DCA_STATE_FILE",
    "/data/dca_fire_history.json"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s"
)

log = logging.getLogger()

specs: Dict[str, Dict] = {}


# ── shared state ─────────────────────────────────────────────────────────────

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


# ── persisted state ──────────────────────────────────────────────────────────

def _default_state() -> Dict:
    return {
        "fired": {},
        "orders": []
    }


def load_state() -> Dict:
    """
    Load:
      {
        "fired": {
          "SYMBOL": ["YYYY-MM-DD", ...]
        },
        "orders": [...]
      }

    Missing or corrupt file -> fresh empty state.
    """

    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("state file did not contain a dict")

        data.setdefault("fired", {})
        data.setdefault("orders", [])

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


def has_fired_today(
    sym: str,
    d: datetime.date
) -> bool:

    return d.isoformat() in STATE_DATA["fired"].get(sym, [])


def mark_fired(
    sym: str,
    d: datetime.date,
    order_record: Dict
):
    STATE_DATA["fired"].setdefault(sym, []).append(
        d.isoformat()
    )

    STATE_DATA["orders"].append(order_record)

    save_state(STATE_DATA)


def fired_count(sym: str) -> int:
    return len(
        STATE_DATA["fired"].get(sym, [])
    )


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

    if method == "GET":
        sp = "&".join(
            f"{k}={v}"
            for k, v in sorted(params.items())
        )

    else:
        sp = (
            json.dumps(
                body,
                separators=(",", ":"),
                sort_keys=True
            )
            if body
            else ""
        )

    sig = hmac.new(
        MEXC_SECRET.encode(),
        (
            MEXC_KEY +
            ts +
            sp
        ).encode(),
        hashlib.sha256
    ).hexdigest()

    hdr = {
        "ApiKey": MEXC_KEY,
        "Request-Time": ts,
        "Signature": sig,
        "Content-Type": "application/json",
        "Accept": "application/json"
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
            params=(
                params
                if method in ("GET", "DELETE")
                else None
            )
        )

    except Exception as e:
        log.error(
            f"mexc {method} {endpoint}: {e}"
        )
        return {}


# ── contract specs / sizing ──────────────────────────────────────────────────

def load_specs():
    """
    Fetch contract specs for EVERY symbol.

    No silent fallback: if any configured symbol is missing,
    the process exits rather than risking a mis-sized order.
    """

    rows = (
        mexc(
            "GET",
            "/api/v1/contract/detail"
        ).get("data") or []
    )

    if not rows:
        log.error(
            "empty contract detail response from MEXC"
        )
        raise SystemExit(1)

    by_sym = {
        c.get("symbol", "").upper(): c
        for c in rows
    }

    missing = [
        s for s in SYMBOLS
        if s not in by_sym
    ]

    if missing:
        log.error(
            f"symbols not found in MEXC contract detail: "
            f"{missing}"
        )
        raise SystemExit(1)

    for sym in SYMBOLS:

        match = by_sym[sym]

        vu = float(
            match.get("volUnit", 1)
        )

        pu = float(
            match.get("priceUnit", 0.5)
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
            "cs": cs
        }

        log.info(
            f"loaded specs for {sym}: "
            f"{specs[sym]}"
        )


def _tick(sym):
    return specs.get(
        sym,
        {}
    ).get("t", 0.5)


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
        int(
            round(v / d) * d
        )
    )


def _contracts(
    sym,
    usd,
    price
):
    """
    Contract count so that `usd` dollars of notional trades
    at `price`.

    For DCA orders, `price` is the 9-day-high limit price.
    """

    cs = specs.get(
        sym,
        {}
    ).get("cs", 1.0)

    return float(
        _rfmt_vol(
            sym,
            max(
                0,
                usd / (cs * price)
            )
        )
    )


def _mos(sym):
    return specs.get(
        sym,
        {}
    ).get("vu", 1.0)


# ── orders ───────────────────────────────────────────────────────────────────

def _open_orders_for_sym(
    sym: str
) -> List[Dict]:

    """
    Fetch open orders and filter by symbol client-side.

    MEXC's symbol query parameter is not treated as a reliable
    server-side filter.
    """

    data = (
        mexc(
            "GET",
            "/api/v1/private/order/list/open_orders",
            params={
                "symbol": sym,
                "page_num": 1,
                "page_size": 100
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
        if o.get("symbol", "").upper() == sym
    ]


def _open_ids(sym: str) -> set:
    return {
        str(o.get("orderId", ""))
        for o in _open_orders_for_sym(sym)
    }


def place_short(
    sym: str,
    limit_price: float,
    sizing_price: float,
    usd_amount: float
) -> Optional[str]:

    """
    Place a limit SHORT / sell-to-open order.

    `sizing_price` is deliberately separate from `limit_price`
    so startup tests can size from mark while DCA orders size
    from their actual 9-day-high limit price.
    """

    vol = _contracts(
        sym,
        usd_amount,
        sizing_price
    )

    if vol < _mos(sym):
        log.warning(
            f"[{sym}] size {vol} < min {_mos(sym)} "
            f"(${usd_amount:.6f}) — order skipped"
        )
        return "SKIP"

    body = {
        "leverage": LEVERAGE,
        "openType": 2,
        "positionMode": 1,
        "price": _rfmt_price(
            sym,
            limit_price
        ),
        "side": 3,
        "symbol": sym,
        "type": 1,
        "vol": _rfmt_vol(
            sym,
            vol
        )
    }

    r = mexc(
        "POST",
        "/api/v1/private/order/create",
        body=body
    )

    if not r.get("success"):
        log.error(
            f"[{sym}] short order rejected: {r}"
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
            f"[{sym}] order/create succeeded but "
            f"no 'orderId' in data: {data!r}"
        )
        return None

    oid = str(oid)

    log.info(
        f"[{sym}] limit SHORT "
        f"{_rfmt_vol(sym, vol)} "
        f"@ {_rfmt_price(sym, limit_price)} "
        f"id={oid} "
        f"usd={usd_amount:.6f} "
        f"sizing_price={sizing_price:.8f}"
    )

    return oid


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
            f"[{sym}] cancelled order id={oid}"
        )
    else:
        log.error(
            f"[{sym}] cancel failed for "
            f"id={oid}: {r}"
        )

    return ok


def is_filled(
    sym: str,
    oid: str
) -> bool:

    return oid not in _open_ids(sym)


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
            d.get(
                "lastPrice",
                0
            )
        ) or 0
    )


# ── daily klines / rolling 9-day high ────────────────────────────────────────

def fetch_daily_bars(
    sym: str,
    lookback_days: int
) -> List[Dict]:

    """
    Fetch closed daily candles.

    Uses realHigh where available.
    Excludes the currently open daily candle.
    """

    now_s = int(
        time.time()
    )

    start_s = (
        now_s -
        (lookback_days + 2) * 86400
    )

    url = (
        f"{MEXC_BASE}/api/v1/contract/kline/{sym}"
        f"?interval=Day1"
        f"&start={start_s}"
        f"&end={now_s}"
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
            f"[{sym}] daily kline fetch unsuccessful: "
            f"{raw}"
        )
        return []

    d = raw.get("data") or {}

    times = d.get("time") or []

    highs = (
        d.get("realHigh")
        or d.get("high")
        or []
    )

    bars = []

    for i in range(
        len(times)
    ):
        t_s = int(
            times[i]
        )

        if t_s + 86400 > now_s:
            continue

        bars.append({
            "t": t_s * 1000,
            "h": float(highs[i])
        })

    bars.sort(
        key=lambda b: b["t"]
    )

    return bars


def rolling_9d_high(
    sym: str
) -> Optional[float]:

    bars = fetch_daily_bars(
        sym,
        ROLL_DAYS + 3
    )

    if len(bars) < ROLL_DAYS:
        log.error(
            f"[{sym}] only {len(bars)} closed "
            f"daily bars available, need {ROLL_DAYS} "
            f"— cannot compute 9d high"
        )
        return None

    window = bars[
        -ROLL_DAYS:
    ]

    return max(
        b["h"]
        for b in window
    )


# ── startup test orders ──────────────────────────────────────────────────────

def run_startup_test_order_for(
    sym: str
):

    log.info(
        f"── startup test order [{sym}]: begin ──────────────────"
    )

    try:

        mark = get_mark(sym)

        if mark <= 0:
            log.error(
                f"[{sym}] test order aborted: "
                f"invalid mark price ({mark})"
            )
            return

        test_price = (
            mark *
            TEST_ORDER_PREMIUM
        )

        log.info(
            f"[{sym}] test order: "
            f"mark={mark:.8f} "
            f"limit={test_price:.8f} "
            f"(+{(TEST_ORDER_PREMIUM - 1) * 100:.0f}%) "
            f"usd={TEST_ORDER_USD:.2f}"
        )

        oid = place_short(
            sym,
            test_price,
            mark,
            TEST_ORDER_USD
        )

        if oid == "SKIP":
            log.warning(
                f"[{sym}] test order skipped — "
                f"below minimum contract size"
            )
            return

        if oid is None:
            log.error(
                f"[{sym}] test order was rejected "
                f"by MEXC"
            )
            return

        log.info(
            f"[{sym}] test order placed "
            f"id={oid} — waiting "
            f"{TEST_ORDER_WAIT_SEC}s"
        )

        time.sleep(
            TEST_ORDER_WAIT_SEC
        )

        if is_filled(sym, oid):

            log.warning(
                f"[{sym}] test order id={oid} "
                f"FILLED during the "
                f"{TEST_ORDER_WAIT_SEC}s wait — "
                f"this is now a real open short "
                f"position. Review MEXC manually."
            )

        else:

            cancelled = cancel_order(
                sym,
                oid
            )

            if cancelled:
                log.info(
                    f"[{sym}] test order id={oid} "
                    f"cancelled successfully"
                )
            else:
                log.error(
                    f"[{sym}] test order id={oid} "
                    f"could not be cancelled — "
                    f"check MEXC manually"
                )

    except Exception as e:

        log.error(
            f"[{sym}] startup test order failed: {e}",
            exc_info=True
        )

    log.info(
        f"── startup test order [{sym}]: end ─────────────────────"
    )


def run_startup_test_orders():

    log.info(
        f"══ startup test orders: "
        f"{len(SYMBOLS)} symbols, "
        f"~{TEST_ORDER_WAIT_SEC}s each ══"
    )

    for sym in SYMBOLS:
        run_startup_test_order_for(sym)

    log.info(
        "══ startup test orders: all symbols done ══"
    )


# ── daily DCA trigger ────────────────────────────────────────────────────────

def run_daily_dca(
    now_utc: datetime.datetime
):

    today = now_utc.date()

    for sym in SYMBOLS:

        if not in_dca_window(
            sym,
            today
        ):
            continue

        if has_fired_today(
            sym,
            today
        ):
            log.info(
                f"[{sym}] DCA: already fired "
                f"for {today.isoformat()} — skipping"
            )
            continue

        mark = get_mark(sym)

        if mark <= 0:
            log.error(
                f"[{sym}] DCA: invalid mark price "
                f"({mark}) — skipping today"
            )
            continue

        target = rolling_9d_high(sym)

        if target is None:
            log.error(
                f"[{sym}] DCA: could not compute "
                f"9d high — skipping today"
            )
            continue

        daily_usd = DCA_DAILY_USD[sym]

        total_budget = DCA_BUDGET_USD[sym]

        log.info(
            f"[{sym}] DCA fire "
            f"{today.isoformat()}: "
            f"daily=${daily_usd:.8f} "
            f"budget=${total_budget:.8f} "
            f"limit SHORT @ 9dHigh={target:.8f} "
            f"(sized off 9dHigh, "
            f"not mark={mark:.8f})"
        )

        oid = place_short(
            sym,
            target,
            target,
            daily_usd
        )

        if oid == "SKIP":
            log.warning(
                f"[{sym}] DCA fire skipped — "
                f"below minimum contract size; "
                f"NOT marked as fired"
            )
            continue

        if oid is None:
            log.error(
                f"[{sym}] DCA fire rejected by MEXC; "
                f"NOT marked as fired"
            )
            continue

        mark_fired(
            sym,
            today,
            {
                "symbol": sym,
                "date": today.isoformat(),
                "order_id": oid,
                "limit_price": target,
                "sizing_price": target,
                "mark_at_fire": mark,
                "usd": daily_usd,
                "symbol_total_budget": total_budget,
            }
        )


# ── SVG status table ─────────────────────────────────────────────────────────

def render_svg(
    marks: Dict[str, float],
    highs: Dict[str, Optional[float]],
    today: datetime.date
) -> str:

    W = 1180
    H = 60 + 26 * len(SYMBOLS)

    now_str = (
        datetime.datetime.now(UTC)
        .strftime("%Y-%m-%d %H:%M UTC")
    )

    svg = [
        '<?xml version="1.0" encoding="UTF-8"?>',

        (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {W} {H}" '
            f'width="100%" '
            f'style="max-width:{W}px;display:block">'
        ),

        (
            f'<rect width="{W}" height="{H}" '
            f'fill="#fafafa"/>'
        ),

        (
            f'<text x="20" y="24" '
            f'font-family="Courier New" '
            f'font-size="13" '
            f'fill="#333" '
            f'font-weight="bold">'
            f'DCA5-Bot — 90-Day DCA Short '
            f'(priced + sized at 9d high)  '
            f'{now_str}</text>'
        ),
    ]

    y = 50

    for sym in SYMBOLS:

        mark = marks.get(
            sym,
            0.0
        )

        high = highs.get(sym)

        high_str = (
            f"{high:,.4f}"
            if high is not None
            else "n/a"
        )

        start = DCA_START_DATE[sym]

        end = (
            start +
            datetime.timedelta(
                days=DCA_DAYS - 1
            )
        )

        n_fired = fired_count(sym)

        active = in_dca_window(
            sym,
            today
        )

        fired_today = has_fired_today(
            sym,
            today
        )

        symbol_budget = DCA_BUDGET_USD[sym]

        daily_budget = DCA_DAILY_USD[sym]

        remaining_usd = max(
            0.0,
            symbol_budget -
            n_fired * daily_budget
        )

        if today < start:

            phase = (
                f"not started "
                f"(begins {start.isoformat()})"
            )

        elif today > end:

            phase = (
                f"window complete "
                f"({end.isoformat()})"
            )

        else:

            phase = (
                f"day "
                f"{(today - start).days + 1}"
                f"/{DCA_DAYS}"
            )

            if fired_today:
                phase += " — fired today"

            elif active:
                phase += " — pending today"

        clr = (
            "#1a8a1a"
            if fired_today
            else (
                "#aa1111"
                if active
                else "#888"
            )
        )

        line = (
            f"{sym:<18} "
            f"mark={mark:>12,.4f}  "
            f"9dHigh={high_str:>12}  "
            f"daily=${daily_budget:>10.6f}  "
            f"budget=${symbol_budget:>8.2f}  "
            f"fired={n_fired:>3}/{DCA_DAYS}  "
            f"remaining=${remaining_usd:>10.2f}  "
            f"{phase}"
        )

        svg.append(
            f'<text x="20" y="{y}" '
            f'font-family="Courier New" '
            f'font-size="10" '
            f'fill="{clr}">{line}</text>'
        )

        y += 26

    svg.append("</svg>")

    return "\n".join(svg)


# ── engine loop ──────────────────────────────────────────────────────────────

def _seconds_until_next_hour() -> float:

    now = time.time()

    return (
        (int(now) // 3600 + 1) * 3600
        + HOURLY_SLEEP_FLOOR_SEC
        - now
    )


def engine_cycle():

    now_utc = datetime.datetime.now(UTC)

    marks = {
        sym: get_mark(sym)
        for sym in SYMBOLS
    }

    highs = {
        sym: rolling_9d_high(sym)
        for sym in SYMBOLS
    }

    if now_utc.hour == 0:
        run_daily_dca(now_utc)

    svg = render_svg(
        marks,
        highs,
        now_utc.date()
    )

    STATE.set_svg(svg)

    n_fired_total = sum(
        len(v)
        for v in STATE_DATA["fired"].values()
    )

    STATE.set_status(
        f"ok  "
        f"{now_utc.strftime('%Y-%m-%d %H:%M UTC')}  "
        f"total_fires={n_fired_total}"
    )


def run_engine():

    load_specs()

    run_startup_test_orders()

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

        STATE.set_status(
            f"error: {e}"
        )

    while True:

        wait_s = _seconds_until_next_hour()

        time.sleep(
            max(0, wait_s)
        )

        try:
            engine_cycle()

        except Exception as e:

            log.error(
                f"engine cycle failed: {e}",
                exc_info=True
            )

            STATE.set_status(
                f"error: {e}"
            )


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

            svg = (
                STATE.get_svg()
                .encode("utf-8")
            )

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "image/svg+xml"
            )

            self.send_header(
                "Content-Length",
                str(len(svg))
            )

            self.send_header(
                "Cache-Control",
                "no-cache"
            )

            self.end_headers()

            self.wfile.write(svg)

        elif self.path == "/orders.json":

            body = json.dumps(
                STATE_DATA["orders"],
                indent=2
            ).encode("utf-8")

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "application/json"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(body)

        elif (
            self.path == "/"
            or self.path == ""
        ):

            status = STATE.get_status()

            html = (
                "<!doctype html>"
                "<html>"
                "<head>"
                "<meta charset='utf-8'>"
                "<meta http-equiv='refresh' content='300'>"
                "<title>DCA5-Bot Overview</title>"
                "<style>"
                "body{font-family:monospace;"
                "background:#fafafa;margin:24px}"
                "img{max-width:100%;height:auto;"
                "border:1px solid #ccc}"
                "</style>"
                "</head>"
                "<body>"
                "<h3>"
                "DCA5-Bot — 90-Day DCA Short Bot "
                "(priced + sized at 9d high)"
                "</h3>"
                f"<p>status: {status}</p>"
                "<img src='/chart.svg' "
                "alt='overview table'/>"
                "<p>"
                "<a href='/orders.json'>"
                "order records (JSON)"
                "</a>"
                "</p>"
                "</body>"
                "</html>"
            )

            body = html.encode(
                "utf-8"
            )

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(body)

        else:

            self.send_response(404)

            self.end_headers()


def run_server():

    server = (
        http.server.ThreadingHTTPServer(
            (
                HTTP_HOST,
                HTTP_PORT
            ),
            Handler
        )
    )

    log.info(
        f"server listening on "
        f"{HTTP_HOST}:{HTTP_PORT}"
    )

    server.serve_forever()


# ── entrypoint ────────────────────────────────────────────────────────────────

def main():

    if not MEXC_KEY or not MEXC_SECRET:

        log.error(
            "MEXC / MEXCSECRET not set"
        )

        raise SystemExit(1)

    log.info(
        "DCA budget configuration:"
    )

    for sym in SYMBOLS:
        log.info(
            f"  {sym}: "
            f"total=${DCA_BUDGET_USD[sym]:.8f} "
            f"daily=${DCA_DAILY_USD[sym]:.8f}"
        )

    log.info(
        f"Original-symbol combined budget: "
        f"${sum(DCA_BUDGET_USD[s] for s in ORIGINAL_SYMBOLS):.2f}"
    )

    log.info(
        f"New-stock combined budget: "
        f"${sum(DCA_BUDGET_USD[s] for s in NEW_STOCK_SYMBOLS):.2f}"
    )

    log.info(
        f"Grand configured DCA budget: "
        f"${sum(DCA_BUDGET_USD.values()):.2f}"
    )

    server_thread = threading.Thread(
        target=run_server,
        daemon=True
    )

    server_thread.start()

    run_engine()


if __name__ == "__main__":
    main()
