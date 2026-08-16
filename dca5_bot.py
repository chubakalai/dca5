#!/usr/bin/env python3
"""
DCA5-Bot — Multi-Symbol 90-Day DCA Short Bot
priced and sized at rolling 9-day high

Behavior:
  - On startup: run one TEST order against EVERY symbol, sequentially.
    Each test order:
      * uses the symbol's current mark + 10% as the limit price
      * uses EXACTLY the symbol's minimum contract volume
      * therefore tests the minimum order-size threshold
      * waits TEST_ORDER_WAIT_SEC
      * cancels if still open
    A symbol failing its test does not prevent other symbols from being
    tested or the engine from starting.

  - Every hour on the hour:
      * refresh mark prices
      * refresh rolling 9-day highs
      * refresh the SVG status table

  - At hour == 00 UTC:
      * each symbol receives one daily DCA allocation
      * the allocation is ACCRUED in persistent state
      * if the accrued amount is not above that symbol's minimum
        executable notional, no order is placed
      * once accrued USD exceeds the minimum executable notional,
        one limit SHORT is placed for the entire accrued amount
      * the order is priced AND sized at the current rolling 9-day high
      * only after successful order placement is the accumulator reset
      * the current date is then recorded as fired

  - Existing symbols:
      * $1,000 total budget EACH over 90 days
      * $11.111111... daily allocation

  - New stock symbols:
      * $1,000 total shared budget
      * There are 8 new symbols in the supplied list
      * therefore $125 total per symbol
      * $1.388888... daily allocation per symbol

  - DCA orders are never automatically cancelled.
    Previous unfilled orders remain open and new orders may stack.

  - If a daily kline fetch fails or fewer than ROLL_DAYS closed bars
    are available, that symbol receives no daily allocation for that
    wake and is retried at the next midnight wake.

  - Fire history, order records, and accrued-but-not-yet-ordered DCA
    amounts are persisted to STATE_FILE.

  - The persisted accumulator is important: if a symbol has accumulated
    several days of DCA allocation and the process restarts, the
    accumulated amount survives the restart.

Environment:
  MEXC        - MEXC API key
  MEXCSECRET  - MEXC API secret

MEXC diagnostics carried over from previous live tests:
  - Kline endpoint returns parallel arrays.
  - realHigh is used for actual traded-price OHLC.
  - timestamps are Unix seconds.
  - open-orders symbol filtering is unreliable server-side, so this
    script filters by symbol client-side.
  - order-create response data is a dict containing orderId.
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

MEXC_KEY = os.getenv("MEXC")
MEXC_SECRET = os.getenv("MEXCSECRET")
MEXC_BASE = "https://api.mexc.co"


# ── symbols ──────────────────────────────────────────────────────────────────

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


LEVERAGE = 30
ROLL_DAYS = 9


# ── DCA budgets ──────────────────────────────────────────────────────────────

DCA_DAYS = 90

ORIGINAL_DCA_BUDGET_USD = 1000.0

# All 8 newly supplied stock symbols share this pool.
NEW_STOCK_GROUP_BUDGET_USD = 1000.0

DCA_BUDGET_USD: Dict[str, float] = {
    sym: ORIGINAL_DCA_BUDGET_USD
    for sym in ORIGINAL_SYMBOLS
}

NEW_STOCK_PER_SYMBOL_BUDGET_USD = (
    NEW_STOCK_GROUP_BUDGET_USD /
    len(NEW_STOCK_SYMBOLS)
)

for sym in NEW_STOCK_SYMBOLS:
    DCA_BUDGET_USD[sym] = NEW_STOCK_PER_SYMBOL_BUDGET_USD


DCA_DAILY_USD: Dict[str, float] = {
    sym: DCA_BUDGET_USD[sym] / DCA_DAYS
    for sym in SYMBOLS
}


# ── DCA dates ────────────────────────────────────────────────────────────────

DCA_START_DATE: Dict[str, datetime.date] = {
    sym: datetime.date(2026, 8, 1)
    for sym in SYMBOLS
}


def in_dca_window(
    sym: str,
    d: datetime.date
) -> bool:

    start = DCA_START_DATE[sym]

    end = (
        start +
        datetime.timedelta(days=DCA_DAYS - 1)
    )

    return start <= d <= end


# ── timing / testing ─────────────────────────────────────────────────────────

HOURLY_SLEEP_FLOOR_SEC = 5

TEST_ORDER_PREMIUM = 1.10
TEST_ORDER_WAIT_SEC = 60

HTTP_HOST = "0.0.0.0"
HTTP_PORT = int(
    os.getenv("PORT", "8080")
)

STATE_FILE = os.getenv(
    "DCA_STATE_FILE",
    "/data/dca_fire_history.json"
)


# ── logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s"
)

log = logging.getLogger()


# ── contract specifications ──────────────────────────────────────────────────

specs: Dict[str, Dict] = {}


# ── shared server state ──────────────────────────────────────────────────────

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
        "orders": [],
        "accrued": {},
    }


def load_state() -> Dict:
    """
    Load persisted state.

    State shape:

      {
        "fired": {
          "SYMBOL": [
            "YYYY-MM-DD",
            ...
          ]
        },

        "orders": [
          {...}
        ],

        "accrued": {
          "SYMBOL": 12.345678
        }
      }

    Older state files without "accrued" are automatically upgraded.
    """

    try:

        with open(
            STATE_FILE,
            "r"
        ) as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError(
                "state file did not contain a dict"
            )

        data.setdefault(
            "fired",
            {}
        )

        data.setdefault(
            "orders",
            []
        )

        data.setdefault(
            "accrued",
            {}
        )

        return data

    except FileNotFoundError:

        log.info(
            f"no state file at {STATE_FILE} "
            f"— starting fresh"
        )

        return _default_state()

    except Exception as e:

        log.error(
            f"state file at {STATE_FILE} unreadable "
            f"({e}) — starting fresh"
        )

        return _default_state()


def save_state(state: Dict):

    try:

        os.makedirs(
            os.path.dirname(STATE_FILE) or ".",
            exist_ok=True
        )

        tmp = STATE_FILE + ".tmp"

        with open(
            tmp,
            "w"
        ) as f:
            json.dump(
                state,
                f
            )

        os.replace(
            tmp,
            STATE_FILE
        )

    except Exception as e:

        log.error(
            f"failed to persist state to "
            f"{STATE_FILE}: {e}"
        )


STATE_DATA: Dict = load_state()


def has_fired_today(
    sym: str,
    d: datetime.date
) -> bool:

    return (
        d.isoformat()
        in STATE_DATA["fired"].get(
            sym,
            []
        )
    )


def fired_count(
    sym: str
) -> int:

    return len(
        STATE_DATA["fired"].get(
            sym,
            []
        )
    )


def get_accrued(
    sym: str
) -> float:

    try:
        return float(
            STATE_DATA["accrued"].get(
                sym,
                0.0
            )
        )
    except Exception:
        return 0.0


def set_accrued(
    sym: str,
    amount: float
):

    STATE_DATA["accrued"][sym] = max(
        0.0,
        float(amount)
    )


def record_successful_dca(
    sym: str,
    d: datetime.date,
    order_record: Dict
):
    """
    Reset the accumulated DCA bucket and record the fire only after
    the exchange accepted the order.
    """

    STATE_DATA["fired"].setdefault(
        sym,
        []
    ).append(
        d.isoformat()
    )

    STATE_DATA["orders"].append(
        order_record
    )

    STATE_DATA["accrued"][sym] = 0.0

    save_state(
        STATE_DATA
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
            sorted(
                params.items()
            )
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

        return json.loads(
            r.read()
        )


# ── MEXC signed requests ─────────────────────────────────────────────────────

def mexc(
    method,
    endpoint,
    params=None,
    body=None
):

    params = params or {}

    ts = str(
        int(
            time.time() * 1000
        )
    )

    if method == "GET":

        sp = "&".join(
            f"{k}={v}"
            for k, v in sorted(
                params.items()
            )
        )

    else:

        sp = (
            json.dumps(
                body,
                separators=(
                    ",",
                    ":"
                ),
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
        "Accept": "application/json",
    }

    raw = (
        json.dumps(
            body,
            separators=(
                ",",
                ":"
            ),
            sort_keys=True
        ).encode()
        if body and method not in (
            "GET",
            "DELETE"
        )
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
                if method in (
                    "GET",
                    "DELETE"
                )
                else None
            )
        )

    except Exception as e:

        log.error(
            f"mexc {method} "
            f"{endpoint}: {e}"
        )

        return {}


# ── contract specs ────────────────────────────────────────────────────────────

def load_specs():

    """
    Fetch contract specifications for every symbol.

    No silent fallback. If a symbol does not exist in MEXC contract
    detail, the process exits rather than risking a bad order.
    """

    rows = (
        mexc(
            "GET",
            "/api/v1/contract/detail"
        ).get("data") or []
    )

    if not rows:

        log.error(
            "empty contract detail response "
            "from MEXC"
        )

        raise SystemExit(1)

    by_sym = {
        c.get(
            "symbol",
            ""
        ).upper(): c
        for c in rows
    }

    missing = [
        s
        for s in SYMBOLS
        if s not in by_sym
    ]

    if missing:

        log.error(
            "symbols not found in MEXC "
            f"contract detail: {missing}"
        )

        raise SystemExit(1)

    for sym in SYMBOLS:

        match = by_sym[sym]

        vu = float(
            match.get(
                "volUnit",
                1
            )
        )

        pu = float(
            match.get(
                "priceUnit",
                0.5
            )
        )

        cs = float(
            match.get(
                "contractSize",
                vu
            )
        )

        raw = (
            f"{vu:.10f}"
            .rstrip("0")
        )

        p = (
            len(
                raw.split(".")[1]
            )
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
    ).get(
        "t",
        0.5
    )


def _prec(sym):

    return specs.get(
        sym,
        {}
    ).get(
        "p",
        0
    )


def _rfmt_price(
    sym,
    v
):

    t = _tick(sym)

    r = (
        round(
            v / t
        ) * t
    )

    s = (
        f"{t:.10f}"
        .rstrip("0")
    )

    dec = (
        len(
            s.split(".")[1]
        )
        if "." in s
        else 0
    )

    return (
        f"{r:.{dec}f}"
    )


def _rfmt_vol(
    sym,
    v
):

    p = _prec(sym)

    if p >= 0:

        return (
            f"{round(v, p):.{p}f}"
        )

    d = 10 ** abs(p)

    return str(
        int(
            round(
                v / d
            ) * d
        )
    )


def _min_contracts(
    sym
) -> float:

    return float(
        specs.get(
            sym,
            {}
        ).get(
            "vu",
            1.0
        )
    )


def _contract_size(
    sym
) -> float:

    return float(
        specs.get(
            sym,
            {}
        ).get(
            "cs",
            1.0
        )
    )


def _min_notional_usd(
    sym,
    price
) -> float:

    """
    Minimum executable notional at a given price.

    MEXC's minimum is expressed as contract volume, so the
    corresponding USD notional depends on the order price:

        min_contracts × contract_size × price
    """

    return (
        _min_contracts(sym)
        *
        _contract_size(sym)
        *
        price
    )


def _contracts(
    sym,
    usd,
    price
):

    """
    Calculate contract count for a USD notional at `price`.

    The returned volume is rounded according to the symbol's
    volume precision.
    """

    cs = _contract_size(sym)

    if price <= 0:
        return 0.0

    return float(
        _rfmt_vol(
            sym,
            max(
                0,
                usd / (
                    cs * price
                )
            )
        )
    )


# ── market data ──────────────────────────────────────────────────────────────

def get_mark(
    sym: str
) -> float:

    d = (
        mexc(
            "GET",
            "/api/v1/contract/ticker",
            params={
                "symbol": sym
            }
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


# ── orders ───────────────────────────────────────────────────────────────────

def _open_orders_for_sym(
    sym: str
) -> List[Dict]:

    """
    MEXC's symbol query parameter is not considered reliable.
    Filter the returned list by symbol locally.
    """

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

    if isinstance(
        data,
        dict
    ):

        data = data.get(
            "resultList",
            []
        )

    return [
        o
        for o in data
        if o.get(
            "symbol",
            ""
        ).upper() == sym
    ]


def _open_ids(
    sym: str
) -> set:

    return {
        str(
            o.get(
                "orderId",
                ""
            )
        )
        for o in _open_orders_for_sym(sym)
    }


def place_short(
    sym: str,
    limit_price: float,
    sizing_price: float,
    usd_amount: float
) -> Optional[str]:

    """
    Place a limit SHORT order.

    For normal DCA orders:
        sizing_price == limit_price == 9d high.

    For startup tests:
        sizing_price == mark,
        limit_price == mark * 1.10.

    Returns:
        order ID
        "SKIP" if below minimum volume
        None on exchange rejection
    """

    vol = _contracts(
        sym,
        usd_amount,
        sizing_price
    )

    min_vol = _min_contracts(sym)

    if vol < min_vol:

        log.warning(
            f"[{sym}] calculated volume "
            f"{vol} < minimum "
            f"{min_vol} "
            f"for ${usd_amount:.8f} "
            f"at price {sizing_price:.8f}"
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
        ),
    }

    r = mexc(
        "POST",
        "/api/v1/private/order/create",
        body=body
    )

    if not r.get("success"):

        log.error(
            f"[{sym}] short order rejected: "
            f"{r}"
        )

        return None

    data = (
        r.get("data")
        or {}
    )

    if not isinstance(
        data,
        dict
    ):

        log.error(
            f"[{sym}] unexpected 'data' shape "
            f"from order/create: {data!r}"
        )

        return None

    oid = data.get(
        "orderId"
    )

    if not oid:

        log.error(
            f"[{sym}] order/create succeeded "
            f"but no 'orderId' in data: "
            f"{data!r}"
        )

        return None

    oid = str(oid)

    actual_notional = (
        vol
        *
        _contract_size(sym)
        *
        limit_price
    )

    log.info(
        f"[{sym}] limit SHORT "
        f"vol={_rfmt_vol(sym, vol)} "
        f"@ {_rfmt_price(sym, limit_price)} "
        f"id={oid} "
        f"requested_usd={usd_amount:.8f} "
        f"actual_limit_notional="
        f"${actual_notional:.8f} "
        f"sizing_price={sizing_price:.8f}"
    )

    return oid


def place_minimum_test_short(
    sym: str,
    limit_price: float
) -> Optional[str]:

    """
    Place exactly the symbol's minimum contract volume.

    This is deliberately NOT calculated through the normal USD
    sizing path. The purpose is to test the exchange's minimum
    order-volume threshold directly.

    The resulting order notional is:

        minimum_volume
        × contract_size
        × limit_price

    That is the exact minimum executable notional at this price.
    """

    min_vol = _min_contracts(sym)

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
            min_vol
        ),
    }

    test_price = float(
        _rfmt_price(
            sym,
            limit_price
        )
    )

    threshold_notional = (
        min_vol
        *
        _contract_size(sym)
        *
        test_price
    )

    log.info(
        f"[{sym}] MINIMUM test: "
        f"vol={_rfmt_vol(sym, min_vol)} "
        f"price={test_price:.10f} "
        f"threshold_notional="
        f"${threshold_notional:.10f}"
    )

    r = mexc(
        "POST",
        "/api/v1/private/order/create",
        body=body
    )

    if not r.get("success"):

        log.error(
            f"[{sym}] minimum-threshold test "
            f"order rejected: {r}"
        )

        return None

    data = (
        r.get("data")
        or {}
    )

    if not isinstance(
        data,
        dict
    ):

        log.error(
            f"[{sym}] minimum-threshold test "
            f"returned unexpected data shape: "
            f"{data!r}"
        )

        return None

    oid = data.get(
        "orderId"
    )

    if not oid:

        log.error(
            f"[{sym}] minimum-threshold test "
            f"succeeded but no orderId: "
            f"{data!r}"
        )

        return None

    oid = str(oid)

    log.info(
        f"[{sym}] MINIMUM test order placed "
        f"successfully: id={oid} "
        f"vol={_rfmt_vol(sym, min_vol)} "
        f"@ {test_price:.10f} "
        f"threshold_notional="
        f"${threshold_notional:.10f}"
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
            f"[{sym}] cancelled "
            f"order id={oid}"
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

    return (
        oid not in
        _open_ids(sym)
    )


# ── daily klines / rolling high ──────────────────────────────────────────────

def fetch_daily_bars(
    sym: str,
    lookback_days: int
) -> List[Dict]:

    now_s = int(
        time.time()
    )

    start_s = (
        now_s -
        (lookback_days + 2)
        * 86400
    )

    url = (
        f"{MEXC_BASE}/api/v1/contract/kline/"
        f"{sym}"
        f"?interval=Day1"
        f"&start={start_s}"
        f"&end={now_s}"
    )

    try:

        raw = _get(url)

    except Exception as e:

        log.error(
            f"[{sym}] daily kline "
            f"fetch failed: {e}"
        )

        return []

    if not raw.get("success"):

        log.error(
            f"[{sym}] daily kline "
            f"fetch unsuccessful: {raw}"
        )

        return []

    d = (
        raw.get("data")
        or {}
    )

    times = (
        d.get("time")
        or []
    )

    highs = (
        d.get("realHigh")
        or d.get("high")
        or []
    )

    bars = []

    for i in range(
        len(times)
    ):

        if i >= len(highs):
            break

        t_s = int(
            times[i]
        )

        if (
            t_s + 86400
            > now_s
        ):
            continue

        bars.append({
            "t": t_s * 1000,
            "h": float(
                highs[i]
            ),
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
            f"[{sym}] only "
            f"{len(bars)} closed daily "
            f"bars available, need "
            f"{ROLL_DAYS} — cannot "
            f"compute 9d high"
        )

        return None

    window = bars[
        -ROLL_DAYS:
    ]

    return max(
        b["h"]
        for b in window
    )


# ── startup minimum-order tests ──────────────────────────────────────────────

def run_startup_test_order_for(
    sym: str
):

    log.info(
        f"── startup MINIMUM test "
        f"[{sym}]: begin ───────────────"
    )

    try:

        mark = get_mark(sym)

        if mark <= 0:

            log.error(
                f"[{sym}] test aborted: "
                f"invalid mark price "
                f"({mark})"
            )

            return

        test_price = (
            mark *
            TEST_ORDER_PREMIUM
        )

        rounded_test_price = float(
            _rfmt_price(
                sym,
                test_price
            )
        )

        min_vol = _min_contracts(
            sym
        )

        threshold_notional = (
            min_vol
            *
            _contract_size(sym)
            *
            rounded_test_price
        )

        log.info(
            f"[{sym}] minimum test: "
            f"mark={mark:.10f} "
            f"limit={rounded_test_price:.10f} "
            f"+10% "
            f"minimum_vol="
            f"{_rfmt_vol(sym, min_vol)} "
            f"threshold="
            f"${threshold_notional:.10f}"
        )

        oid = place_minimum_test_short(
            sym,
            rounded_test_price
        )

        if oid is None:

            log.error(
                f"[{sym}] minimum-threshold "
                f"test FAILED"
            )

            return

        log.info(
            f"[{sym}] minimum-threshold "
            f"test order placed id={oid}; "
            f"waiting {TEST_ORDER_WAIT_SEC}s"
        )

        time.sleep(
            TEST_ORDER_WAIT_SEC
        )

        if is_filled(
            sym,
            oid
        ):

            log.warning(
                f"[{sym}] minimum test order "
                f"id={oid} FILLED during "
                f"the {TEST_ORDER_WAIT_SEC}s "
                f"wait — this is now a real "
                f"open short position. "
                f"Review MEXC manually."
            )

        else:

            cancelled = cancel_order(
                sym,
                oid
            )

            if cancelled:

                log.info(
                    f"[{sym}] minimum-threshold "
                    f"test PASSED: order placed "
                    f"at exact minimum volume "
                    f"and cancelled successfully"
                )

            else:

                log.error(
                    f"[{sym}] minimum-threshold "
                    f"test order could not be "
                    f"cancelled — check MEXC"
                )

    except Exception as e:

        log.error(
            f"[{sym}] startup minimum test "
            f"failed: {e}",
            exc_info=True
        )

    log.info(
        f"── startup MINIMUM test "
        f"[{sym}]: end ─────────────────"
    )


def run_startup_test_orders():

    log.info(
        f"══ startup minimum-order tests: "
        f"{len(SYMBOLS)} symbols, "
        f"~{TEST_ORDER_WAIT_SEC}s each ══"
    )

    for sym in SYMBOLS:

        run_startup_test_order_for(
            sym
        )

    log.info(
        "══ startup minimum-order tests: "
        "all symbols done ══"
    )


# ── daily DCA engine ─────────────────────────────────────────────────────────

def run_daily_dca(
    now_utc: datetime.datetime
):

    """
    Add today's DCA allocation to each symbol's persistent accumulator.

    The accumulator is only converted into an order once it is above
    the symbol's current minimum executable notional.

    Example:

        daily allocation = $1.3889
        minimum notional = $10

        Day 1:  accrued $1.3889 -> no order
        Day 2:  accrued $2.7778 -> no order
        ...
        Day 8:  accrued $11.1112 -> order
                accumulator resets to $0

    If order placement fails, the accumulator is NOT reset, so the
    accumulated amount remains available for the next midnight retry.
    """

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
                f"for {today.isoformat()} "
                f"— skipping"
            )
            continue

        target = rolling_9d_high(
            sym
        )

        if target is None:

            log.error(
                f"[{sym}] DCA: could not "
                f"compute 9d high — "
                f"today's allocation is "
                f"NOT accrued; retrying "
                f"next midnight"
            )

            continue

        mark = get_mark(
            sym
        )

        if mark <= 0:

            log.error(
                f"[{sym}] DCA: invalid "
                f"mark price ({mark}) — "
                f"today's allocation is "
                f"NOT accrued"
            )

            continue

        daily_usd = (
            DCA_DAILY_USD[sym]
        )

        previous_accrued = (
            get_accrued(sym)
        )

        accrued = (
            previous_accrued
            +
            daily_usd
        )

        set_accrued(
            sym,
            accrued
        )

        min_vol = _min_contracts(
            sym
        )

        min_notional = (
            min_vol
            *
            _contract_size(sym)
            *
            target
        )

        log.info(
            f"[{sym}] DCA allocation: "
            f"daily=${daily_usd:.8f} "
            f"previous_accrued="
            f"${previous_accrued:.8f} "
            f"new_accrued="
            f"${accrued:.8f} "
            f"minimum_at_9dHigh="
            f"${min_notional:.8f} "
            f"target={target:.8f} "
            f"mark={mark:.8f}"
        )

        # "exceeds" the minimum rather than merely equals it.
        if accrued <= min_notional:

            log.info(
                f"[{sym}] DCA: accumulated "
                f"${accrued:.8f} is not above "
                f"minimum ${min_notional:.8f}; "
                f"no order today"
            )

            save_state(
                STATE_DATA
            )

            continue

        # The entire accumulated amount is placed.
        #
        # Sizing uses target itself, because the order is priced at
        # target and the intended notional is the accumulated amount.
        oid = place_short(
            sym,
            target,
            target,
            accrued
        )

        if oid == "SKIP":

            log.warning(
                f"[{sym}] DCA: accumulated "
                f"${accrued:.8f} still produced "
                f"a below-minimum contract "
                f"volume after rounding; "
                f"keeping accumulator intact"
            )

            save_state(
                STATE_DATA
            )

            continue

        if oid is None:

            log.error(
                f"[{sym}] DCA: order rejected; "
                f"keeping accumulated "
                f"${accrued:.8f} for retry"
            )

            save_state(
                STATE_DATA
            )

            continue

        actual_volume = _contracts(
            sym,
            accrued,
            target
        )

        actual_notional = (
            actual_volume
            *
            _contract_size(sym)
            *
            target
        )

        log.info(
            f"[{sym}] DCA FIRE: "
            f"date={today.isoformat()} "
            f"accumulated=${accrued:.8f} "
            f"actual_order_notional="
            f"${actual_notional:.8f} "
            f"min_notional="
            f"${min_notional:.8f} "
            f"9dHigh={target:.8f} "
            f"order_id={oid}"
        )

        record_successful_dca(
            sym,
            today,
            {
                "symbol": sym,
                "date": today.isoformat(),
                "order_id": oid,
                "limit_price": target,
                "sizing_price": target,
                "mark_at_fire": mark,
                "usd": accrued,
                "actual_order_notional": actual_notional,
                "minimum_notional_at_fire": min_notional,
                "daily_allocation": daily_usd,
                "symbol_total_budget": DCA_BUDGET_USD[sym],
                "accumulated_days_in_order": round(
                    accrued / daily_usd,
                    8
                ),
            }
        )


# ── SVG status ───────────────────────────────────────────────────────────────

def render_svg(
    marks: Dict[str, float],
    highs: Dict[str, Optional[float]],
    today: datetime.date
) -> str:

    W = 1400

    H = (
        70 +
        30 * len(SYMBOLS)
    )

    now_str = (
        datetime.datetime.now(UTC)
        .strftime(
            "%Y-%m-%d %H:%M UTC"
        )
    )

    svg = [

        '<?xml version="1.0" '
        'encoding="UTF-8"?>',

        (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {W} {H}" '
            f'width="100%" '
            f'style="max-width:{W}px;'
            f'display:block">'
        ),

        (
            f'<rect width="{W}" '
            f'height="{H}" '
            f'fill="#fafafa"/>'
        ),

        (
            f'<text x="20" y="24" '
            f'font-family="Courier New" '
            f'font-size="13" '
            f'fill="#333" '
            f'font-weight="bold">'
            f'DCA5-Bot — 90-Day DCA Short '
            f'(9d high pricing/sizing) '
            f'{now_str}</text>'
        ),

        (
            f'<text x="20" y="45" '
            f'font-family="Courier New" '
            f'font-size="10" '
            f'fill="#555">'
            f'Original symbols: $1,000 each | '
            f'New-stock pool: $1,000 total | '
            f'Orders fire only when accrued '
            f'amount exceeds minimum</text>'
        ),
    ]

    y = 70

    for sym in SYMBOLS:

        mark = marks.get(
            sym,
            0.0
        )

        high = highs.get(
            sym
        )

        high_str = (
            f"{high:,.4f}"
            if high is not None
            else "n/a"
        )

        start = DCA_START_DATE[
            sym
        ]

        end = (
            start +
            datetime.timedelta(
                days=DCA_DAYS - 1
            )
        )

        n_fired = fired_count(
            sym
        )

        active = in_dca_window(
            sym,
            today
        )

        fired_today = has_fired_today(
            sym,
            today
        )

        symbol_budget = (
            DCA_BUDGET_USD[sym]
        )

        daily_budget = (
            DCA_DAILY_USD[sym]
        )

        accrued = get_accrued(
            sym
        )

        min_notional = (
            (
                _min_contracts(sym)
                *
                _contract_size(sym)
                *
                high
            )
            if high is not None
            else None
        )

        if min_notional is not None:

            min_str = (
                f"${min_notional:,.4f}"
            )

            ready = (
                accrued >
                min_notional
            )

        else:

            min_str = "n/a"
            ready = False

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

                phase += (
                    " — order fired"
                )

            elif ready:

                phase += (
                    " — READY"
                )

            else:

                phase += (
                    " — accumulating"
                )

        if fired_today:

            clr = "#1a8a1a"

        elif ready:

            clr = "#aa1111"

        elif active:

            clr = "#555"

        else:

            clr = "#888"

        line = (
            f"{sym:<18} "
            f"mark={mark:>12,.4f}  "
            f"9dHigh={high_str:>12}  "
            f"daily=${daily_budget:>9.5f}  "
            f"accrued=${accrued:>10.5f}  "
            f"min={min_str:>11}  "
            f"fired={n_fired:>3}/{DCA_DAYS}  "
            f"{phase}"
        )

        svg.append(
            f'<text x="20" y="{y}" '
            f'font-family="Courier New" '
            f'font-size="10" '
            f'fill="{clr}">{line}</text>'
        )

        y += 30

    svg.append(
        "</svg>"
    )

    return "\n".join(
        svg
    )


# ── engine loop ──────────────────────────────────────────────────────────────

def _seconds_until_next_hour() -> float:

    now = time.time()

    return (
        (
            int(now) // 3600
            + 1
        )
        * 3600
        + HOURLY_SLEEP_FLOOR_SEC
        - now
    )


def engine_cycle():

    now_utc = (
        datetime.datetime.now(
            UTC
        )
    )

    marks = {
        sym: get_mark(sym)
        for sym in SYMBOLS
    }

    highs = {
        sym: rolling_9d_high(sym)
        for sym in SYMBOLS
    }

    if now_utc.hour == 0:

        run_daily_dca(
            now_utc
        )

        # Refresh high values after the DCA pass so the status page
        # represents the same current cycle.
        highs = {
            sym: rolling_9d_high(sym)
            for sym in SYMBOLS
        }

    svg = render_svg(
        marks,
        highs,
        now_utc.date()
    )

    STATE.set_svg(
        svg
    )

    n_fired_total = sum(
        len(v)
        for v in STATE_DATA[
            "fired"
        ].values()
    )

    total_accrued = sum(
        get_accrued(sym)
        for sym in SYMBOLS
    )

    STATE.set_status(
        f"ok  "
        f"{now_utc.strftime('%Y-%m-%d %H:%M UTC')}  "
        f"total_fires={n_fired_total}  "
        f"accrued=${total_accrued:.8f}"
    )


def run_engine():

    load_specs()

    run_startup_test_orders()

    log.info(
        "engine starting — "
        "running initial cycle"
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

        wait_s = (
            _seconds_until_next_hour()
        )

        time.sleep(
            max(
                0,
                wait_s
            )
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

            self.send_response(
                200
            )

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

            self.wfile.write(
                svg
            )

        elif self.path == "/orders.json":

            body = json.dumps(
                STATE_DATA["orders"],
                indent=2
            ).encode(
                "utf-8"
            )

            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "application/json"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(
                body
            )

        elif (
            self.path == "/"
            or self.path == ""
        ):

            status = (
                STATE.get_status()
            )

            html = (
                "<!doctype html>"
                "<html>"
                "<head>"
                "<meta charset='utf-8'>"
                "<meta http-equiv='refresh' "
                "content='300'>"
                "<title>DCA5-Bot Overview</title>"
                "<style>"
                "body{font-family:monospace;"
                "background:#fafafa;"
                "margin:24px}"
                "img{max-width:100%;"
                "height:auto;"
                "border:1px solid #ccc}"
                "</style>"
                "</head>"
                "<body>"
                "<h3>"
                "DCA5-Bot — 90-Day DCA Short Bot "
                "(9d-high pricing/sizing)"
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

            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(
                body
            )

        else:

            self.send_response(
                404
            )

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

    if (
        not MEXC_KEY
        or not MEXC_SECRET
    ):

        log.error(
            "MEXC / MEXCSECRET not set"
        )

        raise SystemExit(1)

    log.info(
        "════════ DCA BUDGET CONFIGURATION ════════"
    )

    for sym in SYMBOLS:

        log.info(
            f"{sym}: "
            f"total=${DCA_BUDGET_USD[sym]:.8f} "
            f"daily=${DCA_DAILY_USD[sym]:.8f}"
        )

    original_total = sum(
        DCA_BUDGET_USD[sym]
        for sym in ORIGINAL_SYMBOLS
    )

    new_stock_total = sum(
        DCA_BUDGET_USD[sym]
        for sym in NEW_STOCK_SYMBOLS
    )

    grand_total = sum(
        DCA_BUDGET_USD.values()
    )

    log.info(
        f"Original-symbol combined budget: "
        f"${original_total:.2f}"
    )

    log.info(
        f"New-stock combined budget: "
        f"${new_stock_total:.2f}"
    )

    log.info(
        f"Grand configured DCA budget: "
        f"${grand_total:.2f}"
    )

    log.info(
        "DCA orders accumulate daily until "
        "their current 9d-high notional is "
        "strictly above the symbol minimum."
    )

    log.info(
        "Startup tests use EXACTLY each "
        "symbol's minimum contract volume."
    )

    log.info(
        "══════════════════════════════════════════"
    )

    server_thread = threading.Thread(
        target=run_server,
        daemon=True
    )

    server_thread.start()

    run_engine()


if __name__ == "__main__":
    main()
