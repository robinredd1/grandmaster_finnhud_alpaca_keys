
# ====== bot.py ======
import os, sys, time, math, asyncio, itertools
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Tuple
import httpx

from config import *

HEADERS_ALPACA = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
}

FINNHUB_BASE = "https://finnhub.io/api/v1"

def now_utc():
    return datetime.now(timezone.utc)

async def fetch_symbols_finnhub(client: httpx.AsyncClient) -> List[str]:
    url = f"{FINNHUB_BASE}/stock/symbol"
    params = {"exchange":"US", "token": FINNHUB_API_KEY}
    r = await client.get(url, params=params, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    syms = []
    for d in data:
        s = d.get("symbol")
        typ = d.get("type","")
        if not s: continue
        if typ in ("Common Stock","ETF","ADR","REIT","ETN","Fund") or typ == "":
            if s.isupper() and s.isascii():
                syms.append(s)
    return sorted(set(syms))

async def fetch_quote(client: httpx.AsyncClient, sym: str) -> Tuple[str, Dict[str, Any]]:
    url = f"{FINNHUB_BASE}/quote"
    params = {"symbol": sym, "token": FINNHUB_API_KEY}
    try:
        r = await client.get(url, params=params, timeout=10.0)
        if r.status_code != 200:
            return sym, {}
        q = r.json()
        if not isinstance(q, dict) or not q.get("c"):
            return sym, {}
        return sym, q
    except Exception:
        return sym, {}

async def fetch_batch_quotes(symbols: List[str], concurrency: int) -> Dict[str, Dict[str, Any]]:
    out = {}
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        async def worker(sym):
            async with sem:
                s, q = await fetch_quote(client, sym)
                if q:
                    out[s] = q
        await asyncio.gather(*[worker(s) for s in symbols])
    return out

def dollars_to_qty(price: float, dollars: float) -> str:
    if not price or price <= 0: return "0"
    return f"{max(dollars/price, 0.0):.4f}"

def rank_by_momentum(quotes: Dict[str, Dict[str, Any]]) -> List[Tuple[str, float, float, float]]:
    ranked = []
    for s, q in quotes.items():
        price = float(q.get("c", 0) or 0)
        day_pct = float(q.get("dp", 0) or 0)
        mom = day_pct
        if price > 0:
            ranked.append((s, mom, day_pct, price))
    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked

def qualifies(day_pct: float, mom: float) -> bool:
    return (mom >= MIN_1MOMENTUM_PCT) and (day_pct >= MIN_DAY_PCT)

def alpaca_get(session: httpx.Client, path: str, params=None):
    r = session.get(ALPACA_BROKER_BASE + path, headers=HEADERS_ALPACA, timeout=20.0, params=params)
    r.raise_for_status()
    return r.json()

def alpaca_post(session: httpx.Client, path: str, payload: dict):
    r = session.post(ALPACA_BROKER_BASE + path, headers=HEADERS_ALPACA, timeout=20.0, json=payload)
    r.raise_for_status()
    return r.json()

def ensure_trailing_stops(session: httpx.Client, trail_percent: float):
    try:
        open_orders = alpaca_get(session, "/v2/orders", params={"status":"open"})
    except Exception as e:
        print(f"[WARN] open orders: {e}")
        open_orders = []
    has_sell = {o.get("symbol"): True for o in open_orders if o.get("side")=="sell"}
    try:
        positions = alpaca_get(session, "/v2/positions")
    except Exception as e:
        print(f"[WARN] positions: {e}")
        positions = []
    for p in positions:
        sym = p.get("symbol"); qty = p.get("qty")
        if sym and qty and not has_sell.get(sym):
            try:
                alpaca_post(session, "/v2/orders", {
                    "symbol": sym, "qty": qty, "side":"sell", "type":"trailing_stop",
                    "time_in_force":"day", "trail_percent": float(trail_percent),
                    "extended_hours": bool(USE_EXTENDED_HOURS),
                })
                print(f"[EXIT] Trailing stop submitted for {sym} trail {trail_percent}%")
            except Exception as e:
                print(f"[WARN] trailing stop {sym}: {e}")

def submit_limit_buy(session: httpx.Client, sym: str, price: float, dollars: float):
    qty = dollars_to_qty(price, dollars)
    if float(qty) <= 0: return None
    limit_price = round(price * (1.0 + LIMIT_SLIPPAGE_BPS/10000.0), 4)
    payload = {"symbol": sym, "qty": qty, "side":"buy", "type":"limit",
               "time_in_force":"day", "limit_price": limit_price,
               "extended_hours": bool(USE_EXTENDED_HOURS)}
    return alpaca_post(session, "/v2/orders", payload)

def submit_time_exit(session: httpx.Client, sym: str, qty: str, price_hint: float=None):
    try:
        r = alpaca_post(session, "/v2/orders", {"symbol":sym,"qty":qty,"side":"sell","type":"market","time_in_force":"day"})
        print(f"[TIME-EXIT] Market sell submitted for {sym} qty={qty}")
        return r
    except Exception as e:
        if price_hint:
            try:
                lp = round(price_hint * 0.995, 4)
                r2 = alpaca_post(session, "/v2/orders", {"symbol":sym,"qty":qty,"side":"sell","type":"limit",
                                                         "time_in_force":"day","limit_price": lp,
                                                         "extended_hours": bool(USE_EXTENDED_HOURS)})
                print(f"[TIME-EXIT] Limit sell submitted for {sym} qty={qty}")
                return r2
            except Exception as e2:
                print(f"[WARN] time-exit {sym}: {e2}")
        else:
            print(f"[WARN] time-exit {sym}: {e}")
    return None

async def main():
    print("=== Grandmaster Finnhub (data) + Alpaca Paper (orders) — Hardcoded Keys ===")
    async with httpx.AsyncClient() as ac:
        if UNIVERSE_MODE.upper() == "AUTO":
            print("[UNIVERSE] Fetching symbols from Finnhub...")
            syms = await fetch_symbols_finnhub(ac)
            print(f"[UNIVERSE] Loaded {len(syms)} US symbols")
        else:
            if not os.path.exists(UNIVERSE_FILE):
                print(f"[FATAL] Universe file {UNIVERSE_FILE} not found."); return
            with open(UNIVERSE_FILE,"r") as f:
                syms = [ln.strip().upper() for ln in f if ln.strip() and not ln.startswith("#")]
            syms = sorted(set(syms))
            print(f"[UNIVERSE] Loaded {len(syms)} symbols from file")

    def batches(seq, n):
        it = iter(seq)
        while True:
            chunk = tuple(itertools.islice(it, n))
            if not chunk: return
            yield chunk
    batch_iter = itertools.cycle(list(batches(syms, SCAN_BATCH_SIZE)))

    alp = httpx.Client()

    first_loop = True
    opened_at = {}
    timed_out = {}

    while True:
        try:
            positions = alpaca_get(alp, "/v2/positions")
        except Exception as e:
            print(f"[WARN] positions: {e}")
            positions = []
        now = now_utc()
        for p in positions:
            sym = p.get("symbol")
            if sym not in opened_at:
                opened_at[sym] = now
        active = {p.get("symbol") for p in positions}
        for s in list(opened_at.keys()):
            if s not in active:
                opened_at.pop(s, None)
                timed_out.pop(s, None)

        if TIME_EXIT_MINUTES and TIME_EXIT_MINUTES > 0:
            cutoff = now - timedelta(minutes=TIME_EXIT_MINUTES)
            for p in positions:
                sym = p.get("symbol"); qty = p.get("qty")
                if sym in opened_at and opened_at[sym] <= cutoff and not timed_out.get(sym):
                    last = None
                    try: last = float(p.get("current_price"))
                    except: pass
                    submit_time_exit(alp, sym, qty, last)
                    timed_out[sym] = True

        if len(positions) >= MAX_OPEN_POSITIONS:
            ensure_trailing_stops(alp, TRAIL_PERCENT)
            time.sleep(BASE_SCAN_DELAY); continue

        batch = next(batch_iter)
        print(f"[SCAN] {len(batch)} symbols @ {datetime.now().strftime('%H:%M:%S')}")

        quotes = await fetch_batch_quotes(list(batch), CONCURRENCY)
        if not quotes:
            print("[WARN] No quotes returned in this batch (throttling or off-hours).")
            time.sleep(BASE_SCAN_DELAY); continue

        ranked = rank_by_momentum(quotes)
        ranked = [(s,m,dp,pr) for (s,m,dp,pr) in ranked if qualifies(dp, m)]
        if not ranked and FORCE_BUY_ON_FIRST_PASS and first_loop:
            for s,q in list(quotes.items())[:TAKE_PER_SCAN]:
                price = float(q.get("c",0) or 0)
                if price <= 0: continue
                ranked.append((s, 0.0, float(q.get("dp",0) or 0), price))

        if ranked:
            print("[TOP]", " | ".join([f"{s}: mom {m:+.2f}% day {d:+.2f}% @ {p:.2f}" for s,m,d,p in ranked[:10]]))
        else:
            print("[TOP] No qualifiers this pass.")

        slots = max(MAX_OPEN_POSITIONS - len(positions), 0)
        to_take = min(slots, TAKE_PER_SCAN)
        if to_take > 0 and ranked:
            picks = ranked[:to_take]
            for s, m, d, price in picks:
                try:
                    order = submit_limit_buy(alp, s, price, DOLLARS_PER_TRADE)
                    if order:
                        print(f"[ENTRY] {s} qty={order.get('qty')} lim={order.get('limit_price')} | mom {m:+.2f}% day {d:+.2f}%")
                except Exception as e:
                    print(f"[ERR] order {s}: {e}")
        ensure_trailing_stops(alp, TRAIL_PERCENT)
        first_loop = False
        time.sleep(BASE_SCAN_DELAY)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
