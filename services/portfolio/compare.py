from typing import Optional, List, Dict
from storage import get_holdings, get_latest_snapshot, save_snapshot
from prices import get_price
from datetime import datetime, timezone

def portfolio_value_now(symbol_filter: Optional[str] = None) -> float:
    total = 0.0
    for h in get_holdings():
        if symbol_filter and h["symbol"] != symbol_filter:
            continue
        q = float(h["quantity"])
        if q == 0:
            continue
        p = get_price(h["symbol"], h["market"])
        total += q * float(p["price_eur"])
    return total

def compare(scope: str, symbol: Optional[str]):
    snap = get_latest_snapshot(scope, symbol)
    now_value = portfolio_value_now(symbol)
    now_ts = datetime.now(timezone.utc).isoformat()

    if not snap:
        return {
            "scope": scope,
            "symbol": symbol,
            "baseline_ts": None,
            "current_ts": now_ts,
            "baseline_value_eur": None,
            "current_value_eur": now_value,
            "change_abs": None,
            "change_pct": None,
            "note": "No snapshot found. Schedule POST /snapshot at 08:00 Europe/Berlin for this scope to enable comparisons.",
        }

    base = float(snap["value_eur"])
    delta = now_value - base
    pct = None if base == 0 else delta / base * 100.0
    return {
        "scope": scope,
        "symbol": symbol,
        "baseline_ts": snap["ts"],
        "current_ts": now_ts,
        "baseline_value_eur": base,
        "current_value_eur": now_value,
        "change_abs": delta,
        "change_pct": pct,
        "note": None,
    }

def snapshot_now(scope: str, symbol: Optional[str]):
    val = portfolio_value_now(symbol)
    save_snapshot(scope, symbol, val)
    return val
