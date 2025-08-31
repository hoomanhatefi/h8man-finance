import os
import time
from datetime import datetime, timezone
import requests
from dateutil import parser as dateparser
from storage import get_price_cache, set_price_cache

EODHD_TOKEN = os.getenv("EODHD_API_TOKEN", "")
FX_BASE_URL = os.getenv("FX_BASE_URL", "http://fx:8000")
DEFAULT_USD_EUR = float(os.getenv("DEFAULT_USD_EUR", "0.92"))
PRICE_TTL_SEC = int(os.getenv("PRICE_TTL_SEC", "60"))

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def market_suffix(market: str) -> str:
    return "US" if market.upper() == "US" else "XETRA"

def eodhd_quote(symbol: str, market: str):
    # real-time endpoint, falls back cleanly
    url = f"https://eodhd.com/api/real-time/{symbol}.{market_suffix(market)}"
    params = {"api_token": EODHD_TOKEN, "fmt": "json"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    # expected fields: close, previousClose, timestamp
    price = data.get("close") or data.get("previousClose")
    ts = data.get("timestamp")
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if isinstance(ts, (int, float)) else now_iso()
    ccy = "USD" if market.upper() == "US" else "EUR"
    return float(price), ccy, ts_iso, "eodhd_realtime"

def fx_usd_eur():
    # expect fx service to return {"pair":"USD_EUR","rate":0.9,"as_of":"..."}
    try:
        r = requests.get(f"{FX_BASE_URL}/latest", timeout=5)
        r.raise_for_status()
        data = r.json()
        rate = float(data.get("rate"))
        return rate
    except Exception:
        return DEFAULT_USD_EUR

def get_price(symbol: str, market: str):
    # check cache
    cached = get_price_cache(symbol, market)
    if cached:
        try:
            fetched_at = dateparser.isoparse(cached["fetched_at"])
            age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
            if age < PRICE_TTL_SEC:
                return {
                    "symbol": symbol,
                    "market": market,
                    "price": float(cached["price"]),
                    "ccy": cached["ccy"],
                    "price_eur": float(cached["price"]) if cached["ccy"] == "EUR" else float(cached["price"]) * fx_usd_eur(),
                    "fx_used": None if cached["ccy"] == "EUR" else fx_usd_eur(),
                    "source": f"cache:{cached.get('source','unknown')}",
                    "fetched_at": cached["fetched_at"],
                }
        except Exception:
            pass

    price, ccy, ts_iso, source = eodhd_quote(symbol, market)
    set_price_cache(symbol, market, price, ccy, ts_iso, source)

    if ccy == "EUR":
        price_eur = price
        fx_used = None
    else:
        rate = fx_usd_eur()
        price_eur = price * rate
        fx_used = rate

    return {
        "symbol": symbol,
        "market": market,
        "price": price,
        "ccy": ccy,
        "price_eur": price_eur,
        "fx_used": fx_used,
        "source": source,
        "fetched_at": ts_iso,
    }
