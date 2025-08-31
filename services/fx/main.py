import os
import json
import time
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

APP_TITLE = "fx"
app = FastAPI(title=APP_TITLE)

# ---------- config ----------
DB_PATH = Path(os.getenv("FX_CACHE_PATH") or os.getenv("DB_PATH") or "fx_cache.sqlite")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

FX_TTL_SEC = int(os.getenv("FX_TTL_SEC", str(6 * 60 * 60)))  # default 6h
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "4.0"))
EODHD_KEY = os.getenv("EODHD_KEY", "").strip()

EODHD_BASE = "https://eodhd.com/api/real-time"
SYMBOL_EURUSD = "EURUSD.FOREX"  # EURUSD → invert to get USD_EUR

# ---------- models ----------
class FxResp(BaseModel):
    pair: str
    rate: float
    source: str
    fetched_at: int
    ttl_sec: int

# ---------- sqlite cache ----------
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fx_cache(
            key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            expires_at INTEGER NOT NULL
        )
    """)
    return conn

def cache_get(key: str) -> Optional[dict]:
    conn = _db()
    try:
        row = conn.execute("SELECT payload, expires_at FROM fx_cache WHERE key=?", (key,)).fetchone()
        if not row:
            return None
        payload_json, expires_at = row
        if int(time.time()) >= expires_at:
            return None
        return json.loads(payload_json)
    finally:
        conn.close()

def cache_put(key: str, payload: dict):
    conn = _db()
    try:
        expires_at = int(time.time()) + FX_TTL_SEC
        conn.execute(
            "INSERT OR REPLACE INTO fx_cache(key, payload, expires_at) VALUES (?, ?, ?)",
            (key, json.dumps(payload), expires_at),
        )
        conn.commit()
    finally:
        conn.close()

# ---------- provider: EODHD ----------
async def fetch_usd_eur_eodhd() -> Tuple[Optional[float], Optional[str]]:
    if not EODHD_KEY:
        return None, "missing_eodhd_key"
    url = f"{EODHD_BASE}/{SYMBOL_EURUSD}"
    params = {"api_token": EODHD_KEY, "fmt": "json"}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return None, f"eodhd_http_{r.status_code}"
        js = r.json()
        eurusd = js.get("close") or js.get("price") or js.get("last")
        try:
            eurusd = float(eurusd)
        except Exception:
            return None, "eodhd_no_price"
        if eurusd <= 0:
            return None, "eodhd_bad_price"
        usd_eur = 1.0 / eurusd
        return usd_eur, "eodhd.com real-time"

# ---------- endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/fx/cache/{key}")
def get_cache_item(key: str):
    item = cache_get(key.upper())
    return {"key": key.upper(), "cached": bool(item), "value": item}

@app.get("/fx/usd-eur")
async def fx_usd_eur():
    """Shortcut for cached USD_EUR (no force refresh)."""
    key = "USD_EUR"
    cached = cache_get(key)
    if cached:
        return cached
    rate, src = await fetch_usd_eur_eodhd()
    if rate is None:
        raise HTTPException(status_code=502, detail=f"Failed to fetch {key}")
    payload = FxResp(pair=key, rate=rate, source=src, fetched_at=int(time.time()), ttl_sec=FX_TTL_SEC).dict()
    cache_put(key, payload)
    return payload

@app.get("/fx")
async def fx(pair: str = Query(..., description="Currency pair like USD_EUR"),
             force: bool = Query(False, description="Force refresh bypassing cache")):
    """Generic endpoint: /fx?pair=USD_EUR&force=true"""
    pair = pair.upper()
    if pair != "USD_EUR":
        raise HTTPException(status_code=400, detail="Only USD_EUR supported for now")

    if not force:
        cached = cache_get(pair)
        if cached:
            return cached

    rate, src = await fetch_usd_eur_eodhd()
    if rate is None:
        raise HTTPException(status_code=502, detail=f"Failed to fetch {pair}")
    payload = FxResp(pair=pair, rate=rate, source=src, fetched_at=int(time.time()), ttl_sec=FX_TTL_SEC).dict()
    cache_put(pair, payload)
    return payload
