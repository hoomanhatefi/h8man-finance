import os
import json
import time
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

APP_TITLE = "fx"
app = FastAPI(title=APP_TITLE)

# ---------- config ----------
PORT = int(os.getenv("PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "info")

# Support either FX_CACHE_PATH or DB_PATH. Prefer FX_CACHE_PATH if set.
_db_path_env = os.getenv("FX_CACHE_PATH") or os.getenv("DB_PATH") or "fx_cache.sqlite"
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / Path(_db_path_env).name

FX_TTL_SEC = int(os.getenv("FX_TTL_SEC", str(6 * 60 * 60)))  # default 6h
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "4.0"))
EODHD_KEY = os.getenv("EODHD_KEY", "").strip()

EODHD_BASE = "https://eodhd.com/api/real-time"
SYMBOL_EURUSD = "EURUSD.FOREX"  # EODHD symbol for EUR/USD

# ---------- models ----------
class FxResp(BaseModel):
    pair: str          # e.g. USD_EUR
    rate: float
    source: str
    fetched_at: int    # unix seconds
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
        row = conn.execute(
            "SELECT payload, expires_at FROM fx_cache WHERE key=?",
            (key,),
        ).fetchone()
        if not row:
            return None
        payload_json, expires_at = row
        now = int(time.time())
        if now >= expires_at:
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
    """
    EODHD returns EURUSD price (USD per 1 EUR).
    We need USD_EUR, so we return 1 / EURUSD.
    """
    if not EODHD_KEY:
        return None, "missing_eodhd_key"

    url = f"{EODHD_BASE}/{SYMBOL_EURUSD}"
    params = {"api_token": EODHD_KEY, "fmt": "json"}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return None, f"eodhd_http_{r.status_code}"
        js = r.json()
        # EODHD returns {"code": "...", "timestamp": ..., "gmtoffset":..., "open":..., "high":..., "low":..., "close":..., "previousClose":..., "change":..., "change_p":...}
        # Use "close" as the latest price
        eurusd = js.get("close") or js.get("price") or js.get("last")  # defensive
        try:
            eurusd = float(eurusd)
        except Exception:
            return None, "eodhd_no_price"
        if eurusd <= 0:
            return None, "eodhd_bad_price"
        usd_eur = 1.0 / eurusd
        return usd_eur, "eodhd.com real-time"
    # unreachable

# ---------- endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/fx/cache/{key}")
def get_cache_item(key: str):
    item = cache_get(key.upper())
    return {"key": key.upper(), "cached": bool(item), "value": item}

@app.get("/fx/usd-eur")
async def usd_eur(force: int = 0):
    """
    Returns USD to EUR rate with cache.
    Query param force=1 bypasses cache and refetches from EODHD.
    """
    key = "USD_EUR"

    if not force:
        cached = cache_get(key)
        if cached:
            return cached

    rate, src = await fetch_usd_eur_eodhd()
    if rate is None:
        raise HTTPException(status_code=502, detail=f"Failed to fetch USD_EUR: {src}")

    payload = FxResp(
        pair=key,
        rate=float(rate),
        source=src or "eodhd.com",
        fetched_at=int(time.time()),
        ttl_sec=FX_TTL_SEC,
    ).dict()

    cache_put(key, payload)
    return payload
