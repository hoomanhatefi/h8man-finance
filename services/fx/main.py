import os, json, time, sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

EODHD_KEY = os.getenv("EODHD_KEY", "")
FX_TTL_SEC = int(os.getenv("FX_TTL_SEC", "86400"))  # 24h cache
DB_PATH = os.getenv("DB_PATH", "cache.db")

app = FastAPI(title="FX Service")

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS cache("
        "k TEXT PRIMARY KEY, v TEXT NOT NULL, ts INTEGER NOT NULL)"
    )
    return conn

def cache_get(k: str, ttl: int):
    conn = db()
    row = conn.execute("SELECT v, ts FROM cache WHERE k=?", (k,)).fetchone()
    conn.close()
    if not row:
        return None
    v, ts = row
    if time.time() - ts > ttl:
        return None
    return json.loads(v)

def cache_put(k: str, v: dict):
    conn = db()
    conn.execute(
        "REPLACE INTO cache(k, v, ts) VALUES(?,?,?)",
        (k, json.dumps(v), int(time.time())),
    )
    conn.commit()
    conn.close()

class FxResp(BaseModel):
    pair: str
    rate: float          # EUR per 1 USD
    source: str
    fetched_at: int
    ttl_sec: int

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

async def fetch_usdeur_from_eodhd(client: httpx.AsyncClient) -> float | None:
    """
    Use EODHD real-time for EURUSD.FOREX and invert:
      EURUSD close = USD per EUR  => USD_EUR = 1 / close
    """
    if not EODHD_KEY:
        return None
    url = f"https://eodhd.com/api/real-time/EURUSD.FOREX?api_token={EODHD_KEY}&fmt=json"
    try:
        r = await client.get(url, timeout=12)
        if r.status_code != 200:
            return None
        data = r.json()
        # some responses are list-like; normalize
        if isinstance(data, list) and data:
            data = data[0]
        close = float(data.get("close") or 0)
        if close <= 0:
            return None
        return 1.0 / close
    except Exception:
        return None

async def fetch_usdeur_from_ecb(client: httpx.AsyncClient) -> float | None:
    """
    Fallback via exchangerate.host (ECB based): base=USD&symbols=EUR already returns USD->EUR
    """
    try:
        r = await client.get("https://api.exchangerate.host/latest?base=USD&symbols=EUR", timeout=12)
        if r.status_code != 200:
            return None
        data = r.json()
        rate = data.get("rates", {}).get("EUR")
        return float(rate) if rate and float(rate) > 0 else None
    except Exception:
        return None

@app.get("/fx", response_model=FxResp)
async def fx(pair: str = "USD_EUR", force: bool = False):
    if pair.upper() != "USD_EUR":
        raise HTTPException(400, "only USD_EUR supported for now")

    key = "fx:USD_EUR"
    if not force:
        cached = cache_get(key, FX_TTL_SEC)
        if cached:
            return FxResp(**cached)

    async with httpx.AsyncClient() as client:
        rate = await fetch_usdeur_from_eodhd(client)
        src = "eodhd"
        if rate is None:
            rate = await fetch_usdeur_from_ecb(client)
            src = "exchangerate.host-ecb" if rate is not None else None

    if rate is None:
        raise HTTPException(502, "Failed to fetch USD_EUR from both sources")

    payload = FxResp(
        pair="USD_EUR",
        rate=rate,
        source=src or "unknown",
        fetched_at=int(time.time()),
        ttl_sec=FX_TTL_SEC,
    ).dict()
    cache_put(key, payload)
    return payload
