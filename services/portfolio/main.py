import os
from fastapi import FastAPI
import httpx

app = FastAPI(title="portfolio-logic")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/fx-check")
async def fx_check():
    base = os.getenv("FX_BASE_URL", "http://fx:8000")
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{base}/health")
        r.raise_for_status()
        return {"fx": r.json()}


from typing import Optional
from models import TxIn, TxOut, Holding, PortfolioOut, PortfolioRow, PriceOut, SnapshotIn, CompareOut
from storage import init_db, get_holdings, upsert_holding, record_tx
from prices import get_price
from compare import compare as cmp, snapshot_now

app = FastAPI(title="Portfolio Logic", version="1.0.0")

@app.on_event("startup")
def _startup():
    init_db()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/portfolio", response_model=PortfolioOut)
def portfolio(with_prices: bool = True):
    holdings = get_holdings()
    rows = []
    total_cost = 0.0
    total_value = 0.0

    for h in holdings:
        row = PortfolioRow(
            symbol=h["symbol"],
            market=h["market"],
            quantity=float(h["quantity"]),
            unit_cost_eur=float(h["unit_cost_eur"]),
        )
        total_cost += row.quantity * row.unit_cost_eur

        if with_prices and row.quantity != 0:
            q = get_price(row.symbol, row.market)
            row.last_price = q["price"]
            row.last_price_ccy = q["ccy"]
            row.last_price_eur = q["price_eur"]
            row.value_eur = row.quantity * row.last_price_eur
            row.pnl_abs = row.value_eur - row.quantity * row.unit_cost_eur
            row.pnl_pct = None if row.quantity == 0 else (row.last_price_eur - row.unit_cost_eur) / row.unit_cost_eur * 100.0 if row.unit_cost_eur != 0 else None
            row.fx_used = q.get("fx_used")
            row.price_source = q["source"]
            row.fetched_at = q["fetched_at"]
            total_value += row.value_eur or 0.0

        rows.append(row)

    totals = {
        "invested_cost_eur": total_cost,
        "current_value_eur": total_value if with_prices else None,
        "unrealized_pnl_eur": None if not with_prices else (total_value - total_cost),
        "unrealized_pnl_pct": None if not with_prices or total_cost == 0 else ((total_value - total_cost) / total_cost * 100.0),
        "currency": "EUR",
    }
    return {"rows": [r.model_dump() for r in rows], "totals": totals}

@app.post("/tx", response_model=TxOut)
def tx(inp: TxIn):
    try:
        updated = upsert_holding(inp.market, inp.symbol, float(inp.quantity), float(inp.price_eur), inp.type)
        record_tx(inp.model_dump())
        # quick totals for confirmation
        pf = portfolio(with_prices=True)
        return {
            "ok": True,
            "message": f"Transaction recorded",
            "holdings_after": {
                "symbol": updated["symbol"],
                "market": updated["market"],
                "quantity": updated["quantity"],
                "unit_cost_eur": updated["unit_cost_eur"],
                "portfolio_totals": pf["totals"],
            },
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/price", response_model=PriceOut)
def price(symbol: str, market: str):
    q = get_price(symbol, market)
    return {
        "symbol": symbol,
        "market": market,
        "price": q["price"],
        "ccy": q["ccy"],
        "price_eur": q["price_eur"],
        "fx_used": q.get("fx_used"),
        "source": q["source"],
        "fetched_at": q["fetched_at"],
    }

@app.post("/snapshot")
def snapshot(inp: SnapshotIn):
    val = snapshot_now(inp.scope, inp.symbol)
    return {"ok": True, "scope": inp.scope, "symbol": inp.symbol, "value_eur": val}

@app.get("/compare", response_model=CompareOut)
def compare(scope: str, symbol: Optional[str] = None):
    return cmp(scope, symbol)
