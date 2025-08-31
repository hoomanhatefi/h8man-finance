from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Dict

TxType = Literal["buy", "sell"]

class TxIn(BaseModel):
    type: TxType
    symbol: str
    market: Literal["US", "XETRA"]
    quantity: float
    price_eur: float
    note: Optional[str] = None
    source: Optional[str] = None

class TxOut(BaseModel):
    ok: bool
    message: str
    holdings_after: Optional[Dict] = None

class Holding(BaseModel):
    symbol: str
    market: Literal["US", "XETRA"]
    quantity: float
    unit_cost_eur: float
    updated_at: str

class PortfolioRow(BaseModel):
    symbol: str
    market: Literal["US", "XETRA"]
    quantity: float
    unit_cost_eur: float
    last_price_ccy: Optional[str] = None
    last_price: Optional[float] = None
    last_price_eur: Optional[float] = None
    value_eur: Optional[float] = None
    pnl_abs: Optional[float] = None
    pnl_pct: Optional[float] = None
    fx_used: Optional[float] = None
    price_source: Optional[str] = None
    fetched_at: Optional[str] = None

class PortfolioOut(BaseModel):
    rows: List[PortfolioRow]
    totals: Dict

class PriceOut(BaseModel):
    symbol: str
    market: Literal["US", "XETRA"]
    price: float
    ccy: str
    price_eur: float
    fx_used: Optional[float] = None
    source: str
    fetched_at: str

class SnapshotIn(BaseModel):
    scope: Literal["daily", "weekly", "monthly"]
    symbol: Optional[str] = None

class CompareOut(BaseModel):
    scope: str
    symbol: Optional[str] = None
    baseline_ts: Optional[str] = None
    current_ts: str
    baseline_value_eur: Optional[float] = None
    current_value_eur: float
    change_abs: Optional[float] = None
    change_pct: Optional[float] = None
    note: Optional[str] = None
