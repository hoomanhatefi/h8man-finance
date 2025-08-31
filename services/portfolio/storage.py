import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

DB_PATH = os.getenv("DB_PATH", "/app/cache.db")

@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

def init_db():
    with db() as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS holdings(
            symbol TEXT PRIMARY KEY,
            market TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_cost_eur REAL NOT NULL,
            updated_at TEXT NOT NULL
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS tx(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            market TEXT NOT NULL,
            quantity REAL NOT NULL,
            price_eur REAL NOT NULL,
            note TEXT,
            source TEXT
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS prices_cache(
            symbol TEXT NOT NULL,
            market TEXT NOT NULL,
            price REAL NOT NULL,
            ccy TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            source TEXT,
            PRIMARY KEY(symbol, market)
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            scope TEXT NOT NULL, -- daily weekly monthly
            symbol TEXT,         -- null means whole portfolio
            value_eur REAL NOT NULL
        )""")

def get_holdings():
    with db() as conn:
        rows = conn.execute("SELECT symbol,market,quantity,unit_cost_eur,updated_at FROM holdings ORDER BY symbol").fetchall()
        return [dict(r) for r in rows]

def get_holding(symbol):
    with db() as conn:
        r = conn.execute("SELECT symbol,market,quantity,unit_cost_eur,updated_at FROM holdings WHERE symbol=?", (symbol,)).fetchone()
        return dict(r) if r else None

def upsert_holding(market, symbol, qty_delta, price_eur, tx_type):
    now = utcnow_iso()
    with db() as conn:
        cur = conn.execute("SELECT quantity, unit_cost_eur FROM holdings WHERE symbol=?", (symbol,))
        r = cur.fetchone()
        if r is None:
            if tx_type == "sell":
                raise ValueError("Cannot sell a position that does not exist")
            quantity = float(qty_delta)
            unit_cost = float(price_eur)
            conn.execute("INSERT INTO holdings(symbol, market, quantity, unit_cost_eur, updated_at) VALUES(?,?,?,?,?)",
                         (symbol, market, quantity, unit_cost, now))
        else:
            old_qty = float(r["quantity"])
            old_uc = float(r["unit_cost_eur"])
            if tx_type == "buy":
                new_qty = old_qty + qty_delta
                if new_qty <= 0:
                    new_uc = 0.0
                else:
                    new_uc = (old_uc * old_qty + price_eur * qty_delta) / new_qty
            else:
                new_qty = old_qty - qty_delta
                if new_qty < -1e-9:
                    raise ValueError("Sell quantity exceeds current holdings")
                new_uc = old_uc if new_qty > 0 else 0.0
            conn.execute("UPDATE holdings SET market=?, quantity=?, unit_cost_eur=?, updated_at=? WHERE symbol=?",
                         (market, new_qty, new_uc, now, symbol))
    return get_holding(symbol)

def record_tx(tx):
    with db() as conn:
        conn.execute("""
            INSERT INTO tx(ts,type,symbol,market,quantity,price_eur,note,source)
            VALUES(?,?,?,?,?,?,?,?)
        """, (utcnow_iso(), tx["type"], tx["symbol"], tx["market"], tx["quantity"], tx["price_eur"], tx.get("note"), tx.get("source")))

def get_price_cache(symbol, market):
    with db() as conn:
        r = conn.execute("SELECT price, ccy, fetched_at, source FROM prices_cache WHERE symbol=? AND market=?", (symbol, market)).fetchone()
        return dict(r) if r else None

def set_price_cache(symbol, market, price, ccy, fetched_at, source):
    with db() as conn:
        conn.execute("""
            INSERT INTO prices_cache(symbol,market,price,ccy,fetched_at,source)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(symbol,market) DO UPDATE SET
              price=excluded.price,
              ccy=excluded.ccy,
              fetched_at=excluded.fetched_at,
              source=excluded.source
        """, (symbol, market, price, ccy, fetched_at, source))

def save_snapshot(scope, symbol, value_eur):
    with db() as conn:
        conn.execute("INSERT INTO snapshots(ts,scope,symbol,value_eur) VALUES(?,?,?,?)",
                     (utcnow_iso(), scope, symbol, value_eur))

def get_latest_snapshot(scope, symbol):
    with db() as conn:
        r = conn.execute("""
           SELECT ts, value_eur FROM snapshots
           WHERE scope=? AND COALESCE(symbol,'')=COALESCE(?, '')
           ORDER BY ts DESC LIMIT 1
        """, (scope, symbol)).fetchone()
        return dict(r) if r else None
