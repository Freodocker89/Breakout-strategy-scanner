import ccxt
import pandas as pd
from functools import lru_cache

EXCHANGE = ccxt.bitget({"enableRateLimit": True})

def _markets_df():
    mkts = EXCHANGE.load_markets()
    rows = []
    for m in mkts.values():
        if m.get("swap") and str(m.get("quote")).upper() == "USDT":
            rows.append({
                "symbol": m["symbol"],
                "base": m.get("base"),
                "quote": m.get("quote"),
                "active": m.get("active", True),
                "baseVolume": m.get("info", {}).get("baseVolume") or 0
            })
    return pd.DataFrame(rows)

@lru_cache(maxsize=1)
def list_usdt_perps() -> pd.DataFrame:
    df = _markets_df()
    return df[df["active"] == True].reset_index(drop=True)

@lru_cache(maxsize=4096)
def fetch_ohlcv(symbol: str, timeframe: str = "4h", limit: int = 1500) -> pd.DataFrame:
    ohlcv = EXCHANGE.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    cols = ["timestamp","open","high","low","close","volume"]
    df = pd.DataFrame(ohlcv, columns=cols)
    df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df
