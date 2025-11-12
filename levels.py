import numpy as np
import pandas as pd

def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    tr = np.maximum.reduce([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()])
    return tr.rolling(length, min_periods=1).mean()

def _pivots(series: pd.Series, left: int, right: int, mode: str):
    s = series.values
    n = len(s)
    piv = np.full(n, False)
    for i in range(left, n - right):
        w = s[i-left:i+right+1]
        if mode == "high":
            piv[i] = s[i] == w.max()
        else:
            piv[i] = s[i] == w.min()
    idx = np.where(piv)[0]
    vals = series.iloc[idx].values
    return idx, vals

def _cluster_last_level(idx, vals, df, tol_abs, max_gap_bars, want_res=True):
    if len(idx) == 0:
        return None
    y = None
    touches = 0
    first_bar = None
    for k in range(len(idx)-1, -1, -1):  # newest → oldest
        i = idx[k]
        v = vals[k]
        if y is None:
            y = v; touches = 1; first_bar = i
        else:
            if abs(v - y) <= tol_abs and (first_bar - i) <= max_gap_bars:
                touches += 1
                first_bar = i
                y = max(y, v) if want_res else min(y, v)
            else:
                break
    return dict(y=y, first_bar=int(first_bar), touches=int(touches))

def breakout_signals(df: pd.DataFrame, params: dict) -> dict:
    swL = int(params["swL"]); swR = int(params["swR"])
    atrLen = int(params["atrLen"]); tolATR = float(params["tolATR"])
    minTouches = int(params["minTouches"]); maxGapBars = int(params["maxGapBars"])
    side = params.get("side", "res")  # 'res' long breakouts; 'sup' short

    atr = _atr(df, atrLen)
    tol_abs = atr * tolATR

    ph_idx, ph_vals = _pivots(df["high"], swL, swR, "high")
    res = _cluster_last_level(ph_idx, ph_vals, df, tol_abs.iloc[-1], maxGapBars, want_res=True)

    pl_idx, pl_vals = _pivots(df["low"], swL, swR, "low")
    sup = _cluster_last_level(pl_idx, pl_vals, df, tol_abs.iloc[-1], maxGapBars, want_res=False)

    close = float(df["close"].iloc[-1])
    out = {"status":"no-level","live_level":None,"live_touches":0,
           "cand_level":None,"cand_touches":0,"distance":None}

    def _eval(level, want_break_up: bool):
        if not level: return None
        live = level["touches"] >= minTouches
        y = float(level["y"])
        out["live_level"] = y; out["live_touches"] = level["touches"]
        out["distance"] = (close - y) if want_break_up else (y - close)
        if live:
            hit = (close > y) if want_break_up else (close < y)
            out["status"] = "breakout" if hit else "near"
        else:
            out["status"] = "no-level"
        return out

    if side == "res":
        return _eval(res, True) or out
    else:
        return _eval(sup, False) or out
