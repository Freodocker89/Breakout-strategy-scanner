import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from data import list_usdt_perps, fetch_ohlcv
from levels import breakout_signals

st.set_page_config(page_title="Bitget Breakout Scanner", layout="wide")
st.title("Breakout Level Scanner — Bitget USDT-Perps")

with st.sidebar:
    st.subheader("Scan Settings")
    timeframe = st.selectbox("Timeframe", ["1h","4h","1d"], index=1)
    scan_all = st.checkbox("Scan ALL Bitget USDT-Perps", value=True)
    top_n = st.slider("Top N by 24h volume (ignored if Scan ALL is on)", 10, 400, 40, step=10)

    st.subheader("Level Parameters")
    swL = st.number_input("Pivot Left bars", 1, 10, 3)
    swR = st.number_input("Pivot Right bars", 1, 10, 3)
    atrLen = st.number_input("ATR Length", 5, 50, 14)
    tolATR = st.number_input("Tolerance (× ATR)", 0.01, 1.0, 0.15, step=0.01)
    minTouches = st.number_input("Min touches", 2, 6, 3)
    maxGapBars = st.number_input("Max bars between touches", 20, 600, 120, step=10)

    mode = st.radio("Signal side", ["Long breakout (Resistance)", "Short breakout (Support)"], index=0)

params = dict(
    swL=int(swL), swR=int(swR), atrLen=int(atrLen),
    tolATR=float(tolATR), minTouches=int(minTouches),
    maxGapBars=int(maxGapBars),
    side='res' if mode.startswith("Long") else 'sup'
)

@st.cache_data(ttl=300)
def symbols_all():
    df = list_usdt_perps()
    return df['symbol'].tolist(), df

def pick_symbols(scan_all: bool, top_n: int):
    syms, df = symbols_all()
    if scan_all:
        return syms
    return df.sort_values('baseVolume', ascending=False).head(top_n)['symbol'].tolist()

syms = pick_symbols(scan_all, top_n)
st.caption(f"Scanning {len(syms)} markets.")

def process(sym):
    try:
        df = fetch_ohlcv(sym, timeframe=timeframe, limit=1500)
        sig = breakout_signals(df, params)
        return dict(symbol=sym, **sig)
    except Exception as e:
        return dict(symbol=sym, status=f"error: {e}")

rows = []
max_workers = 8              # tune down if Streamlit Cloud rate-limits
chunk_size  = 80             # symbols per burst
pause_between_chunks = 1.2   # seconds between bursts

progress = st.progress(0, text="Starting scan…")
total = len(syms); done = 0

with ThreadPoolExecutor(max_workers=max_workers) as ex:
    for i in range(0, total, chunk_size):
        batch = syms[i:i+chunk_size]
        futures = [ex.submit(process, s) for s in batch]
        for f in as_completed(futures):
            rows.append(f.result())
            done += 1
            progress.progress(done/total, text=f"Scanning… {done}/{total}")
        time.sleep(pause_between_chunks)

progress.empty()
res = pd.DataFrame(rows)
if not res.empty:
    order = pd.Categorical(res['status'], categories=['breakout','near','none','no-level'], ordered=True)
    res = res.sort_values(['status','distance'], key=lambda c: order if c.name=='status' else c)

col1, col2 = st.columns([2,1])
with col1:
    st.subheader("Signals")
    cols = ['symbol','status','live_level','live_touches','cand_level','cand_touches','distance']
    show = [c for c in cols if c in res.columns]
    st.dataframe(res[show], use_container_width=True, height=560)

with col2:
    st.subheader("Summary")
    s = res.get('status')
    st.metric("Breakouts", int((s=='breakout').sum()) if s is not None else 0)
    st.metric("Near Levels", int((s=='near').sum()) if s is not None else 0)
    st.metric("Errors", int(res['status'].astype(str).str.startswith('error').sum()) if 'status' in res else 0)
