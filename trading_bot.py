"""
Trading Bot Dashboard  (Liquidity-Sweep on live Deriv data + forex analyzer)
----------------------------------------------------------------------------
BOT: a Smart-Money "liquidity sweep" strategy. Pick a market and it streams REAL
candles from Deriv's free public data feed (no login needed for data), draws them
as a live candlestick chart, marks swing highs/lows (liquidity), detects sweeps
(wick past a level + close back inside), waits for a confirming candle, then
enters the reversal with a stop past the wick and a target at opposing liquidity.
A "Simulator" option is included as an offline fallback.

ANALYZER: type any forex pair for real daily ECB rates + a Buy/Sell/Wait read.

HONEST NOTE: this is paper trading / analysis only. Real *data* is not real
*trading* — placing live Deriv orders needs your account login + authorization,
which this app does not do. Nothing here is financial advice, and an indicator
strategy on any single market can and does lose.

Deploy (Render):  build: pip install -r requirements.txt
                  start: gunicorn trading_bot:app --workers 1 --threads 8 --bind 0.0.0.0:$PORT
"""

import json
import random
import threading
import time
import urllib.request
from datetime import datetime, date, timedelta

from flask import Flask, jsonify, request

try:
    import websocket            # websocket-client
    HAS_WS = True
except ImportError:
    HAS_WS = False

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
NOTIONAL       = 100_000        # paper position notional -> return-based P&L
START_BALANCE  = 10_000.0
PIVOT          = 2              # swing strength (candles each side)
R_MULT         = 2.0           # fallback target when no opposing liquidity
SETUP_EXPIRY   = 2             # candles allowed to confirm a sweep
MAX_CANDLES    = 220
CHART_WINDOW   = 60
GRAN           = 60            # Deriv candle size in seconds (1 minute)
DERIV_WS       = "wss://ws.derivws.com/websockets/v3"
DERIV_APP_ID   = 1089          # Deriv's public app id (data only). Swap for your own if flaky.

# Simulator feed settings
TICK_SECONDS   = 0.35
CANDLE_TICKS   = 6

# Friendly market names -> Deriv symbols (SIM = offline simulator)
MARKETS = {
    "R_75": "Volatility 75", "R_100": "Volatility 100", "R_50": "Volatility 50",
    "R_25": "Volatility 25", "R_10": "Volatility 10",
    "frxEURUSD": "EUR/USD", "frxGBPJPY": "GBP/JPY", "frxXAUUSD": "Gold",
    "SIM": "Simulator (offline)",
}


class SimulatedBroker:
    def __init__(self, start_price=1.1000):
        self._price = start_price

    def tick(self):
        self._price += random.gauss(0, 0.00018) + (1.1000 - self._price) * 0.008
        return round(self._price, 5)


# ----------------------------------------------------------------------------
# Deriv live data feed (WebSocket, market data only — no auth)
# ----------------------------------------------------------------------------
class DerivFeed(threading.Thread):
    def __init__(self, bot, symbol, gran):
        super().__init__(daemon=True)
        self.bot, self.symbol, self.gran = bot, symbol, gran
        self.ws = None
        self._stop = False

    def run(self):
        while not self._stop:
            try:
                self.ws = websocket.WebSocketApp(
                    f"{DERIV_WS}?app_id={DERIV_APP_ID}",
                    on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_err, on_close=self._on_closed)
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self.bot.feed_state(f"connection error: {e}")
            if self._stop:
                break
            self.bot.feed_state("reconnecting…")
            time.sleep(3)

    def _on_open(self, ws):
        self.bot.feed_state("connected — loading history…")
        ws.send(json.dumps({"ticks_history": self.symbol, "style": "candles",
                            "granularity": self.gran, "count": 200,
                            "end": "latest", "subscribe": 1}))

    def _on_message(self, ws, msg):
        try:
            d = json.loads(msg)
        except Exception:
            return
        t = d.get("msg_type")
        if t == "candles":
            rows = [(int(c["open_time"]), float(c["open"]), float(c["high"]),
                     float(c["low"]), float(c["close"])) for c in d.get("candles", [])]
            self.bot.seed(rows)
        elif t == "ohlc":
            o = d["ohlc"]
            self.bot.update(int(o["open_time"]), float(o["open"]), float(o["high"]),
                            float(o["low"]), float(o["close"]))
        elif t == "error":
            self.bot.feed_state("Deriv error: " + d.get("error", {}).get("message", "unknown"))

    def _on_err(self, ws, err):
        self.bot.feed_state(f"connection error: {err}")

    def _on_closed(self, ws, *a):
        pass

    def stop(self):
        self._stop = True
        try:
            self.ws.close()
        except Exception:
            pass


# ----------------------------------------------------------------------------
# Liquidity-Sweep bot
# ----------------------------------------------------------------------------
class TradingBot:
    def __init__(self):
        self.lock = threading.Lock()
        self.broker = SimulatedBroker()
        self._reset()

    # ---- state ------------------------------------------------------------
    def _reset(self):
        self.running = False
        self.lot = 0.10
        self.source = "deriv"
        self.symbol = "R_75"
        self.gran = GRAN
        self.balance = START_BALANCE
        self.trades = []
        self.feed = None
        self.thread = None
        self._feedmsg = "Idle — press Start."
        self._reset_market()

    def _reset_market(self):
        self.candles = []          # {i,o,h,l,c}
        self.cur = None
        self.cur_ot = None
        self.ticks = 0
        self.idx = 0
        self.swing_highs = []
        self.swing_lows = []
        self.position = None
        self.pending = None
        self.events = []
        self.last_price = None
        self.state = "Waiting for data…"

    def feed_state(self, msg):
        self._feedmsg = msg

    # ---- strategy ---------------------------------------------------------
    def _rebuild_swings(self):
        self.swing_highs, self.swing_lows = [], []
        cs = self.candles
        for p in range(PIVOT, len(cs) - PIVOT):
            win = cs[p - PIVOT:p + PIVOT + 1]
            mid = cs[p]
            hs = [x["h"] for x in win]
            ls = [x["l"] for x in win]
            if mid["h"] == max(hs) and hs.count(mid["h"]) == 1:
                self.swing_highs.append((mid["i"], mid["h"]))
            if mid["l"] == min(ls) and ls.count(mid["l"]) == 1:
                self.swing_lows.append((mid["i"], mid["l"]))
        self.swing_highs = self.swing_highs[-40:]
        self.swing_lows = self.swing_lows[-40:]

    def _detect_swings(self):
        n = len(self.candles)
        if n < 2 * PIVOT + 1:
            return
        p = n - 1 - PIVOT
        win = self.candles[p - PIVOT:p + PIVOT + 1]
        mid = self.candles[p]
        hs = [x["h"] for x in win]
        ls = [x["l"] for x in win]
        if mid["h"] == max(hs) and hs.count(mid["h"]) == 1:
            self.swing_highs.append((mid["i"], mid["h"])); self.swing_highs = self.swing_highs[-40:]
        if mid["l"] == min(ls) and ls.count(mid["l"]) == 1:
            self.swing_lows.append((mid["i"], mid["l"])); self.swing_lows = self.swing_lows[-40:]

    def _detect_sweep(self, c):
        if self.swing_lows:
            _, low = self.swing_lows[-1]
            if c["l"] < low and c["c"] > low:
                self.pending = {"dir": "long", "extreme": c["l"], "expiry": self.idx + SETUP_EXPIRY}
                self.events.append({"i": c["i"], "type": "sweep_low", "price": c["l"]}); self.events = self.events[-30:]
                return
        if self.swing_highs:
            _, high = self.swing_highs[-1]
            if c["h"] > high and c["c"] < high:
                self.pending = {"dir": "short", "extreme": c["h"], "expiry": self.idx + SETUP_EXPIRY}
                self.events.append({"i": c["i"], "type": "sweep_high", "price": c["h"]}); self.events = self.events[-30:]

    def _try_confirm(self, c):
        p = self.pending
        if self.idx > p["expiry"]:
            self.pending = None; return
        if p["dir"] == "long":
            if c["c"] < p["extreme"]:
                self.pending = None
            elif c["c"] > c["o"]:
                self._enter("long", c["c"], p["extreme"])
        else:
            if c["c"] > p["extreme"]:
                self.pending = None
            elif c["c"] < c["o"]:
                self._enter("short", c["c"], p["extreme"])

    def _atr(self, n=14):
        cs = self.candles[-n:]
        if len(cs) < 2:
            return None
        return sum(x["h"] - x["l"] for x in cs) / len(cs)

    def _target(self, direction, entry, risk):
        if direction == "long":
            highs = [lv for _, lv in self.swing_highs if lv > entry]
            if highs and min(highs) - entry >= risk:
                return min(highs)
            return entry + R_MULT * risk
        lows = [lv for _, lv in self.swing_lows if lv < entry]
        if lows and entry - max(lows) >= risk:
            return max(lows)
        return entry - R_MULT * risk

    def _enter(self, direction, price, extreme):
        buf = max((self._atr() or price * 0.0005) * 0.25, price * 1e-6)
        if direction == "long":
            stop = extreme - buf; risk = price - stop
        else:
            stop = extreme + buf; risk = stop - price
        if risk <= 0:
            self.pending = None; return
        target = self._target(direction, price, risk)
        dec = 5
        self.position = {"dir": direction, "entry": round(price, dec), "stop": round(stop, dec),
                         "target": round(target, dec), "lot": self.lot, "i": self.idx}
        self.events.append({"i": self.idx, "type": "entry", "price": price}); self.events = self.events[-30:]
        self.pending = None

    def _pnl(self, entry, exit_, d, lot):
        return round((exit_ / entry - 1) * d * lot * NOTIONAL, 2)

    def _check_exit(self, high, low):
        pos = self.position
        if not pos:
            return
        if pos["dir"] == "long":
            if low <= pos["stop"]:
                self._close(pos["stop"], "SL")
            elif high >= pos["target"]:
                self._close(pos["target"], "TP")
        else:
            if high >= pos["stop"]:
                self._close(pos["stop"], "SL")
            elif low <= pos["target"]:
                self._close(pos["target"], "TP")

    def _close(self, price, reason):
        pos = self.position
        d = 1 if pos["dir"] == "long" else -1
        pnl = self._pnl(pos["entry"], price, d, pos["lot"])
        self.balance += pnl
        self.trades.append({"id": len(self.trades) + 1, "side": "BUY" if d == 1 else "SELL",
                            "lot": pos["lot"], "entry": pos["entry"], "exit": round(price, 5),
                            "pnl": pnl, "result": "WIN" if pnl >= 0 else "LOSS",
                            "reason": reason, "closed": datetime.now().strftime("%H:%M:%S")})
        self.position = None

    def _describe(self):
        if self.position:
            p = self.position
            self.state = f"In {p['dir'].upper()} @ {p['entry']} · SL {p['stop']} · TP {p['target']}"
        elif self.pending:
            k = "sell-side" if self.pending["dir"] == "long" else "buy-side"
            self.state = f"{k} sweep @ {round(self.pending['extreme'],5)} — waiting for a confirming candle"
        else:
            sh = self.swing_highs[-1][1] if self.swing_highs else None
            sl = self.swing_lows[-1][1] if self.swing_lows else None
            self.state = f"Watching liquidity — resting high {sh} / low {sl}"

    def _on_candle(self, c):
        self._detect_swings()
        if self.position:
            self._check_exit(c["h"], c["l"])
        elif self.pending:
            self._try_confirm(c)
        else:
            self._detect_sweep(c)
        self._describe()

    # ---- data intake: Deriv --------------------------------------------
    def seed(self, rows):
        with self.lock:
            self.candles = [{"i": i, "o": r[1], "h": r[2], "l": r[3], "c": r[4]}
                            for i, r in enumerate(rows)][-MAX_CANDLES:]
            self.idx = len(self.candles)
            self.cur = None; self.cur_ot = None
            self._rebuild_swings()
            self.last_price = self.candles[-1]["c"] if self.candles else None
            self._feedmsg = f"live: {MARKETS.get(self.symbol, self.symbol)}"
            self._describe()

    def update(self, ot, o, h, l, c):
        with self.lock:
            if not self.running:
                return
            self.last_price = c
            if self.cur_ot is None or ot == self.cur_ot:
                self.cur_ot = ot
                self.cur = {"o": o, "h": h, "l": l, "c": c}
            else:
                done = {"i": self.idx, **{k: round(v, 6) for k, v in self.cur.items()}}
                self.candles.append(done)
                if len(self.candles) > MAX_CANDLES:
                    self.candles.pop(0)
                self.idx += 1
                self._on_candle(done)
                self.cur_ot = ot
                self.cur = {"o": o, "h": h, "l": l, "c": c}
            if self.position and self.cur:
                self._check_exit(self.cur["h"], self.cur["l"])

    # ---- data intake: simulator ----------------------------------------
    def _sim_loop(self):
        while True:
            with self.lock:
                if not self.running or self.source != "sim":
                    break
                p = self.broker.tick(); self.last_price = p
                if self.cur is None:
                    self.cur = {"o": p, "h": p, "l": p, "c": p}; self.ticks = 1
                else:
                    self.cur["h"] = max(self.cur["h"], p); self.cur["l"] = min(self.cur["l"], p)
                    self.cur["c"] = p; self.ticks += 1
                if self.position:
                    self._check_exit(self.cur["h"], self.cur["l"])
                if self.ticks >= CANDLE_TICKS:
                    done = {"i": self.idx, **{k: round(v, 5) for k, v in self.cur.items()}}
                    self.candles.append(done)
                    if len(self.candles) > MAX_CANDLES:
                        self.candles.pop(0)
                    self.idx += 1; self.cur = None; self.ticks = 0
                    self._on_candle(done)
            time.sleep(TICK_SECONDS)

    # ---- controls ---------------------------------------------------------
    def _start_feed(self):
        self._stop_feed()
        if self.source == "sim":
            self._feedmsg = "running simulator"
            self.thread = threading.Thread(target=self._sim_loop, daemon=True); self.thread.start()
        elif not HAS_WS:
            self._feedmsg = "websocket-client not installed — pick Simulator."
        else:
            self._feedmsg = "connecting to Deriv…"
            self.feed = DerivFeed(self, self.symbol, self.gran); self.feed.start()

    def _stop_feed(self):
        if self.feed:
            try:
                self.feed.stop()
            except Exception:
                pass
            self.feed = None

    def start(self):
        with self.lock:
            if self.running:
                return
            self.running = True
        self._start_feed()

    def stop(self):
        with self.lock:
            self.running = False
            if self.position and self.last_price:
                self._close(self.last_price, "stopped")
        self._stop_feed()

    def reset(self):
        self.stop()
        with self.lock:
            sym, src = self.symbol, self.source
            self._reset()
            self.symbol, self.source = sym, src

    def set_symbol(self, sym):
        if sym not in MARKETS:
            return
        was = self.running
        self.stop()
        with self.lock:
            self.source = "sim" if sym == "SIM" else "deriv"
            self.symbol = sym
            self._reset_market()
        if was:
            self.start()

    def set_lot(self, lot):
        with self.lock:
            self.lot = max(0.01, round(float(lot), 2))

    # ---- snapshot ---------------------------------------------------------
    def _open_pnl(self):
        if not self.position or self.last_price is None:
            return 0.0
        d = 1 if self.position["dir"] == "long" else -1
        return self._pnl(self.position["entry"], self.last_price, d, self.position["lot"])

    def status(self):
        with self.lock:
            wins = sum(1 for t in self.trades if t["result"] == "WIN")
            losses = len(self.trades) - wins
            open_pnl = self._open_pnl()
            window = self.candles[-CHART_WINDOW:]
            start_i = window[0]["i"] if window else 0
            evs = [{"x": e["i"] - start_i, "type": e["type"], "price": e["price"]}
                   for e in self.events if e["i"] >= start_i]
            return {
                "running": self.running, "symbol": self.symbol,
                "market": MARKETS.get(self.symbol, self.symbol),
                "strategy": "Liquidity Sweep", "lot": self.lot,
                "price": self.last_price, "state": self.state, "feed": self._feedmsg,
                "balance": round(self.balance, 2),
                "equity": round(self.balance + open_pnl, 2),
                "open_pnl": open_pnl, "realised": round(self.balance - START_BALANCE, 2),
                "wins": wins, "losses": losses, "total": len(self.trades),
                "win_rate": round(wins / len(self.trades) * 100, 1) if self.trades else 0.0,
                "position": self.position, "trades": list(reversed(self.trades))[:50],
                "candles": [[c["o"], c["h"], c["l"], c["c"]] for c in window],
                "swing_high": self.swing_highs[-1][1] if self.swing_highs else None,
                "swing_low": self.swing_lows[-1][1] if self.swing_lows else None,
                "events": evs,
            }


bot = TradingBot()


# ----------------------------------------------------------------------------
# Forex analyzer (real daily ECB rates via Frankfurter — no key, no account)
# ----------------------------------------------------------------------------
def _http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "trading-bot/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def fetch_fx_series(pair, days=200):
    base, quote = pair[:3].upper(), pair[3:6].upper()
    end = date.today(); start = end - timedelta(days=days)
    path = f"/v1/{start.isoformat()}..{end.isoformat()}?base={base}&symbols={quote}"
    last_err = None
    for h in ("https://api.frankfurter.dev", "https://api.frankfurter.app"):
        try:
            data = _http_json(h + path)
            series = [(d, v[quote]) for d, v in sorted(data.get("rates", {}).items()) if quote in v]
            if series:
                return series
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Couldn't get data for {base}{quote} (try a pair like EURUSD or GBPJPY). {last_err}")


def sma(v, n): return sum(v[-n:]) / n if len(v) >= n else None
def sma_series(v, n): return [sum(v[i + 1 - n:i + 1]) / n if i + 1 >= n else None for i in range(len(v))]


def rsi(v, n=14):
    if len(v) < n + 1:
        return None
    g = [max(v[i] - v[i - 1], 0.0) for i in range(1, len(v))]
    l = [max(v[i - 1] - v[i], 0.0) for i in range(1, len(v))]
    ag, al = sum(g[:n]) / n, sum(l[:n]) / n
    for i in range(n, len(g)):
        ag = (ag * (n - 1) + g[i]) / n; al = (al * (n - 1) + l[i]) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def analyze(pair):
    series = fetch_fx_series(pair)
    dates = [d for d, _ in series]; closes = [c for _, c in series]
    price = closes[-1]
    s20, s50, r = sma(closes, 20), sma(closes, 50), rsi(closes, 14)
    vol = (lambda rr: (sum((x - sum(rr) / len(rr)) ** 2 for x in rr) / (len(rr) - 1)) ** 0.5 if len(rr) > 1 else 0.0)(
        [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))][-20:])
    score, reasons = 0, []
    if s20 and s50:
        if s20 > s50:
            score += 2; reasons.append(f"Uptrend: 20-day avg ({s20:.5f}) above 50-day avg ({s50:.5f}).")
        else:
            score -= 2; reasons.append(f"Downtrend: 20-day avg ({s20:.5f}) below 50-day avg ({s50:.5f}).")
    if s20:
        if price > s20:
            score += 1; reasons.append("Price is above its 20-day average (near-term strength).")
        else:
            score -= 1; reasons.append("Price is below its 20-day average (near-term weakness).")
    if r is not None:
        if r >= 70:
            score -= 1; reasons.append(f"RSI is {r:.0f} - overbought, momentum may be stretched.")
        elif r <= 30:
            score += 1; reasons.append(f"RSI is {r:.0f} - oversold, could be due a bounce.")
        else:
            reasons.append(f"RSI is {r:.0f} - neutral momentum.")
    if score >= 2:
        signal, side = "LOOKS LONG", "BUY"
    elif score <= -2:
        signal, side = "LOOKS SHORT", "SELL"
    else:
        signal, side = "NO CLEAR SETUP", "WAIT"
    entry = stop = target = None
    if side != "WAIT":
        d = price * vol * 1.5
        entry, stop, target = (price, price - d, price + d * 1.5) if side == "BUY" else (price, price + d, price - d * 1.5)
    return {"pair": pair[:6].upper(), "asof": dates[-1], "price": price, "dates": dates, "closes": closes,
            "sma20": sma_series(closes, 20), "sma50": sma_series(closes, 50),
            "rsi": round(r, 1) if r is not None else None, "signal": signal, "side": side,
            "reasons": reasons, "entry": entry, "stop": stop, "target": target}


# ----------------------------------------------------------------------------
# Web app
# ----------------------------------------------------------------------------
app = Flask(__name__)


@app.route("/")
def index():
    return PAGE


@app.route("/api/status")
def api_status():
    return jsonify(bot.status())


@app.route("/api/start", methods=["POST"])
def api_start():
    bot.start(); return jsonify(ok=True)


@app.route("/api/stop", methods=["POST"])
def api_stop():
    bot.stop(); return jsonify(ok=True)


@app.route("/api/reset", methods=["POST"])
def api_reset():
    bot.reset(); return jsonify(ok=True)


@app.route("/api/lot", methods=["POST"])
def api_lot():
    bot.set_lot(request.json.get("lot", 0.10)); return jsonify(ok=True, lot=bot.lot)


@app.route("/api/symbol", methods=["POST"])
def api_symbol():
    bot.set_symbol(request.json.get("symbol", "R_75")); return jsonify(ok=True, symbol=bot.symbol)


@app.route("/api/analyze")
def api_analyze():
    pair = request.args.get("pair", "").replace("/", "").replace(" ", "").strip()
    if len(pair) < 6:
        return jsonify(ok=False, error="Enter a 6-letter pair like EURUSD or GBPJPY.")
    try:
        return jsonify(ok=True, **analyze(pair))
    except Exception as e:
        return jsonify(ok=False, error=str(e))


# ----------------------------------------------------------------------------
# Dashboard
# ----------------------------------------------------------------------------
_OPTS = "".join(f'<option value="{k}">{v}</option>' for k, v in MARKETS.items())
PAGE = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Trading Bot</title>
<style>
  :root{--bg:#0f141b;--panel:#161d27;--panel2:#1c2530;--line:#26313f;--text:#dbe3ee;
    --muted:#8493a6;--up:#3fd07a;--down:#ff5d6c;--accent:#f0a92b;--idle:#5a6b80;
    --mono:ui-monospace,"SF Mono",Menlo,Consolas,monospace;--sans:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;line-height:1.4}
  .wrap{max-width:1040px;margin:0 auto;padding:20px 16px 60px}
  .bar{display:flex;flex-wrap:wrap;gap:12px;align-items:center;background:var(--panel);
    border:1px solid var(--line);border-radius:12px;padding:14px 16px;margin-bottom:16px}
  .dot{width:10px;height:10px;border-radius:50%;background:var(--idle)}
  .dot.live{background:var(--up);animation:pulse 1.6s infinite}
  @keyframes pulse{0%{box-shadow:0 0 0 0 rgba(63,208,122,.5)}70%{box-shadow:0 0 0 8px rgba(63,208,122,0)}100%{box-shadow:0 0 0 0 rgba(63,208,122,0)}}
  .state{font-family:var(--mono);font-size:13px}.badge{font-family:var(--mono);font-size:11px;color:var(--accent);border:1px solid var(--accent);border-radius:6px;padding:2px 8px}
  .grow{flex:1}
  label{color:var(--muted);font-size:12px;margin-right:6px}
  input,select{background:var(--panel2);border:1px solid var(--line);color:var(--text);font-family:var(--mono);font-size:14px;padding:8px 10px;border-radius:8px}
  input[type=number]{width:82px}#pair{width:150px;text-transform:uppercase;letter-spacing:1px}
  button{border:0;border-radius:8px;padding:9px 16px;font-weight:600;font-size:13px;cursor:pointer;color:#0b0f14;font-family:var(--sans)}
  .start{background:var(--up)}.stop{background:var(--down);color:#fff}.ghost{background:transparent;color:var(--muted);border:1px solid var(--line)}
  button:disabled{opacity:.4;cursor:not-allowed}
  h2{font-size:13px;color:var(--muted);font-weight:600;margin:0 0 8px 2px}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden;margin-bottom:16px}.pad{padding:14px 16px}
  .botstate{font-family:var(--mono);font-size:12px;color:var(--accent);margin:0 0 4px}
  .feed{font-family:var(--mono);font-size:11px;color:var(--muted);margin:0 0 10px}
  .chartbox{border:1px solid var(--line);border-radius:8px;background:var(--panel2);padding:8px}
  #botchart,#chart{width:100%;display:block}#botchart{height:260px}#chart{height:220px}
  #chart polyline{fill:none;vector-effect:non-scaling-stroke}
  .lpx{stroke:var(--accent);stroke-width:1.6}.l20{stroke:var(--up);stroke-width:1;opacity:.8}.l50{stroke:var(--muted);stroke-width:1;opacity:.7}
  #botchart rect,#botchart line{vector-effect:non-scaling-stroke}
  .legend{display:flex;gap:16px;flex-wrap:wrap;font-family:var(--mono);font-size:11px;color:var(--muted);margin:6px 2px 0}
  .legend i{display:inline-block;width:14px;height:2px;vertical-align:middle;margin-right:5px}.legend i.dash{border-top:1px dashed var(--muted);background:none;height:0}
  .stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:16px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px}
  .card .k{color:var(--muted);font-size:12px;margin-bottom:6px}.card .v{font-family:var(--mono);font-size:22px;font-variant-numeric:tabular-nums}
  .up{color:var(--up)}.down{color:var(--down)}
  table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:13px}
  th{text-align:right;color:var(--muted);font-weight:500;font-size:11px;padding:10px 14px;border-bottom:1px solid var(--line);background:var(--panel2)}
  th:first-child,td:first-child{text-align:left}
  td{text-align:right;padding:9px 14px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
  tr:last-child td{border-bottom:0}
  .pill{padding:2px 8px;border-radius:5px;font-size:11px;font-weight:600}.buy{background:rgba(63,208,122,.15);color:var(--up)}.sell{background:rgba(255,93,108,.15);color:var(--down)}
  .empty{padding:26px;text-align:center;color:var(--muted);font-family:var(--mono)}
  .arow{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.amsg{color:var(--muted);font-family:var(--mono);font-size:12px}
  .asig{display:flex;gap:16px;align-items:flex-start;flex-wrap:wrap;margin-top:12px}
  .sigbox{min-width:190px;border:1px solid var(--line);border-radius:10px;padding:14px 16px}.sigbox.buy{border-color:var(--up);background:rgba(63,208,122,.08)}.sigbox.sell{border-color:var(--down);background:rgba(255,93,108,.08)}
  .sigk{color:var(--muted);font-size:12px}.sigv{font-family:var(--mono);font-size:24px;margin:4px 0}.sigv.buy{color:var(--up)}.sigv.sell{color:var(--down)}.sigv.wait{color:var(--muted)}
  .sigsub{color:var(--muted);font-family:var(--mono);font-size:11px}.reasons{margin:0;padding-left:18px;flex:1;min-width:240px}.reasons li{margin:4px 0}
  .levels{display:flex;gap:18px;flex-wrap:wrap;margin-top:12px;font-family:var(--mono);font-size:13px}
  .caveat{color:var(--idle);font-size:11px;font-family:var(--mono);margin-top:12px;border-top:1px solid var(--line);padding-top:10px}
  .note{color:var(--idle);font-size:12px;font-family:var(--mono);text-align:center;margin-top:24px}
</style></head>
<body><div class="wrap">

  <div class="bar">
    <span class="dot" id="dot"></span><span class="state" id="state">STOPPED</span>
    <span class="badge">Liquidity Sweep</span>
    <label for="symbol" style="margin-left:6px">Market</label>
    <select id="symbol" onchange="setSymbol()">__OPTS__</select>
    <span class="grow"></span>
    <button class="start" id="btnStart" onclick="call('start')">Start</button>
    <button class="stop" id="btnStop" onclick="call('stop')">Stop</button>
    <span style="width:1px;height:24px;background:var(--line)"></span>
    <label for="lot">Lot</label>
    <input type="number" id="lot" min="0.01" step="0.01" value="0.10" onchange="setLot()">
    <button class="ghost" onclick="call('reset')">Reset</button>
  </div>

  <h2>Live chart &amp; strategy</h2>
  <div class="panel pad">
    <div class="botstate" id="botstate">Pick a market and press Start.</div>
    <div class="feed" id="feed"></div>
    <div class="chartbox"><svg id="botchart" preserveAspectRatio="none"></svg></div>
    <div class="legend">
      <span><i class="dash"></i>Swing high / low (liquidity)</span>
      <span><i style="background:var(--accent)"></i>Sweep</span>
      <span><i style="background:var(--up)"></i>Take-profit</span>
      <span><i style="background:var(--down)"></i>Stop-loss</span>
    </div>
  </div>

  <div class="stats">
    <div class="card"><div class="k">Balance</div><div class="v" id="balance">--</div></div>
    <div class="card"><div class="k">Equity</div><div class="v" id="equity">--</div></div>
    <div class="card"><div class="k">Open P&amp;L</div><div class="v" id="openpnl">--</div></div>
    <div class="card"><div class="k">Net P&amp;L</div><div class="v" id="realised">--</div></div>
    <div class="card"><div class="k">Win rate</div><div class="v" id="winrate">--</div></div>
    <div class="card"><div class="k">Wins / Losses</div><div class="v"><span class="up" id="wins">0</span> / <span class="down" id="losses">0</span></div></div>
    <div class="card"><div class="k">Price</div><div class="v" id="price">--</div></div>
    <div class="card"><div class="k">Trades</div><div class="v" id="total">0</div></div>
  </div>

  <h2>Open position</h2>
  <div class="panel"><div id="openpos"><div class="empty">Flat — no open position</div></div></div>

  <h2>Trade history</h2>
  <div class="panel"><table>
    <thead><tr><th>#</th><th>Side</th><th>Lot</th><th>Entry</th><th>Exit</th><th>Exit by</th><th>P&amp;L</th><th>Closed</th></tr></thead>
    <tbody id="tbody"><tr><td colspan="8" class="empty">No trades yet — press Start</td></tr></tbody>
  </table></div>

  <h2>Market analysis (real daily rates)</h2>
  <div class="panel pad">
    <div class="arow">
      <input type="text" id="pair" placeholder="EURUSD, GBPJPY…" onkeydown="if(event.key==='Enter')analyze()">
      <button class="start" id="btnAnalyze" onclick="analyze()">Analyze</button>
      <span class="amsg" id="amsg">Type a pair and press Analyze.</span>
    </div>
    <div id="aresult" style="display:none">
      <div class="chartbox" style="margin-top:14px"><svg id="chart" viewBox="0 0 700 220" preserveAspectRatio="none"></svg></div>
      <div class="legend"><span><i style="background:var(--accent)"></i>Price</span><span><i style="background:var(--up)"></i>20-day avg</span><span><i style="background:var(--muted)"></i>50-day avg</span></div>
      <div class="asig"><div class="sigbox" id="sigbox"><div class="sigk">Signal</div><div class="sigv" id="sigv">--</div><div class="sigsub" id="sigsub"></div></div><ul class="reasons" id="reasons"></ul></div>
      <div class="levels" id="levels"></div>
      <div class="caveat">Educational read of lagging daily indicators (ECB rates) — not a prediction or financial advice.</div>
    </div>
  </div>

  <div class="note">Bot = liquidity-sweep on live Deriv data (paper) · Analyzer = real daily ECB rates</div>
</div>

<script>
const $=id=>document.getElementById(id);
const money=n=>(n>=0?'+':'')+n.toFixed(2);
const cls=n=>n>=0?'up':'down';
const css=v=>getComputedStyle(document.documentElement).getPropertyValue(v).trim();
let symTouched=false;

async function call(a){ await fetch('/api/'+a,{method:'POST'}); refresh(); }
async function setLot(){ await fetch('/api/lot',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({lot:parseFloat($('lot').value)||0.1})}); }
async function setSymbol(){ symTouched=true; await fetch('/api/symbol',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:$('symbol').value})}); refresh(); }

function drawCandles(s){
  const c=s.candles||[]; const svg=$('botchart');
  if(!c.length){ svg.innerHTML=''; return; }
  const W=700,H=260,pad=8;
  let lo=Math.min(...c.map(k=>k[2])), hi=Math.max(...c.map(k=>k[1]));
  [s.swing_high,s.swing_low].forEach(v=>{if(v!=null){lo=Math.min(lo,v);hi=Math.max(hi,v);}});
  if(s.position){[s.position.stop,s.position.target,s.position.entry].forEach(v=>{lo=Math.min(lo,v);hi=Math.max(hi,v);});}
  const rng=(hi-lo)||1e-9;
  const slot=(W-2*pad)/Math.max(c.length,1), bw=Math.max(slot*0.6,1.2);
  const x=i=>pad+i*slot, y=p=>H-pad-((p-lo)/rng)*(H-2*pad);
  const up=css('--up'),down=css('--down');
  const hline=(p,col)=>`<line x1="${pad}" y1="${y(p).toFixed(1)}" x2="${W-pad}" y2="${y(p).toFixed(1)}" stroke="${col}" stroke-dasharray="4 3" stroke-width="1"/>`;
  let out='';
  if(s.swing_high!=null) out+=hline(s.swing_high,css('--muted'));
  if(s.swing_low!=null)  out+=hline(s.swing_low,css('--muted'));
  if(s.position){ out+=hline(s.position.stop,down)+hline(s.position.target,up); }
  c.forEach((k,i)=>{const [o,h,l,cl]=k,col=cl>=o?up:down,cx=x(i)+slot/2;
    out+=`<line x1="${cx.toFixed(1)}" y1="${y(h).toFixed(1)}" x2="${cx.toFixed(1)}" y2="${y(l).toFixed(1)}" stroke="${col}" stroke-width="1"/>`;
    const a=y(o),b=y(cl);out+=`<rect x="${(cx-bw/2).toFixed(1)}" y="${Math.min(a,b).toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(Math.abs(a-b),1).toFixed(1)}" fill="${col}"/>`;});
  (s.events||[]).forEach(e=>{if(e.x<0||e.x>=c.length)return;const cx=x(e.x)+slot/2,col=e.type==='entry'?css('--text'):css('--accent');
    out+=`<circle cx="${cx.toFixed(1)}" cy="${y(e.price).toFixed(1)}" r="3" fill="${col}"/>`;});
  svg.setAttribute('viewBox',`0 0 ${W} ${H}`); svg.innerHTML=out;
}

async function refresh(){
  let s; try{ s=await (await fetch('/api/status')).json(); }catch(e){ return; }
  $('dot').className='dot'+(s.running?' live':'');
  $('state').textContent=s.running?'RUNNING':'STOPPED';
  $('btnStart').disabled=s.running; $('btnStop').disabled=!s.running;
  if(!symTouched && s.symbol) $('symbol').value=s.symbol;
  $('botstate').textContent=s.state||''; $('feed').textContent=s.feed||'';
  $('balance').textContent=s.balance.toFixed(2); $('equity').textContent=s.equity.toFixed(2);
  $('price').textContent=s.price!=null?(''+s.price):'--';
  $('winrate').textContent=s.win_rate+'%'; $('wins').textContent=s.wins; $('losses').textContent=s.losses; $('total').textContent=s.total;
  $('openpnl').textContent=money(s.open_pnl); $('openpnl').className='v '+cls(s.open_pnl);
  $('realised').textContent=money(s.realised); $('realised').className='v '+cls(s.realised);
  drawCandles(s);
  if(s.position){const p=s.position,sd=p.dir==='long'?'BUY':'SELL';
    $('openpos').innerHTML=`<table><thead><tr><th>Side</th><th>Lot</th><th>Entry</th><th>Stop</th><th>Target</th><th>Now</th><th>Open P&L</th></tr></thead>
      <tbody><tr><td><span class="pill ${sd==='BUY'?'buy':'sell'}">${sd}</span></td><td>${p.lot.toFixed(2)}</td><td>${p.entry}</td><td>${p.stop}</td><td>${p.target}</td>
      <td>${s.price!=null?s.price:'--'}</td><td class="${cls(s.open_pnl)}">${money(s.open_pnl)}</td></tr></tbody></table>`;
  } else { $('openpos').innerHTML='<div class="empty">Flat — no open position</div>'; }
  const tb=$('tbody');
  if(!s.trades.length){ tb.innerHTML='<tr><td colspan="8" class="empty">No trades yet — press Start</td></tr>'; }
  else{ tb.innerHTML=s.trades.map(t=>`<tr><td>${t.id}</td><td><span class="pill ${t.side==='BUY'?'buy':'sell'}">${t.side}</span></td>
    <td>${t.lot.toFixed(2)}</td><td>${t.entry}</td><td>${t.exit}</td><td>${t.reason}</td><td class="${cls(t.pnl)}">${money(t.pnl)}</td><td>${t.closed}</td></tr>`).join(''); }
}

async function analyze(){
  const pair=($('pair').value||'').trim().toUpperCase();
  if(pair.length<6){ $('amsg').textContent='Enter a 6-letter pair like EURUSD.'; return; }
  $('btnAnalyze').disabled=true; $('amsg').textContent='Analyzing '+pair+'…';
  let d; try{ d=await (await fetch('/api/analyze?pair='+encodeURIComponent(pair))).json(); }
  catch(e){ $('amsg').textContent='Network error — is the pair valid?'; $('btnAnalyze').disabled=false; return; }
  $('btnAnalyze').disabled=false;
  if(!d.ok){ $('amsg').textContent=d.error||'Could not analyze.'; $('aresult').style.display='none'; return; }
  $('amsg').textContent=''; renderAnalysis(d);
}
function scale(v,w,h,mn,mx,pad){const n=v.length,xs=(w-2*pad)/(n-1);let p=[];
  for(let i=0;i<n;i++){if(v[i]==null)continue;p.push((pad+i*xs).toFixed(1)+','+(h-pad-((v[i]-mn)/(mx-mn))*(h-2*pad)).toFixed(1));}return p.join(' ');}
function renderAnalysis(d){
  $('aresult').style.display='block';
  const w=700,h=220,pad=10,all=d.closes.filter(v=>v!=null),mn=Math.min(...all),mx=Math.max(...all);
  const poly=(v,c)=>`<polyline points="${scale(v,w,h,mn,mx,pad)}" class="${c}"/>`;
  $('chart').innerHTML=poly(d.sma50,'l50')+poly(d.sma20,'l20')+poly(d.closes,'lpx');
  const side=d.side.toLowerCase();
  $('sigbox').className='sigbox '+(side==='wait'?'':side);
  $('sigv').className='sigv '+side; $('sigv').textContent=d.signal;
  $('sigsub').textContent=`${d.pair} · ${d.price.toFixed(5)} · RSI ${d.rsi} · as of ${d.asof}`;
  $('reasons').innerHTML=d.reasons.map(r=>`<li>${r}</li>`).join('');
  $('levels').innerHTML=d.side==='WAIT'?'<span>Indicators are mixed — no trade suggested.</span>'
    :`<span><b>${d.side}</b> around ${d.entry.toFixed(5)}</span><span>Illustrative stop ${d.stop.toFixed(5)}</span><span>Illustrative target ${d.target.toFixed(5)}</span>`;
}
setInterval(refresh,1000); refresh();
</script>
</body></html>""".replace("__OPTS__", _OPTS)


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    print(f"Trading bot running -> http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
