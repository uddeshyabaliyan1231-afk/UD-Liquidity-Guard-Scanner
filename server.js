
const express = require("express");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

const API = {
  instruments:
    "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments?margin_currency_short_name[]=USDT",
  prices:
    "https://public.coindcx.com/market_data/v3/current_prices/futures/rt",
  candles:
    "https://public.coindcx.com/market_data/candlesticks",
  orderbook:
    "https://public.coindcx.com/market_data/v3/orderbook"
};

const CONFIG = {
  concurrency: 2,
  requestSpacingMs: 300,
  minQuoteVolume: 100000,
  minBookDepth: 1500,
  maxSpreadPercent: 0.12,
  minImbalance: 1.15,
  maxWallConcentration: 0.45,
  scanIntervalMs: 60000,
  requestTimeoutMs: 20000,
  retries: 3
};

let latestResults = [];
let rejectedCounts = {};

let scanStatus = {
  running: false,
  lastScan: null,
  totalPairs: 0,
  eligiblePairs: 0,
  checked: 0,
  signals: 0,
  skipped: 0,
  message: "Waiting for scan"
};

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

let requestTail = Promise.resolve();
let lastRequestAt = 0;

async function waitForRequestSlot() {
  const previous = requestTail;
  let release;

  requestTail = new Promise(resolve => {
    release = resolve;
  });

  await previous;

  const wait = Math.max(
    0,
    CONFIG.requestSpacingMs - (Date.now() - lastRequestAt)
  );

  if (wait) await sleep(wait);

  lastRequestAt = Date.now();
  release();
}

async function getJSON(url, attempts = CONFIG.retries) {
  let lastError;

  for (let i = 0; i < attempts; i++) {
    try {
      await waitForRequestSlot();

      const response = await fetch(url, {
        headers: {
          "User-Agent": "UD-Liquidity-Guard-Scanner/3.0"
        },
        signal: AbortSignal.timeout(CONFIG.requestTimeoutMs)
      });

      if (!response.ok) {
        throw new Error("CoinDCX API status " + response.status);
      }

      return await response.json();

    } catch (error) {
      lastError = error;

      if (i < attempts - 1) {
        await sleep(1000 * (i + 1));
      }
    }
  }

  throw lastError;
}

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function getInstruments(data) {
  if (Array.isArray(data)) return data;
  if (Array.isArray(data?.instruments)) return data.instruments;
  if (Array.isArray(data?.data)) return data.data;
  return [];
}

function getPriceMap(data) {
  const rows = Array.isArray(data)
    ? data
    : Array.isArray(data?.prices)
    ? data.prices
    : Array.isArray(data?.data)
    ? data.data
    : null;

  if (rows) {
    const map = {};

    for (const item of rows) {
      const pair = item?.pair || item?.instrument;
      if (pair) map[pair] = item;
    }

    return map;
  }

  if (data && typeof data === "object") {
    return data.prices || data.data || data;
  }

  return {};
}

function getTickerPrice(ticker) {
  return num(
    ticker?.ls ??
    ticker?.last_price ??
    ticker?.lastPrice ??
    ticker?.price ??
    ticker?.p
  );
}

function getTickerVolume(ticker) {
  return num(
    ticker?.v ??
    ticker?.volume ??
    ticker?.volume_24h ??
    ticker?.volume24h
  );
}

function ema(values, period) {
  if (values.length < period) return null;

  let result =
    values.slice(0, period).reduce((a, b) => a + b, 0) / period;

  const multiplier = 2 / (period + 1);

  for (let i = period; i < values.length; i++) {
    result =
      values[i] * multiplier +
      result * (1 - multiplier);
  }

  return result;
}

function rsi(values, period = 14) {
  if (values.length <= period) return null;

  let gains = 0;
  let losses = 0;

  for (let i = values.length - period; i < values.length; i++) {
    const change = values[i] - values[i - 1];

    if (change > 0) gains += change;
    else losses += Math.abs(change);
  }

  if (losses === 0) return 100;

  const rs = gains / Math.max(losses, 1e-10);
  return 100 - 100 / (1 + rs);
}

function analyseCandles(candles) {
  if (!Array.isArray(candles) || candles.length < 205) {
    return null;
  }

  const sorted = [...candles].sort(
    (a, b) => num(a.time) - num(b.time)
  );

  const closed = sorted.slice(0, -1);

  if (closed.length < 200) return null;

  const closes = closed.map(c => num(c.close));
  const volumes = closed.map(c => num(c.volume));

  const last = closed[closed.length - 1];

  const price = num(last.close);
  const open = num(last.open);
  const high = num(last.high);
  const low = num(last.low);
  const range = high - low;

  if (
    !(price > 0) ||
    !(range > 0) ||
    closes.some(x => !(x > 0))
  ) {
    return null;
  }

  const e20 = ema(closes, 20);
  const e50 = ema(closes, 50);
  const e200 = ema(closes, 200);

  if (![e20, e50, e200].every(Number.isFinite)) {
    return null;
  }

  let trend = "SIDEWAYS";

  if (price > e20 && e20 > e50 && e50 > e200) {
    trend = "BULLISH";
  } else if (price < e20 && e20 < e50 && e50 < e200) {
    trend = "BEARISH";
  }

  const recentVol =
    volumes.slice(-20).reduce((a, b) => a + b, 0) / 20;

  const previousVol =
    volumes.slice(-40, -20).reduce((a, b) => a + b, 0) / 20;

  return {
    price,
    open,
    high,
    low,
    trend,
    rsi: rsi(closes),
    volumeRising: recentVol > previousVol,
    upperWickRatio:
      (high - Math.max(open, price)) / range,
    lowerWickRatio:
      (Math.min(open, price) - low) / range
  };
}

async function getCandles(pair, resolution) {
  const minutes = {
    "5": 5,
    "15": 15,
    "30": 30,
    "60": 60,
    "240": 240
  }[resolution];

  const now = Math.floor(Date.now() / 1000);
  const from = now - minutes * 60 * 210;

  const url = new URL(API.candles);

  url.searchParams.set("pair", pair);
  url.searchParams.set("from", String(from));
  url.searchParams.set("to", String(now));
  url.searchParams.set("resolution", resolution);
  url.searchParams.set("pcode", "f");

  const data = await getJSON(url.toString());

  if (Array.isArray(data)) return data;
  return Array.isArray(data?.data) ? data.data : [];
}

function parseSide(side) {
  if (!side) return [];

  if (Array.isArray(side)) {
    return side.map(x => ({
      price: num(x?.price ?? x?.[0]),
      qty: num(x?.quantity ?? x?.qty ?? x?.[1])
    })).filter(x => x.price > 0 && x.qty > 0);
  }

  if (typeof side === "object") {
    return Object.entries(side)
      .map(([price, qty]) => ({
        price: num(price),
        qty: num(qty)
      }))
      .filter(x => x.price > 0 && x.qty > 0);
  }

  return [];
}

async function checkOrderbook(pair) {
  const url =
    API.orderbook + "/" +
    encodeURIComponent(pair) + "-futures/50";

  const data = await getJSON(url);

  const bids = parseSide(data?.bids).sort(
    (a, b) => b.price - a.price
  );

  const asks = parseSide(data?.asks).sort(
    (a, b) => a.price - b.price
  );

  if (!bids.length || !asks.length) {
    return { safe: false, reason: "EMPTY_ORDERBOOK" };
  }

  const bestBid = bids[0].price;
  const bestAsk = asks[0].price;
  const mid = (bestBid + bestAsk) / 2;

  if (bestAsk <= bestBid || mid <= 0) {
    return { safe: false, reason: "INVALID_BOOK" };
  }

  const spread = (bestAsk - bestBid) / mid * 100;

  const bidDepth = bids.reduce(
    (sum, x) => sum + x.price * x.qty, 0
  );

  const askDepth = asks.reduce(
    (sum, x) => sum + x.price * x.qty, 0
  );

  const totalDepth = bidDepth + askDepth;

  const imbalance =
    bidDepth / Math.max(askDepth, 1e-10);

  const largestWall = Math.max(
    ...bids.map(x => x.price * x.qty),
    ...asks.map(x => x.price * x.qty)
  );

  const wallConcentration =
    largestWall / Math.max(totalDepth, 1e-10);

  const failures = [];

  if (spread > CONFIG.maxSpreadPercent) {
    failures.push("HIGH_SPREAD");
  }

  if (Math.min(bidDepth, askDepth) < CONFIG.minBookDepth) {
    failures.push("LOW_DEPTH");
  }

  if (wallConcentration > CONFIG.maxWallConcentration) {
    failures.push("WALL_CONCENTRATION");
  }

  return {
    safe: failures.length === 0,
    reason: failures.join(","),
    spread,
    bidDepth,
    askDepth,
    imbalance,
    wallConcentration
  };
}

function reject(reason) {
  rejectedCounts[reason] =
    (rejectedCounts[reason] || 0) + 1;

  return null;
}

async function scanCoin(item) {
  const instrument = item.instrument;
  const ticker = item.ticker;

  const pair = typeof instrument === "string"
    ? instrument
    : instrument?.pair;

  if (!pair || !ticker) {
    return reject("MISSING_TICKER");
  }

  const price = getTickerPrice(ticker);
  const volume = getTickerVolume(ticker);

  if (!(price > 0)) {
    return reject("INVALID_PRICE");
  }

  if (volume * price < CONFIG.minQuoteVolume) {
    return reject("LOW_24H_VOLUME");
  }

  const resolutions = ["240", "60", "30", "15", "5"];

  let candleSets;

  try {
    candleSets = await Promise.all(
      resolutions.map(r => getCandles(pair, r))
    );
  } catch (error) {
    return reject(
      error.message.includes("429")
        ? "COINDCX_RATE_LIMIT"
        : "CANDLE_API_ERROR"
    );
  }

  const analyses = candleSets.map(analyseCandles);

  if (analyses.some(x => !x)) {
    return reject("INSUFFICIENT_CANDLES");
  }

  const [h4, h1, m30, m15, m5] = analyses;

  let direction;

  if (
    h4.trend === "BULLISH" &&
    h1.trend === "BULLISH"
  ) {
    direction = "BUY";
  } else if (
    h4.trend === "BEARISH" &&
    h1.trend === "BEARISH"
  ) {
    direction = "SELL";
  } else {
    return reject("HIGHER_TF_CONFLICT");
  }

  const expected =
    direction === "BUY" ? "BULLISH" : "BEARISH";

  if (m30.trend !== expected) {
    return reject("30M_TREND_CONFLICT");
  }

  let score = 0;

  if (m15.trend === expected) score += 2;
  if (m5.trend === expected) score += 2;

  if (m15.volumeRising) score += 1;
  if (m5.volumeRising) score += 1;

  if (direction === "BUY") {
    if (m5.rsi >= 48 && m5.rsi <= 72) score += 1;
    if (m5.price > m5.open) score += 1;
    if (m5.upperWickRatio <= 0.45) score += 1;
  } else {
    if (m5.rsi >= 28 && m5.rsi <= 52) score += 1;
    if (m5.price < m5.open) score += 1;
    if (m5.lowerWickRatio <= 0.45) score += 1;
  }

  if (score < 7) {
    return reject("CONFIRMATION_SCORE_LOW");
  }

  let book;

  try {
    book = await checkOrderbook(pair);
  } catch (error) {
    return reject(
      error.message.includes("429")
        ? "COINDCX_RATE_LIMIT"
        : "ORDERBOOK_API_ERROR"
    );
  }

  if (!book.safe) {
    return reject(book.reason || "LIQUIDITY_FAILED");
  }

  if (direction === "BUY") {
    if (book.imbalance < CONFIG.minImbalance) {
      return reject("BUY_ORDERBOOK_IMBALANCE");
    }
  } else {
    if (book.imbalance > 1 / CONFIG.minImbalance) {
      return reject("SELL_ORDERBOOK_IMBALANCE");
    }
  }

  const atrProxy = Math.max(
    m5.high - m5.low,
    m5.price * 0.002
  );

  const entry = m5.price;

  return {
    symbol: pair,
    pair,
    signal: direction,
    confirmationScore: score,
    entry,

    stopLoss: direction === "BUY"
      ? entry - atrProxy * 1.5
      : entry + atrProxy * 1.5,

    target1: direction === "BUY"
      ? entry + atrProxy * 1.5
      : entry - atrProxy * 1.5,

    target2: direction === "BUY"
      ? entry + atrProxy * 2.5
      : entry - atrProxy * 2.5,

    timeframe: "5M",

    liquidity: {
      status: "PASSED",
      spreadPercent: Number(book.spread.toFixed(4)),
      bidDepth: Number(book.bidDepth.toFixed(2)),
      askDepth: Number(book.askDepth.toFixed(2)),
      imbalance: Number(book.imbalance.toFixed(3)),
      wallConcentration:
        Number(book.wallConcentration.toFixed(3))
    },

    trend: {
      "4H": h4.trend,
      "1H": h1.trend,
      "30M": m30.trend,
      "15M": m15.trend,
      "5M": m5.trend
    },

    scannedAt: new Date().toISOString()
  };
}

async function mapAll(items) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;

      if (index >= items.length) return;

      try {
        results[index] = await scanCoin(items[index]);
      } catch {
        results[index] = reject("UNEXPECTED_ERROR");
      }

      scanStatus.checked++;
    }
  }

  const workers = Array.from(
    {
      length: Math.min(CONFIG.concurrency, items.length)
    },
    () => worker()
  );

  await Promise.all(workers);
  return results;
}

async function runScan() {
  if (scanStatus.running) return;

  scanStatus.running = true;
  scanStatus.checked = 0;
  scanStatus.signals = 0;
  scanStatus.skipped = 0;
  scanStatus.eligiblePairs = 0;

  rejectedCounts = {};

  scanStatus.message =
    "Loading all active CoinDCX USDT futures pairs";

  try {
    const [instrumentData, priceData] = await Promise.all([
      getJSON(API.instruments),
      getJSON(API.prices)
    ]);

    const instruments = getInstruments(instrumentData);
    const priceMap = getPriceMap(priceData);

    const active = instruments.filter(item => {
      if (typeof item === "string") return true;

      return (
        !item?.status ||
        String(item.status).toLowerCase() === "active"
      );
    });

    const candidates = active.map(instrument => {
      const pair = typeof instrument === "string"
        ? instrument
        : instrument?.pair;

      return {
        instrument,
        ticker: pair ? priceMap[pair] : null
      };
    }).filter(x =>
      x.instrument &&
      (
        typeof x.instrument === "string" ||
        x.instrument.pair
      )
    );

    scanStatus.totalPairs = candidates.length;

    scanStatus.skipped =
      candidates.filter(x => !x.ticker).length;

    const withTicker = candidates.filter(x => x.ticker);

    scanStatus.eligiblePairs = withTicker.length;

    scanStatus.message =
      "Deep scanning " + withTicker.length + " active pairs";

    const results = await mapAll(withTicker);

    latestResults = results.filter(
      x => x && (
        x.signal === "BUY" ||
        x.signal === "SELL"
      )
    );

    scanStatus.signals = latestResults.length;
    scanStatus.lastScan = new Date().toISOString();
    scanStatus.message = "Full market scan completed";

  } catch (error) {
    scanStatus.message = "Scan error: " + error.message;

  } finally {
    scanStatus.running = false;
  }
}

/* ================= DASHBOARD ================= */

const dashboard = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#080d18">
<title>UD Liquidity Guard Scanner</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#080d18;color:#e8edf7;font-family:Arial,sans-serif}
header{padding:20px 16px;background:#101a2b;border-bottom:1px solid #26344c}
h1{font-size:22px;margin:0 0 7px}
.sub{color:#93a4bd;font-size:12px}
main{max-width:1100px;margin:auto;padding:16px}
.status{display:flex;align-items:center;gap:8px;margin:8px 0 18px;color:#aab9ce;font-size:13px}
.dot{width:9px;height:9px;border-radius:50%;background:#f0b44d}
.dot.live{background:#24d18a}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.card{background:#111c2e;border:1px solid #253650;border-radius:12px;padding:15px}
.card small{color:#91a3bd;font-size:12px}
.card strong{display:block;font-size:23px;margin-top:9px}
.green{color:#20d997}.red{color:#ff6577}
.panel{background:#111c2e;border:1px solid #253650;border-radius:12px;padding:15px;margin-top:16px}
.panel h2{font-size:16px;margin:0 0 14px}
.progress{height:9px;background:#26354c;border-radius:10px;overflow:hidden}
.bar{height:100%;width:0;background:#27c994;transition:width .3s}
.row{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap}
button{border:0;border-radius:8px;padding:11px 15px;font-weight:bold;background:#20c997;color:#07131a;cursor:pointer}
button.secondary{background:#263650;color:#e8edf7}
button:disabled{opacity:.5}
input{background:#091321;color:white;border:1px solid #344761;padding:11px;border-radius:8px;min-width:0}
.controls{display:flex;gap:8px;flex-wrap:wrap}
.controls input{flex:1}
.tablewrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:680px;font-size:12px}
th,td{text-align:left;padding:12px 9px;border-bottom:1px solid #263650;white-space:nowrap}
th{color:#91a3bd}
.pill{padding:5px 8px;border-radius:6px;font-weight:bold}
.buy{background:#123e34;color:#42e4ad}
.sell{background:#451d2a;color:#ff8090}
.muted{color:#91a3bd}
#message{font-size:13px;color:#aab9ce;margin:12px 0}
#rejections{display:flex;gap:7px;flex-wrap:wrap}
.tag{background:#202e43;padding:7px 9px;border-radius:7px;font-size:11px;color:#c0cde0}
footer{text-align:center;color:#687d99;font-size:11px;padding:25px}
@media(max-width:600px){
 .cards{grid-template-columns:repeat(2,1fr)}
 h1{font-size:19px}
 main{padding:11px}
 .card{padding:12px}
 .card strong{font-size:20px}
}
</style>
</head>
<body>
<header>
 <h1>UD Liquidity Guard Scanner</h1>
 <div class="sub">CoinDCX Futures · Multi-Timeframe Analysis · 5M Entry</div>
</header>
<main>
 <div class="status"><span class="dot" id="dot"></span><span id="state">Connecting to scanner...</span></div>

 <section class="cards">
  <div class="card"><small>Total pairs</small><strong id="total">--</strong></div>
  <div class="card"><small>Checked pairs</small><strong id="checked">--</strong></div>
  <div class="card"><small>BUY / SELL signals</small><strong id="signals">--</strong></div>
  <div class="card"><small>Skipped pairs</small><strong id="skipped">--</strong></div>
 </section>

 <section class="panel">
  <div class="row">
   <h2>Market scan progress</h2>
   <span class="muted" id="percent">0%</span>
  </div>
  <div class="progress"><div class="bar" id="bar"></div></div>
  <div id="message">Waiting for scanner...</div>
  <div class="row">
   <span class="muted" id="lastscan">Last scan: --</span>
   <button id="scanBtn" onclick="startScan()">START NEW SCAN</button>
  </div>
 </section>

 <section class="panel">
  <h2>Manual coin search</h2>
  <div class="controls">
   <input id="pair" placeholder="Enter pair e.g. BTCUSDT">
   <button onclick="searchPair()">SEARCH</button>
  </div>
  <div id="searchResult" class="muted" style="margin-top:12px"></div>
 </section>

 <section class="panel">
  <div class="row">
   <h2>Trade-ready signals</h2>
   <span class="muted">Only BUY / SELL</span>
  </div>
  <div class="tablewrap">
   <table>
    <thead>
     <tr>
      <th>Coin</th><th>Signal</th><th>Entry</th>
      <th>Stop Loss</th><th>Target 1</th><th>Target 2</th>
      <th>Score</th><th>Liquidity</th>
     </tr>
    </thead>
    <tbody id="signalRows">
     <tr><td colspan="8" class="muted">No signals available yet</td></tr>
    </tbody>
   </table>
  </div>
 </section>

 <section class="panel">
  <h2>Rejected pair reasons</h2>
  <div id="rejections" class="muted">Waiting for scan data...</div>
 </section>
</main>
<footer>UD Liquidity Guard Scanner · Data from CoinDCX public market APIs</footer>

<script>
const el = id => document.getElementById(id);

function money(value){
 if(value===null || value===undefined || !Number.isFinite(Number(value))) return "--";
 return Number(value).toPrecision(8).replace(/0+$/,"").replace(/\\.$/,"");
}

function time(value){
 if(!value) return "--";
 const d=new Date(value);
 return isNaN(d.getTime()) ? "--" : d.toLocaleString();
}

async function getAPI(path){
 const response=await fetch(path,{cache:"no-store"});
 if(!response.ok) throw new Error("API error "+response.status);
 return response.json();
}

async function refresh(){
 try{
  const [status,signalData,rejectionData]=await Promise.all([
   getAPI("/api/status"),
   getAPI("/api/signals"),
   getAPI("/api/rejections")
  ]);

  el("total").textContent=status.totalPairs ?? 0;
  el("checked").textContent=(status.checked ?? 0)+" / "+(status.eligiblePairs ?? 0);
  el("signals").textContent=signalData.count ?? 0;
  el("skipped").textContent=status.skipped ?? 0;

  const total=Number(status.eligiblePairs || 0);
  const checked=Number(status.checked || 0);
  const percent=total ? Math.min(100,checked/total*100) : 0;

  el("bar").style.width=percent+"%";
  el("percent").textContent=percent.toFixed(1)+"%";
  el("message").textContent=status.message || "Waiting...";
  el("lastscan").textContent="Last completed scan: "+time(status.lastScan);

  el("dot").className=status.running ? "dot live" : "dot";
  el("state").textContent=status.running ? "SCANNER RUNNING" : "SCANNER IDLE";
  el("scanBtn").disabled=!!status.running;
  el("scanBtn").textContent=status.running ? "SCANNING..." : "START NEW SCAN";

  renderSignals(signalData.signals || []);
  renderRejections(rejectionData.rejected || {});

 }catch(error){
  el("state").textContent="Connection error: "+error.message;
  el("dot").className="dot";
 }
}

function renderSignals(signals){
 const body=el("signalRows");

 if(!signals.length){
  body.innerHTML='<tr><td colspan="8" class="muted">No trade-ready BUY / SELL signals currently available.</td></tr>';
  return;
 }

 body.innerHTML=signals.map(s=>{
  const cls=s.signal==="BUY" ? "buy" : "sell";
  return '<tr>'+
   '<td><b>'+safe(s.symbol || s.pair)+'</b></td>'+
   '<td><span class="pill '+cls+'">'+safe(s.signal)+'</span></td>'+
   '<td>'+money(s.entry)+'</td>'+
   '<td>'+money(s.stopLoss)+'</td>'+
   '<td>'+money(s.target1)+'</td>'+
   '<td>'+money(s.target2)+'</td>'+
   '<td>'+safe(s.confirmationScore)+'/9</td>'+
   '<td><span class="pill buy">PASSED</span></td>'+
  '</tr>';
 }).join("");
}

function renderRejections(data){
 const box=el("rejections");
 const entries=Object.entries(data).sort((a,b)=>b[1]-a[1]);

 if(!entries.length){
  box.textContent="No rejection data yet.";
  return;
 }

 box.innerHTML=entries.map(([name,count])=>
  '<span class="tag">'+safe(name)+': '+Number(count)+'</span>'
 ).join("");
}

function safe(value){
 return String(value ?? "").replace(/[&<>"']/g,c=>({
  "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
 }[c]));
}

async function startScan(){
 el("scanBtn").disabled=true;
 try{
  await getAPI("/api/scan");
  await refresh();
 }catch(error){
  el("message").textContent=error.message;
 }
}

async function searchPair(){
 const pair=el("pair").value.trim().toUpperCase();
 if(!pair){
  el("searchResult").textContent="Enter a CoinDCX pair first.";
  return;
 }

 el("searchResult").textContent="Searching "+pair+"...";
 try{
  const data=await getAPI("/api/search?pair="+encodeURIComponent(pair));

  if(data.signal==="NO TRADE"){
   el("searchResult").textContent=pair+": NO TRADE — "+data.message;
   return;
  }

  el("searchResult").innerHTML=
   '<b>'+safe(data.pair)+'</b> · <span class="pill '+(data.signal==="BUY"?"buy":"sell")+'">'+safe(data.signal)+'</span><br>'+
   'Entry: '+money(data.entry)+'<br>'+
   'Stop Loss: '+money(data.stopLoss)+'<br>'+
   'Target 1: '+money(data.target1)+'<br>'+
   'Target 2: '+money(data.target2)+'<br>'+
   'Confirmation: '+safe(data.confirmationScore)+'/9<br>'+
   'Liquidity: PASSED';

 }catch(error){
  el("searchResult").textContent="Search failed: "+error.message;
 }
}

el("pair").addEventListener("keydown",e=>{
 if(e.key==="Enter") searchPair();
});

refresh();
setInterval(refresh,3000);
</script>
</body>
</html>`;

/* Dashboard is now the home page */
app.get("/", (req, res) => {
  res.type("html").send(dashboard);
});

/* ================= API ROUTES ================= */

app.get("/api/status", (req, res) => {
  res.json(scanStatus);
});

app.get("/api/signals", (req, res) => {
  res.json({
    count: latestResults.length,
    signals: latestResults
  });
});

app.get("/api/rejections", (req, res) => {
  res.json({
    rejected: rejectedCounts
  });
});

app.get("/api/scan", (req, res) => {
  if (scanStatus.running) {
    return res.json({
      message: "Scan already running",
      status: scanStatus
    });
  }

  runScan();

  res.json({
    message: "Full market scan started",
    status: scanStatus
  });
});

app.get("/api/search", async (req, res) => {
  const pair = String(req.query.pair || "").trim().toUpperCase();

  if (!pair) {
    return res.status(400).json({
      error: "pair query is required"
    });
  }

  try {
    const [instrumentData, priceData] = await Promise.all([
      getJSON(API.instruments),
      getJSON(API.prices)
    ]);

    const instruments = getInstruments(instrumentData);
    const priceMap = getPriceMap(priceData);

    const instrument = instruments.find(x =>
      (typeof x === "string" ? x : x?.pair) === pair
    );

    const ticker = priceMap[pair];

    if (!instrument || !ticker) {
      return res.status(404).json({
        error: "Active CoinDCX pair or ticker not found",
        pair
      });
    }

    const result = await scanCoin({
      instrument,
      ticker
    });

    if (!result) {
      return res.json({
        pair,
        signal: "NO TRADE",
        message: "Mandatory confirmations not passed"
      });
    }

    return res.json(result);

  } catch (error) {
    return res.status(502).json({
      error: error.message
    });
  }
});

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    time: new Date().toISOString()
  });
});

app.listen(PORT, () => {
  console.log(
    "UD Liquidity Guard running on port " + PORT
  );

  runScan();

  setInterval(
    runScan,
    CONFIG.scanIntervalMs
  );
});
