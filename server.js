
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
  shortlist: 35,
  concurrency: 4,
  minQuoteVolume: 100000,
  minBookDepth: 1500,
  maxSpreadPercent: 0.12,
  minImbalance: 1.15,
  maxWallConcentration: 0.45,
  scanIntervalMs: 60000
};

let latestResults = [];
let scanStatus = {
  running: false,
  lastScan: null,
  totalPairs: 0,
  checked: 0,
  signals: 0,
  message: "Waiting for first scan"
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function getJSON(url) {
  const response = await fetch(url, {
    headers: { "User-Agent": "UD-Liquidity-Guard-Scanner/1.0" },
    signal: AbortSignal.timeout(15000)
  });

  if (!response.ok) {
    throw new Error(`CoinDCX API error: ${response.status}`);
  }

  return response.json();
}

function number(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function getInstrumentList(data) {
  if (Array.isArray(data)) return data;

  if (data && Array.isArray(data.instruments)) {
    return data.instruments;
  }

  if (data && Array.isArray(data.data)) {
    return data.data;
  }

  return [];
}

function ema(values, period) {
  if (values.length < period) return null;

  const multiplier = 2 / (period + 1);
  let result = values.slice(0, period)
    .reduce((a, b) => a + b, 0) / period;

  for (let i = period; i < values.length; i++) {
    result = (values[i] * multiplier) +
      (result * (1 - multiplier));
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

  const rs = gains / losses;
  return 100 - (100 / (1 + rs));
}

function analyseCandles(candles) {
  if (!Array.isArray(candles) || candles.length < 205) {
    return null;
  }

  const sorted = [...candles].sort(
    (a, b) => number(a.time) - number(b.time)
  );

  const completed = sorted.slice(0, -1);

  if (completed.length < 200) return null;

  const closes = completed.map(c => number(c.close));
  const volumes = completed.map(c => number(c.volume));
  const last = completed[completed.length - 1];
  const previous = completed[completed.length - 2];

  const price = number(last.close);
  const open = number(last.open);
  const high = number(last.high);
  const low = number(last.low);
  const candleRange = high - low;

  if (!price || !candleRange) return null;

  const ema20 = ema(closes, 20);
  const ema50 = ema(closes, 50);
  const ema200 = ema(closes, 200);
  const currentRsi = rsi(closes);

  const recentVolume =
    volumes.slice(-20).reduce((a, b) => a + b, 0) / 20;

  const previousVolume =
    volumes.slice(-40, -20).reduce((a, b) => a + b, 0) / 20;

  const volumeRising = recentVolume > previousVolume;

  const upperWick = high - Math.max(open, price);
  const lowerWick = Math.min(open, price) - low;

  const upperWickRatio = upperWick / candleRange;
  const lowerWickRatio = lowerWick / candleRange;

  let trend = "SIDEWAYS";

  if (price > ema20 && ema20 > ema50 && ema50 > ema200) {
    trend = "BULLISH";
  } else if (price < ema20 && ema20 < ema50 && ema50 < ema200) {
    trend = "BEARISH";
  }

  return {
    price,
    open,
    high,
    low,
    ema20,
    ema50,
    ema200,
    rsi: currentRsi,
    trend,
    volumeRising,
    upperWickRatio,
    lowerWickRatio,
    lastTime: number(last.time),
    previousClose: number(previous.close)
  };
}

async function getCandles(pair, resolution, count = 205) {
  const now = Math.floor(Date.now() / 1000);

  const minutes = {
    "5": 5,
    "15": 15,
    "30": 30,
    "60": 60,
    "240": 240
  }[resolution];

  const from = now - (minutes * 60 * count);

  const url = new URL(API.candles);
  url.searchParams.set("pair", pair);
  url.searchParams.set("from", String(from));
  url.searchParams.set("to", String(now));
  url.searchParams.set("resolution", resolution);
  url.searchParams.set("pcode", "f");

  const result = await getJSON(url.toString());

  if (Array.isArray(result)) return result;
  return result.data || [];
}

function parseBookSide(side) {
  if (!side) return [];

  if (Array.isArray(side)) {
    return side.map(level => ({
      price: number(level.price ?? level[0]),
      qty: number(level.quantity ?? level.qty ?? level[1])
    })).filter(x => x.price > 0 && x.qty > 0);
  }

  return Object.entries(side).map(([price, qty]) => ({
    price: number(price),
    qty: number(qty)
  })).filter(x => x.price > 0 && x.qty > 0);
}

async function checkOrderbook(instrument) {
  const url =
    `${API.orderbook}/${encodeURIComponent(instrument)}-futures/50`;

  const data = await getJSON(url);

  const bids = parseBookSide(data.bids)
    .sort((a, b) => b.price - a.price);

  const asks = parseBookSide(data.asks)
    .sort((a, b) => a.price - b.price);

  if (!bids.length || !asks.length) {
    return { safe: false, reason: "ORDERBOOK_EMPTY" };
  }

  const bestBid = bids[0].price;
  const bestAsk = asks[0].price;
  const mid = (bestBid + bestAsk) / 2;

  if (!mid || bestAsk <= bestBid) {
    return { safe: false, reason: "INVALID_ORDERBOOK" };
  }

  const spread = ((bestAsk - bestBid) / mid) * 100;

  const bidDepth = bids.reduce(
    (sum, x) => sum + x.price * x.qty, 0
  );

  const askDepth = asks.reduce(
    (sum, x) => sum + x.price * x.qty, 0
  );

  const totalDepth = bidDepth + askDepth;
  const imbalance = bidDepth / Math.max(askDepth, 0.000001);

  const largestBid = Math.max(
    ...bids.map(x => x.price * x.qty)
  );

  const largestAsk = Math.max(
    ...asks.map(x => x.price * x.qty)
  );

  const wallConcentration =
    Math.max(largestBid, largestAsk) /
    Math.max(totalDepth, 0.000001);

  const checks = {
    spread: spread <= CONFIG.maxSpreadPercent,
    depth: Math.min(bidDepth, askDepth) >= CONFIG.minBookDepth,
    walls: wallConcentration <= CONFIG.maxWallConcentration
  };

  const failures = Object.entries(checks)
    .filter(([, passed]) => !passed)
    .map(([name]) => name.toUpperCase());

  return {
    safe: failures.length === 0,
    reason: failures.join(", "),
    spread,
    bidDepth,
    askDepth,
    totalDepth,
    imbalance,
    wallConcentration
  };
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runWorker() {
    while (true) {
      const index = nextIndex++;
      if (index >= items.length) return;

      try {
        results[index] = await worker(items[index], index);
      } catch (error) {
        results[index] = {
          error: error.message
        };
      }

      scanStatus.checked++;
      await sleep(150);
    }
  }

  await Promise.all(
    Array.from(
      { length: Math.min(limit, items.length) },
      () => runWorker()
    )
  );

  return results;
}

async function scanCoin(instrument, ticker) {
  const pair = instrument.pair || instrument;
  const symbol = String(pair).replace(/^B-/, "").replace("_USDT", "");

  const price = number(ticker.ls);
  const quoteVolume = number(ticker.v) * price;

  if (!price || quoteVolume < CONFIG.minQuoteVolume) {
    return null;
  }

  const resolutions = ["240", "60", "30", "15", "5"];

  const candleSets = await Promise.all(
    resolutions.map(r => getCandles(pair, r))
  );

  const analyses = candleSets.map(analyseCandles);

  if (analyses.some(x => !x)) {
    return null;
  }

  const [h4, h1, m30, m15, m5] = analyses;

  const book = await checkOrderbook(pair);

  if (!book.safe) {
    return null;
  }

  const higherTrends = [h4.trend, h1.trend, m30.trend];

  let direction = null;

  if (higherTrends.every(x => x === "BULLISH")) {
    direction = "BUY";
  } else if (higherTrends.every(x => x === "BEARISH")) {
    direction = "SELL";
  } else {
    return null;
  }

  if (m15.trend !== (direction === "BUY" ? "BULLISH" : "BEARISH")) {
    return null;
  }

  if (m5.trend !== (direction === "BUY" ? "BULLISH" : "BEARISH")) {
    return null;
  }

  if (!m15.volumeRising || !m5.volumeRising) {
    return null;
  }

  if (direction === "BUY") {
    if (m5.rsi < 50 || m5.rsi > 72) return null;
    if (m5.upperWickRatio > 0.45) return null;
    if (book.imbalance < CONFIG.minImbalance) return null;
    if (m5.price <= m5.open) return null;
  } else {
    if (m5.rsi < 28 || m5.rsi > 50) return null;
    if (m5.lowerWickRatio > 0.45) return null;
    if (book.imbalance > 1 / CONFIG.minImbalance) return null;
    if (m5.price >= m5.open) return null;
  }

  const atrProxy = Math.max(
    m5.high - m5.low,
    m5.price * 0.002
  );

  const entry = m5.price;
  const stopLoss = direction === "BUY"
    ? entry - atrProxy * 1.5
    : entry + atrProxy * 1.5;

  const target1 = direction === "BUY"
    ? entry + atrProxy * 1.5
    : entry - atrProxy * 1.5;

  const target2 = direction === "BUY"
    ? entry + atrProxy * 2.5
    : entry - atrProxy * 2.5;

  return {
    symbol,
    pair,
    signal: direction,
    entry,
    stopLoss,
    target1,
    target2,
    timeframe: "5M",
    higherTimeframeTrend: direction,
    liquidity: {
      status: "PASSED",
      spreadPercent: Number(book.spread.toFixed(4)),
      bidDepth: Number(book.bidDepth.toFixed(2)),
      askDepth: Number(book.askDepth.toFixed(2)),
      orderbookImbalance: Number(book.imbalance.toFixed(3)),
      wallConcentration: Number(book.wallConcentration.toFixed(3))
    },
    checks: {
      "4H": h4.trend,
      "1H": h1.trend,
      "30M": m30.trend,
      "15M": m15.trend,
      "5M": m5.trend,
      volume: "RISING",
      liquidity: "PASSED"
    },
    scannedAt: new Date().toISOString()
  };
}

async function runScan() {
  if (scanStatus.running) return;

  scanStatus.running = true;
  scanStatus.checked = 0;
  scanStatus.signals = 0;
  scanStatus.message = "Fetching CoinDCX markets...";

  try {
    const [instrumentData, priceData] = await Promise.all([
      getJSON(API.instruments),
      getJSON(API.prices)
    ]);

    const instruments = getInstrumentList(instrumentData);
    const prices = priceData.prices || {};

    const active = instruments.filter(item => {
      if (typeof item === "string") return true;
      return !item.status || item.status === "active";
    });

    scanStatus.totalPairs = active.length;

    const candidates = active
      .map(item => {
        const pair = typeof item === "string" ? item : item.pair;
        return {
          instrument: item,
          ticker: prices[pair]
        };
      })
      .filter(x => x.ticker && number(x.ticker.ls) > 0)
      .sort((a, b) =>
        (number(b.ticker.v) * number(b.ticker.ls)) -
        (number(a.ticker.v) * number(a.ticker.ls))
      )
      .slice(0, CONFIG.shortlist);

    scanStatus.message =
      `Checking ${candidates.length} shortlisted pairs out of ${active.length}`;

    const results = await mapLimit(
      candidates,
      CONFIG.concurrency,
      async item => scanCoin(item.instrument, item.ticker)
    );

    latestResults = results.filter(
      x => x && !x.error && (x.signal === "BUY" || x.signal === "SELL")
    );

    scanStatus.signals = latestResults.length;
    scanStatus.lastScan = new Date().toISOString();
    scanStatus.message = "Scan completed";
  } catch (error) {
    scanStatus.message = error.message;
  } finally {
    scanStatus.running = false;
  }
}

app.get("/", (req, res) => {
  res.json({
    name: "UD Liquidity Guard Scanner",
    status: "Online",
    endpoints: ["/api/scan", "/api/signals", "/api/status"]
  });
});

app.get("/api/status", (req, res) => {
  res.json(scanStatus);
});

app.get("/api/signals", (req, res) => {
  res.json({
    count: latestResults.length,
    signals: latestResults
  });
});

app.get("/api/scan", async (req, res) => {
  if (scanStatus.running) {
    return res.json({
      message: "Scan already running",
      status: scanStatus
    });
  }

  runScan();

  res.json({
    message: "Scan started",
    status: scanStatus
  });
});

app.get("/api/health", (req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`UD Liquidity Guard running on port ${PORT}`);
  runScan();
  setInterval(runScan, CONFIG.scanIntervalMs);
});
