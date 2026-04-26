// InvestOS Ticker Proxy — Netlify Serverless Function v4
// Twelve Data API — handles TSX (.TO), US stocks, crypto, ETFs
// Free tier: 800 calls/day

const https = require('https');

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: { 'User-Agent': 'InvestOS/1.0', 'Accept': 'application/json' }
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject);
    req.setTimeout(12000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

// Parse ticker → Twelve Data symbol + exchange params
// CNQ.TO → symbol=CNQ&exchange=TSX
// NVDA   → symbol=NVDA
// BTC-USD → symbol=BTC/USD (crypto)
function parseSymbol(raw) {
  const upper = raw.toUpperCase();

  // Crypto: BTC-USD → BTC/USD
  if (upper.endsWith('-USD') || upper.endsWith('-USDT')) {
    return { symbol: upper.replace('-', '/'), exchange: '' };
  }

  // TSX: CNQ.TO → CNQ + exchange=TSX
  if (upper.endsWith('.TO')) {
    return { symbol: upper.replace('.TO', ''), exchange: 'TSX' };
  }

  // TSX Venture: CNQ.V → CNQ + exchange=TSXV
  if (upper.endsWith('.V')) {
    return { symbol: upper.replace('.V', ''), exchange: 'TSXV' };
  }

  // US stock — no exchange needed
  return { symbol: upper, exchange: '' };
}

function buildURL(base, params) {
  const q = Object.entries(params)
    .filter(([, v]) => v)
    .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
    .join('&');
  return `${base}?${q}`;
}

function computeFields(closes, highs, lows, volumes) {
  const price     = closes.length ? closes[closes.length - 1] : 0;
  const prevClose = closes.length > 1 ? closes[closes.length - 2] : price;
  const dayChg    = prevClose ? ((price - prevClose) / prevClose * 100) : 0;

  const ma = (arr, n) => arr.length >= n
    ? arr.slice(-n).reduce((a, b) => a + b, 0) / n : null;

  const ma20  = ma(closes, 20);
  const ma50  = ma(closes, 50);
  const ma200 = ma(closes, 200);

  const c252  = closes.slice(-252);
  const high52 = c252.length ? Math.max(...c252) : price;
  const low52  = c252.length ? Math.min(...c252) : price;

  // RSI 14
  let rsi = 50;
  if (closes.length >= 15) {
    const diffs  = closes.slice(-15).map((c, i, a) => i === 0 ? 0 : c - a[i - 1]).slice(1);
    const gains  = diffs.map(d => d > 0 ? d : 0);
    const losses = diffs.map(d => d < 0 ? Math.abs(d) : 0);
    const avgG   = gains.reduce((a, b) => a + b, 0) / 14;
    const avgL   = losses.reduce((a, b) => a + b, 0) / 14;
    rsi = avgL === 0 ? 100 : Math.round(100 - 100 / (1 + avgG / avgL));
  }

  // ATR 14
  let atr = 0;
  if (highs.length >= 14 && lows.length >= 14) {
    const trs = highs.slice(-14).map((h, i) => h - lows.slice(-14)[i]);
    atr = trs.reduce((a, b) => a + b, 0) / 14;
  }

  const avgVol   = volumes.length
    ? Math.round(volumes.slice(-20).reduce((a, b) => a + b, 0) / Math.min(20, volumes.length))
    : 0;
  const todayVol = volumes.length ? volumes[volumes.length - 1] : 0;

  const perf = n => closes.length > n
    ? ((closes[closes.length - 1] - closes[closes.length - 1 - n]) / closes[closes.length - 1 - n] * 100)
    : 0;

  return {
    price, prevClose, dayChg,
    ma20, ma50, ma200,
    aboveMa20: ma20 != null ? price > ma20 : false,
    aboveMa50: ma50 != null ? price > ma50 : false,
    aboveMa200: ma200 != null ? price > ma200 : false,
    high52, low52,
    range52pos: high52 > low52 ? Math.round((price - low52) / (high52 - low52) * 100) : 50,
    perf5d: perf(5), perf20d: perf(20), perf60d: perf(60),
    rsi, atr,
    avgVol, todayVol,
    volRatio: avgVol > 0 ? todayVol / avgVol : 1,
  };
}

exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
    'Cache-Control': 'public, max-age=300',
  };

  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers, body: '' };

  const raw    = (event.queryStringParameters?.s || '').trim();
  const APIKEY = process.env.TWELVE_DATA_KEY;

  if (!raw)    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing ?s= param' }) };
  if (!APIKEY) return { statusCode: 500, headers, body: JSON.stringify({ error: 'TWELVE_DATA_KEY not set' }) };

  const { symbol, exchange } = parseSymbol(raw);

  try {
    // Fetch quote + time_series in parallel
    const baseParams = { symbol, apikey: APIKEY };
    if (exchange) baseParams.exchange = exchange;

    const quoteURL = buildURL('https://api.twelvedata.com/quote', baseParams);
    const tsURL    = buildURL('https://api.twelvedata.com/time_series', {
      ...baseParams, interval: '1day', outputsize: '130',
    });

    const [qRes, tsRes] = await Promise.all([httpsGet(quoteURL), httpsGet(tsURL)]);

    const quote = JSON.parse(qRes.body);
    const ts    = JSON.parse(tsRes.body);

    // Twelve Data returns {code, message} on error
    if (quote.code || quote.status === 'error') {
      return {
        statusCode: 404, headers,
        body: JSON.stringify({ error: quote.message || `Symbol not found: ${raw}` }),
      };
    }

    // Build closes/volumes arrays (Twelve Data returns newest first → reverse)
    const values   = (ts.values || []).reverse();
    const closes   = values.map(d => parseFloat(d.close));
    const volumes  = values.map(d => parseInt(d.volume || 0));
    const highs    = values.map(d => parseFloat(d.high));
    const lows     = values.map(d => parseFloat(d.low));
    const timestamps = values.map(d => Math.floor(new Date(d.datetime).getTime() / 1000));

    const computed = computeFields(closes, highs, lows, volumes);

    // Return in Yahoo chart shape so parseYahooChart works
    const result = {
      chart: {
        result: [{
          meta: {
            symbol:                     raw.toUpperCase(),
            longName:                   quote.name || raw,
            shortName:                  quote.name || raw,
            currency:                   quote.currency || 'USD',
            exchangeName:               quote.exchange || exchange || '',
            instrumentType:             quote.type || 'Common Stock',
            regularMarketPrice:         computed.price,
            regularMarketPreviousClose: computed.prevClose,
            regularMarketChangePercent: computed.dayChg,
            marketCap:                  null,
            fiftyTwoWeekHigh:           computed.high52,
            fiftyTwoWeekLow:            computed.low52,
            sector: '', industry: '',
          },
          timestamp: timestamps,
          indicators: {
            quote: [{ close: closes, volume: volumes, high: highs, low: lows }]
          },
          _computed: { ...computed, closes, volumes, timestamps },
        }],
        error: null,
      }
    };

    return { statusCode: 200, headers, body: JSON.stringify(result) };

  } catch (err) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};
