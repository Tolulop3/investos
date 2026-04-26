// InvestOS Ticker Proxy — Netlify Serverless Function v3
// Uses Twelve Data API — works from cloud IPs, has TSX stocks, 6mo OHLCV
// Free tier: 800 calls/day — sufficient for lookup tab usage

const https = require('https');

function httpsGet(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'InvestOS/1.0',
        'Accept': 'application/json',
      }
    }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString() }));
    });
    req.on('error', reject);
    req.setTimeout(12000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

// Convert Twelve Data response → same shape as parseYahooChart expects
function buildChartResponse(quote, timeseries, ticker) {
  const closes    = (timeseries || []).map(d => parseFloat(d.close)).reverse();
  const volumes   = (timeseries || []).map(d => parseInt(d.volume || 0)).reverse();
  const highs     = (timeseries || []).map(d => parseFloat(d.high)).reverse();
  const lows      = (timeseries || []).map(d => parseFloat(d.low)).reverse();
  const timestamps = (timeseries || []).map(d => Math.floor(new Date(d.datetime).getTime()/1000)).reverse();

  const price     = parseFloat(quote.close || quote.price || 0);
  const prevClose = parseFloat(quote.previous_close || price);
  const dayChg    = prevClose ? ((price - prevClose) / prevClose * 100) : 0;

  // Moving averages from closes array
  const ma = (arr, n) => arr.length >= n
    ? arr.slice(-n).reduce((a,b) => a+b, 0) / n : null;

  const ma20  = ma(closes, 20);
  const ma50  = ma(closes, 50);
  const ma200 = ma(closes, 200);

  const high52 = closes.length ? Math.max(...closes.slice(-252)) : 0;
  const low52  = closes.length ? Math.min(...closes.slice(-252)) : 0;

  // RSI (14)
  let rsi = 50;
  if (closes.length >= 15) {
    const diffs = closes.slice(-15).map((c,i,a) => i===0 ? 0 : c - a[i-1]).slice(1);
    const gains = diffs.map(d => d > 0 ? d : 0);
    const losses = diffs.map(d => d < 0 ? Math.abs(d) : 0);
    const avgG = gains.reduce((a,b)=>a+b,0)/14;
    const avgL = losses.reduce((a,b)=>a+b,0)/14;
    rsi = avgL === 0 ? 100 : Math.round(100 - 100/(1 + avgG/avgL));
  }

  // ATR (14)
  let atr = 0;
  if (highs.length >= 14 && lows.length >= 14) {
    const trs = highs.slice(-14).map((h,i) => h - lows.slice(-14)[i]);
    atr = trs.reduce((a,b)=>a+b,0)/14;
  }

  const avgVol   = volumes.length ? Math.round(volumes.slice(-20).reduce((a,b)=>a+b,0)/Math.min(20,volumes.length)) : 0;
  const todayVol = volumes.length ? volumes[volumes.length-1] : 0;

  const perf = (n) => closes.length >= n
    ? ((closes[closes.length-1] - closes[closes.length-1-n]) / closes[closes.length-1-n] * 100) : 0;

  return {
    chart: {
      result: [{
        meta: {
          symbol:                    ticker,
          longName:                  quote.name || ticker,
          shortName:                 quote.name || ticker,
          currency:                  quote.currency || 'USD',
          exchangeName:              quote.exchange || '',
          instrumentType:            quote.type || 'Common Stock',
          regularMarketPrice:        price,
          regularMarketPreviousClose: prevClose,
          regularMarketChangePercent: dayChg,
          marketCap:                 null,
          fiftyTwoWeekHigh:          high52,
          fiftyTwoWeekLow:           low52,
          sector:                    '',
          industry:                  '',
        },
        timestamp: timestamps,
        indicators: {
          quote: [{ close: closes, volume: volumes, high: highs, low: lows }]
        },
        // Extra pre-computed fields for parseYahooChart
        _computed: {
          price, prevClose, dayChg,
          ma20, ma50, ma200,
          aboveMa20: ma20 ? price > ma20 : false,
          aboveMa50: ma50 ? price > ma50 : false,
          aboveMa200: ma200 ? price > ma200 : false,
          high52, low52,
          range52pos: high52 > low52 ? Math.round((price - low52)/(high52-low52)*100) : 50,
          perf5d:  perf(5),
          perf20d: perf(20),
          perf60d: perf(60),
          rsi, atr,
          avgVol, todayVol,
          volRatio: avgVol > 0 ? todayVol/avgVol : 1,
          closes, volumes, timestamps,
        }
      }],
      error: null
    }
  };
}

exports.handler = async (event) => {
  const corsHeaders = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type':                 'application/json',
    'Cache-Control':                'public, max-age=300',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: corsHeaders, body: '' };
  }

  const raw    = (event.queryStringParameters?.s || '').trim().toUpperCase();
  const APIKEY = process.env.TWELVE_DATA_KEY;

  if (!raw) return { statusCode: 400, headers: corsHeaders, body: JSON.stringify({ error: 'Missing ?s= param' }) };
  if (!APIKEY) return { statusCode: 500, headers: corsHeaders, body: JSON.stringify({ error: 'API key not configured' }) };

  // Twelve Data uses different format: CNQ.TO → CNQ:TSX, NVDA → NVDA:NASDAQ
  // But actually Twelve Data accepts Yahoo-style symbols directly for most
  // Just pass the symbol as-is — it handles .TO suffix for TSX
  const symbol = raw;

  try {
    // Fetch quote and time series in parallel
    const quoteUrl = `https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbol)}&apikey=${APIKEY}`;
    const tsUrl    = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(symbol)}&interval=1day&outputsize=130&apikey=${APIKEY}`;

    const [qRes, tsRes] = await Promise.all([
      httpsGet(quoteUrl),
      httpsGet(tsUrl),
    ]);

    if (qRes.status !== 200) {
      return { statusCode: qRes.status, headers: corsHeaders, body: JSON.stringify({ error: `Quote fetch failed: ${qRes.status}` }) };
    }

    const quote = JSON.parse(qRes.body);
    const ts    = JSON.parse(tsRes.body);

    // Check for API errors
    if (quote.status === 'error' || quote.code) {
      return { statusCode: 404, headers: corsHeaders, body: JSON.stringify({ error: quote.message || 'Symbol not found' }) };
    }

    const timeseries = ts.values || [];

    const result = buildChartResponse(quote, timeseries, symbol);

    return {
      statusCode: 200,
      headers:    corsHeaders,
      body:       JSON.stringify(result),
    };

  } catch (err) {
    return {
      statusCode: 500,
      headers:    corsHeaders,
      body:       JSON.stringify({ error: err.message }),
    };
  }
};
