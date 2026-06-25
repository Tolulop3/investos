// netlify/functions/ticker.js
// InvestOS Yahoo Finance proxy — server-side fetch bypasses CORS + crumb requirement
//
// SECURITY:
//   - Requires x-investos-key header matching INVESTOS_API_KEY env var
//   - CORS restricted to investos-proxy.netlify.app only
//   - Input sanitized: uppercase, whitespace stripped, length capped
//   - No stack traces in error responses
//   - Set INVESTOS_API_KEY in Netlify → Site Settings → Environment Variables

const https = require('https');

// ── Allowed origin (your Netlify site only) ──────────────────────────────────
const ALLOWED_ORIGIN = 'https://investos-proxy.netlify.app';

// ── Rate limiting — prevents token burn from bots/scrapers ───────────────────
// In-memory: resets on each cold start (Netlify functions are ephemeral)
// Limit: max 30 calls per 10-minute window globally
const RATE_WINDOW_MS  = 10 * 60 * 1000;  // 10 minutes
const RATE_LIMIT      = 30;               // max calls per window
let   _rateCount      = 0;
let   _rateWindowStart = Date.now();

function checkRateLimit() {
  const now = Date.now();
  if (now - _rateWindowStart > RATE_WINDOW_MS) {
    _rateCount = 0;
    _rateWindowStart = now;
  }
  _rateCount++;
  return _rateCount <= RATE_LIMIT;
}

function httpsGet(url, headers) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers }, (res) => {
      let data = '';
      const cookies = res.headers['set-cookie'] || [];
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, data, cookies }));
    });
    req.on('error', reject);
    req.setTimeout(12000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

exports.handler = async function(event) {
  // ── CORS headers — restricted to your site only ──────────────────────────
  const origin = event.headers && (event.headers.origin || event.headers.Origin);
  const corsOrigin = (origin === ALLOWED_ORIGIN) ? ALLOWED_ORIGIN : 'null';

  const cors = {
    'Access-Control-Allow-Origin':  corsOrigin,
    'Access-Control-Allow-Headers': 'Content-Type, x-investos-key',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Content-Type': 'application/json',
  };

  // ── Handle CORS preflight ─────────────────────────────────────────────────
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: cors, body: '' };
  }

  // ── AUTH CHECK: require secret key header ────────────────────────────────
  // Set INVESTOS_API_KEY in Netlify → Site Settings → Environment Variables
  // Dashboard must send: fetch(url, { headers: { 'x-investos-key': 'YOUR_KEY' } })
  const expectedKey = process.env.INVESTOS_API_KEY;
  const providedKey = event.headers && (
    event.headers['x-investos-key'] ||
    event.headers['X-Investos-Key']
  );

  if (!expectedKey) {
    // INVESTOS_API_KEY not set in Netlify env — block all requests until configured
    return {
      statusCode: 503,
      headers: cors,
      body: JSON.stringify({ error: 'Service not configured' }),
    };
  }

  if (!providedKey || providedKey !== expectedKey) {
    return {
      statusCode: 401,
      headers: cors,
      body: JSON.stringify({ error: 'Unauthorised' }),
    };
  }

  // ── Rate limit check ─────────────────────────────────────────────────────
  if (!checkRateLimit()) {
    return {
      statusCode: 429,
      headers: cors,
      body: JSON.stringify({ error: 'Rate limit exceeded — try again in a few minutes' }),
    };
  }

  // ── Input sanitization ────────────────────────────────────────────────────
  const raw    = ((event.queryStringParameters || {}).s || '');
  const ticker = raw.toUpperCase().replace(/[^A-Z0-9.\-]/g, '').slice(0, 20);
  if (!ticker) {
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Missing ?s=TICKER' }) };
  }

  const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

  try {
    // Step 1 — get crumb + session cookie from Yahoo
    const crumbRes = await httpsGet(
      'https://query2.finance.yahoo.com/v1/test/getcrumb',
      { 'User-Agent': UA, 'Accept': '*/*', 'Accept-Language': 'en-US,en;q=0.9' }
    );
    const crumb     = (crumbRes.data || '').trim();
    const cookieStr = crumbRes.cookies.map(c => c.split(';')[0]).join('; ');

    if (!crumb || crumb.length < 3) throw new Error('Yahoo crumb unavailable — try again');

    // Step 2 — fetch chart data with crumb
    const chartUrl = 'https://query1.finance.yahoo.com/v8/finance/chart/' +
      encodeURIComponent(ticker) +
      '?interval=1d&range=1y&events=div%2Csplit&crumb=' +
      encodeURIComponent(crumb);

    const chartRes = await httpsGet(chartUrl, {
      'User-Agent':      UA,
      'Accept':          'application/json',
      'Accept-Language': 'en-US,en;q=0.9',
      'Cookie':          cookieStr,
    });

    if (chartRes.status === 404) {
      return { statusCode: 404, headers: cors, body: JSON.stringify({ error: 'Ticker not found: ' + ticker }) };
    }
    if (chartRes.status !== 200) {
      throw new Error('Yahoo HTTP ' + chartRes.status);
    }

    const json   = JSON.parse(chartRes.data);
    const result = json && json.chart && json.chart.result && json.chart.result[0];
    if (!result) throw new Error('No chart data returned');

    return {
      statusCode: 200,
      headers: { ...cors, 'Cache-Control': 'public, max-age=300' },
      body: chartRes.data,
    };

  } catch (err) {
    // Safe error — no stack trace exposed
    return {
      statusCode: 502,
      headers: cors,
      body: JSON.stringify({ error: err.message || 'Proxy error' }),
    };
  }
};
