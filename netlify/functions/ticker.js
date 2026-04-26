// InvestOS Ticker Proxy — Netlify Serverless Function v2
// Multi-endpoint strategy: v8 chart → v7 quote → v8 query2
// Rotates endpoints and user agents to avoid 429 blocking

const https = require('https');

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
];

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function httpsGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': randomUA(),
        'Accept': 'application/json,text/html,*/*;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        ...headers,
      },
    }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString();
        resolve({ status: res.statusCode, body, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.setTimeout(12000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function tryChartEndpoint(ticker, host) {
  const url = `https://${host}/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&range=6mo&includePrePost=false`;
  const r = await httpsGet(url, { 'Referer': 'https://finance.yahoo.com/' });
  if (r.status === 200) {
    const data = JSON.parse(r.body);
    if (data?.chart?.result?.[0]) return data;
  }
  throw new Error(`${host} returned ${r.status}`);
}

async function tryQuoteEndpoint(ticker) {
  const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(ticker)}&fields=regularMarketPrice,regularMarketPreviousClose,regularMarketVolume,averageDailyVolume3Month,fiftyTwoWeekHigh,fiftyTwoWeekLow,fiftyDayAverage,twoHundredDayAverage,regularMarketChangePercent,marketCap,longName,shortName,currency,exchange,quoteType`;
  const r = await httpsGet(url, { 'Referer': 'https://finance.yahoo.com/quote/' + ticker });
  if (r.status === 200) {
    const data = JSON.parse(r.body);
    if (data?.quoteResponse?.result?.[0]) return { quote: data.quoteResponse.result[0] };
  }
  throw new Error(`v7 quote returned ${r.status}`);
}

async function getCrumbAndCookie() {
  // Small delay to avoid burst detection
  await new Promise(r => setTimeout(r, Math.random() * 800 + 200));
  
  const r1 = await httpsGet('https://finance.yahoo.com/quote/AAPL', {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Upgrade-Insecure-Requests': '1',
  });
  
  const cookie = (r1.headers['set-cookie'] || [])
    .map(c => c.split(';')[0])
    .filter(c => c.length > 0)
    .join('; ');

  if (!cookie) throw new Error('No cookie');

  await new Promise(r => setTimeout(r, 300));

  const r2 = await httpsGet('https://query1.finance.yahoo.com/v1/test/getcrumb', {
    'Cookie': cookie,
    'Referer': 'https://finance.yahoo.com/',
  });

  const crumb = r2.body.trim();
  if (!crumb || crumb.includes('<') || crumb.length > 20) {
    throw new Error(`Bad crumb: ${crumb.substring(0, 30)}`);
  }
  return { crumb, cookie };
}

exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
    'Cache-Control': 'public, max-age=300', // cache 5 min to reduce Yahoo hits
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const ticker = (event.queryStringParameters?.s || '').toUpperCase().trim();
  if (!ticker) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing ?s= param' }) };
  }

  const errors = [];

  // ── Route 1: query1 chart (no crumb) ─────────────────────────
  try {
    const data = await tryChartEndpoint(ticker, 'query1.finance.yahoo.com');
    return { statusCode: 200, headers, body: JSON.stringify(data) };
  } catch(e) { errors.push('q1:' + e.message); }

  // ── Route 2: query2 chart ─────────────────────────────────────
  try {
    const data = await tryChartEndpoint(ticker, 'query2.finance.yahoo.com');
    return { statusCode: 200, headers, body: JSON.stringify(data) };
  } catch(e) { errors.push('q2:' + e.message); }

  // ── Route 3: crumb + cookie auth ─────────────────────────────
  try {
    const { crumb, cookie } = await getCrumbAndCookie();
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&range=6mo&crumb=${encodeURIComponent(crumb)}`;
    const r = await httpsGet(url, { 'Cookie': cookie, 'Referer': 'https://finance.yahoo.com/' });
    if (r.status === 200) {
      return { statusCode: 200, headers, body: r.body };
    }
    errors.push('crumb:' + r.status);
  } catch(e) { errors.push('crumb:' + e.message); }

  // ── Route 4: v7 quote (limited data fallback) ─────────────────
  try {
    const data = await tryQuoteEndpoint(ticker);
    return { statusCode: 200, headers, body: JSON.stringify(data) };
  } catch(e) { errors.push('v7:' + e.message); }

  // All routes failed
  return {
    statusCode: 429,
    headers,
    body: JSON.stringify({ error: 'All Yahoo endpoints rate-limited', detail: errors.join(' | ') }),
  };
};
