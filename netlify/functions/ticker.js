// InvestOS Ticker Proxy — Netlify Serverless Function
// Handles Yahoo Finance crumb+cookie auth server-side.
// No CORS issues. Works for all tickers: TSX (.TO), US, ETFs.

const https = require('https');

function httpsGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const opts = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json,text/html,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        ...headers,
      },
    };
    const req = https.get(url, opts, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data, headers: res.headers }));
    });
    req.on('error', reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function getCrumb() {
  // Step 1: hit Yahoo Finance to get a session cookie
  const r1 = await httpsGet('https://finance.yahoo.com/', {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  });
  const cookie = (r1.headers['set-cookie'] || [])
    .map(c => c.split(';')[0])
    .join('; ');

  if (!cookie) throw new Error('No cookie from Yahoo');

  // Step 2: get crumb token using the session cookie
  const r2 = await httpsGet('https://query1.finance.yahoo.com/v1/test/getcrumb', {
    'Cookie': cookie,
  });

  const crumb = r2.body.trim();
  if (!crumb || crumb.includes('<') || crumb.length > 20) {
    throw new Error(`Bad crumb: ${crumb.substring(0, 50)}`);
  }

  return { crumb, cookie };
}

exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json',
  };

  // Handle preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const ticker = (event.queryStringParameters?.s || '').toUpperCase().trim();
  if (!ticker) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: 'Missing ticker param ?s=' }) };
  }

  try {
    const { crumb, cookie } = await getCrumb();

    const chartUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?interval=1d&range=6mo&crumb=${encodeURIComponent(crumb)}`;
    const r3 = await httpsGet(chartUrl, {
      'Cookie': cookie,
      'Accept': 'application/json',
    });

    if (r3.status !== 200) {
      return {
        statusCode: r3.status,
        headers,
        body: JSON.stringify({ error: `Yahoo returned ${r3.status}`, detail: r3.body.substring(0, 200) }),
      };
    }

    const data = JSON.parse(r3.body);
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(data),
    };

  } catch (err) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: err.message }),
    };
  }
};
