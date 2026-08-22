// netlify/functions/_rateLimiter.js
//
// Durable, per-identity rate limiting for ticker.js, backed by Netlify Blobs.
//
// Replaces the original in-memory `_rateCount` counter (2026-06-xx), which
// reset on every cold start and wasn't shared across concurrent Lambda
// instances -- meaning the advertised "30 calls / 10 min" limit was never a
// real distributed limit, just a per-instance approximation. Blobs persists
// across invocations and instances, so the count is real.
//
// Separate identities (2026-08-21, added alongside VETT's own API key) so
// VETT's traffic and the dashboard's traffic sit in independent buckets --
// a burst from VETT (search-as-you-type / hover) can't 429 the dashboard,
// and vice versa.
//
// This is a soft abuse limiter, not a security boundary: get-then-set
// against Blobs isn't atomic, so a handful of requests can race past the
// limit under heavy concurrency. That's an acceptable tradeoff -- the goal
// is stopping quota drain, not enforcing an exact ceiling.

const RATE_WINDOW_MS = 10 * 60 * 1000; // 10 minutes

const RATE_LIMITS = {
  dashboard: 30, // unchanged from the original single-bucket limit
  vett:      60, // higher: hover/type usage, but VETT debounces + caches client-side
};

function windowKeyFor(identity, now) {
  const windowStart = Math.floor(now / RATE_WINDOW_MS) * RATE_WINDOW_MS;
  return `${identity}:${windowStart}`;
}

// store: a Netlify Blobs store (or any object exposing async get(key,{type})
// and async set(key, value) -- injected so this is testable without real
// Blobs). identity: 'dashboard' | 'vett'. now: Date.now() (ms).
// Returns true if the request is allowed (and records it), false if over
// the limit for this identity's current window.
async function checkRateLimit(store, identity, now) {
  const limit = RATE_LIMITS[identity] ?? RATE_LIMITS.dashboard;
  const key = windowKeyFor(identity, now);

  const current = (await store.get(key, { type: 'json' })) || 0;
  if (current >= limit) return false;

  await store.set(key, JSON.stringify(current + 1));
  return true;
}

module.exports = { checkRateLimit, windowKeyFor, RATE_LIMITS, RATE_WINDOW_MS };
