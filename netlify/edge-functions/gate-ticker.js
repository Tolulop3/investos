// netlify/edge-functions/gate-ticker.js
// ─────────────────────────────────────────────────────────────────────────
// Rejects unauthorised /api/ticker requests at Netlify's edge, before the
// request ever reaches netlify/functions/ticker.js -- Netlify bills a
// function invocation the moment exports.handler starts running, even if
// the very first line rejects with 401. Moving the auth check earlier
// *inside* ticker.js can't fix that, since any invocation at all is
// billed; only rejecting before the origin function is invoked avoids it.
// Edge Functions run ahead of the origin (Deno runtime, not the Node.js
// Lambda runtime netlify/functions/ uses), which is what actually gates
// the invocation itself.
//
// Mirrors ticker.js's exact auth logic (same ALLOWED_ORIGIN, same
// key-or-origin fallback) so behaviour for legitimate callers is
// unchanged -- this only removes the billed round-trip for callers that
// were already getting rejected, it doesn't change who's authorised.
//
// VERIFICATION STATUS (2026-08-08): the exported auth logic has been
// exercised directly via Node's native Request/Response through 5 real
// cases (OPTIONS preflight, valid origin, valid key without origin,
// wrong origin+key, no origin/key) -- see
// tests/test_invariants.py::test_gate_ticker_edge_function_auth_matrix.
// A manual post-deploy run additionally confirmed both real-world
// checks: (1) a real dashboard lookup still works (legitimate requests
// pass through untouched), (2) an unauthorised request gets 401 without
// ticker.js's own log lines appearing for it -- proving the edge gate
// itself is what rejects it, before the billed origin function is ever
// invoked.
//
// UPDATE (2026-08-21): added INVESTOS_API_KEY_VETT as a second accepted
// key (VETT gets its own, independently revocable, so leaking or rotating
// one key never touches the other), extending the same matrix -- see the
// same test, now covering both keys. Rate limiting (previously an
// in-memory counter in ticker.js, now a durable Netlify Blobs-backed one
// in _rateLimiter.js) deliberately stays at the origin function, not here
// -- Blobs' behavior from this Deno edge runtime vs. the Node function
// runtime hasn't been verified, so it isn't worth the risk of a silent
// failure in the pre-invocation gate. This means over-limit requests still
// bill one invocation before being rejected; only unauthorised ones are
// free. Revisit if Blobs-from-edge is confirmed to work the same way.
// ─────────────────────────────────────────────────────────────────────────

const ALLOWED_ORIGIN = 'https://investos-proxy.netlify.app';

export default async (request, context) => {
  // CORS preflight must always reach the origin untouched -- ticker.js
  // owns the preflight response (status 204 + CORS headers). Blocking it
  // here would break every browser request, not just unauthorised ones.
  if (request.method === 'OPTIONS') {
    return context.next();
  }

  const origin = request.headers.get('origin');
  const originIsValid = origin === ALLOWED_ORIGIN;

  const expectedDashboardKey = Netlify.env.get('INVESTOS_API_KEY');
  const expectedVettKey      = Netlify.env.get('INVESTOS_API_KEY_VETT');
  const providedKey = request.headers.get('x-investos-key');

  const keyMatch = !!providedKey && (
    (!!expectedDashboardKey && providedKey === expectedDashboardKey) ||
    (!!expectedVettKey && providedKey === expectedVettKey)
  );

  let authorised;
  if (expectedDashboardKey || expectedVettKey) {
    // At least one key configured — require a match OR a valid origin, same as ticker.js.
    authorised = originIsValid || keyMatch;
  } else {
    // No key configured — require a valid origin only, same as ticker.js.
    authorised = originIsValid;
  }

  if (!authorised) {
    return new Response(
      JSON.stringify({ error: 'Unauthorised' }),
      {
        status: 401,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': 'null',
        },
      }
    );
  }

  return context.next();
};

export const config = { path: '/api/ticker' };
