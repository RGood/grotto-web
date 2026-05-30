/** MIME type for GRTC-framed request and response bodies. */
export const GROTTO_CONTENT_TYPE = 'application/vnd.grotto+grtc';

/** Response header on the bidi server leg; must be sent on the client leg request. */
export const GROTTO_SESSION_ID_HEADER = 'Grotto-Session-Id';

/** Request/response marker for which half of a bidi RPC an HTTP POST carries. */
export const GROTTO_BIDI_LEG_HEADER = 'Grotto-Bidi-Leg';

export const GROTTO_BIDI_LEG_SERVER = 'server';
export const GROTTO_BIDI_LEG_CLIENT = 'client';

/** Max wait for the client leg after the server leg returns a session id. */
export const BIDI_CLIENT_LEG_TIMEOUT_MS = 10_000;

/**
 * Builds the HTTP URL for an RPC by appending the gRPC method path
 * (e.g. `/ping.PingService/Ping`) to the server target.
 */
export function methodRpcUrl(baseUrl: string, method: string): string {
  const url = new URL(baseUrl);
  const methodPath = method.startsWith('/') ? method : `/${method}`;
  const basePath = url.pathname.replace(/\/$/, '');
  url.pathname = `${basePath}${methodPath}`;
  return url.toString();
}
