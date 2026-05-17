/**
 * Builds the WebSocket URL for an RPC by appending the gRPC method path
 * (e.g. `/ping.PingService/Ping`) to the channel target.
 */
export function methodWebSocketUrl(baseUrl: string, method: string): string {
  const url = new URL(baseUrl);
  const methodPath = method.startsWith('/') ? method : `/${method}`;
  const basePath = url.pathname.replace(/\/$/, '');
  url.pathname = `${basePath}${methodPath}`;
  return url.toString();
}
