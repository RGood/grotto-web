/** gRPC message framing: 1-byte compression flag + 4-byte length + payload. */

export function wrapGrpcMessage(payload: Uint8Array, compressed = false): Uint8Array {
  const out = new Uint8Array(5 + payload.byteLength);
  out[0] = compressed ? 1 : 0;
  new DataView(out.buffer, out.byteOffset, out.byteLength).setUint32(1, payload.byteLength, false);
  out.set(payload, 5);
  return out;
}

export function unwrapGrpcMessage(frame: Uint8Array): Uint8Array {
  if (frame.byteLength < 5) {
    throw new Error('grotto: invalid gRPC message frame');
  }
  const length = new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getUint32(1, false);
  if (5 + length > frame.byteLength) {
    throw new Error('grotto: truncated gRPC message frame');
  }
  return frame.subarray(5, 5 + length);
}
