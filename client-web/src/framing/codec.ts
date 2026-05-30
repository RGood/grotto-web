/** Grotto RPC wire format — binary frames over a streaming HTTP body. */

export const GROTTO_MAGIC = 0x47525443; // "GRTC"

export const enum FrameType {
  HEADERS = 1,
  MESSAGE = 2,
  HALF_CLOSE = 3,
  TRAILERS = 4,
  CANCEL = 5,
}

export const HEADER_SIZE = 16;

export interface FrameHeader {
  callId: number;
  type: FrameType;
  flags: number;
}

export function encodeFrame(
  callId: number,
  type: FrameType,
  payload: Uint8Array,
  flags = 0
): Uint8Array {
  const out = new Uint8Array(HEADER_SIZE + payload.byteLength);
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);
  view.setUint32(0, GROTTO_MAGIC, false);
  view.setUint32(4, callId >>> 0, false);
  view.setUint8(8, type);
  view.setUint8(9, flags);
  view.setUint16(10, 0, false);
  view.setUint32(12, payload.byteLength, false);
  out.set(payload, HEADER_SIZE);
  return out;
}

export function decodeFrame(data: Uint8Array): {
  header: FrameHeader;
  payload: Uint8Array;
} {
  if (data.byteLength < HEADER_SIZE) {
    throw new Error('grotto: frame too short');
  }
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const magic = view.getUint32(0, false);
  if (magic !== GROTTO_MAGIC) {
    throw new Error(`grotto: invalid magic 0x${magic.toString(16)}`);
  }
  const callId = view.getUint32(4, false);
  const type = view.getUint8(8) as FrameType;
  const flags = view.getUint8(9);
  const length = view.getUint32(12, false);
  if (HEADER_SIZE + length > data.byteLength) {
    throw new Error('grotto: truncated frame payload');
  }
  return {
    header: { callId, type, flags },
    payload: data.subarray(HEADER_SIZE, HEADER_SIZE + length),
  };
}

export function encodeJsonPayload(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

export function decodeJsonPayload<T>(payload: Uint8Array): T {
  return JSON.parse(new TextDecoder().decode(payload)) as T;
}
