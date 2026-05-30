import { decodeFrame, HEADER_SIZE, type FrameHeader } from './codec.js';

export type DecodedFrame = {
  header: FrameHeader;
  payload: Uint8Array;
};

/** Incrementally decodes concatenated GRTC frames from a byte stream. */
export class FrameStreamReader {
  private buffer = new Uint8Array(0);

  push(chunk: Uint8Array): DecodedFrame[] {
    if (chunk.byteLength === 0) {
      return [];
    }
    const merged = new Uint8Array(this.buffer.byteLength + chunk.byteLength);
    merged.set(this.buffer);
    merged.set(chunk, this.buffer.byteLength);
    this.buffer = merged;

    const frames: DecodedFrame[] = [];
    while (true) {
      const frame = this.tryShiftFrame();
      if (!frame) {
        break;
      }
      frames.push(frame);
    }
    return frames;
  }

  private tryShiftFrame(): DecodedFrame | null {
    if (this.buffer.byteLength < HEADER_SIZE) {
      return null;
    }
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset, this.buffer.byteLength);
    const length = view.getUint32(12, false);
    const total = HEADER_SIZE + length;
    if (this.buffer.byteLength < total) {
      return null;
    }
    const slice = this.buffer.subarray(0, total);
    this.buffer = this.buffer.subarray(total);
    const { header, payload } = decodeFrame(slice);
    return { header, payload };
  }
}
