const BASE64_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

function utf8Encode(input: string): Uint8Array {
  if (typeof TextEncoder !== 'undefined') return new TextEncoder().encode(input);
  // Very old runtimes without TextEncoder: percent-encode then unescape gives raw UTF-8 bytes.
  const escaped = encodeURIComponent(input).replace(/%([0-9A-F]{2})/g, (_, hex: string) =>
    String.fromCharCode(parseInt(hex, 16)),
  );
  const bytes = new Uint8Array(escaped.length);
  for (let i = 0; i < escaped.length; i++) bytes[i] = escaped.charCodeAt(i);
  return bytes;
}

function bytesToBase64(bytes: Uint8Array): string {
  let out = '';
  for (let i = 0; i < bytes.length; i += 3) {
    const b0 = bytes[i] ?? 0;
    const b1 = bytes[i + 1];
    const b2 = bytes[i + 2];
    const triple = (b0 << 16) | ((b1 ?? 0) << 8) | (b2 ?? 0);
    out += BASE64_ALPHABET.charAt((triple >> 18) & 63);
    out += BASE64_ALPHABET.charAt((triple >> 12) & 63);
    out += b1 === undefined ? '=' : BASE64_ALPHABET.charAt((triple >> 6) & 63);
    out += b2 === undefined ? '=' : BASE64_ALPHABET.charAt(triple & 63);
  }
  return out;
}

/**
 * Base64URL without padding, as required by the `header-…` WebSocket subprotocol
 * of AppSync Events. Implemented by hand so it works identically in browsers,
 * React Native (any Hermes version) and Node without `btoa`/`Buffer`.
 */
export function base64UrlEncode(input: string): string {
  return bytesToBase64(utf8Encode(input)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/** RFC 4122 v4 id; uses crypto.randomUUID when the runtime provides it. */
export function randomId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (c && typeof c.randomUUID === 'function') return c.randomUUID();
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (ch) => {
    const r = (Math.random() * 16) | 0;
    const v = ch === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
