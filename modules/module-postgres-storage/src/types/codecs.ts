import * as t from 'ts-codec';

export const BIGINT_MAX = BigInt('9223372036854775807');

/**
 * Wraps a codec which is encoded to a JSON string
 */
export const jsonb = <Decoded>(subCodec: t.Codec<Decoded, any>) =>
  t.codec<Decoded, string>(
    'jsonb',
    (decoded: Decoded) => {
      return JSON.stringify(subCodec.encode(decoded) as any);
    },
    (encoded: string | { data: string }) => {
      const s = typeof encoded == 'object' ? encoded.data : encoded;
      return subCodec.decode(JSON.parse(s));
    }
  );

export const bigint = t.codec<bigint, string | number>(
  'bigint',
  (decoded: BigInt) => {
    return decoded.toString();
  },
  (encoded: string | number) => {
    return BigInt(encoded);
  }
);

export const uint8array = t.codec<Uint8Array, Uint8Array>(
  'uint8array',
  (d) => d,
  (e) => e
);
