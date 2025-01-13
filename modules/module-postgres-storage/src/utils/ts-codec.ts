import * as t from 'ts-codec';

/**
 * Returns a new codec with a subset of keys. Equivalent to the TypeScript Pick utility.
 */
export const pick = <T extends t.AnyObjectCodecShape, Keys extends keyof T>(codec: t.ObjectCodec<T>, keys: Keys[]) => {
  // Filter the shape by the specified keys
  const newShape = Object.fromEntries(
    Object.entries(codec.props.shape).filter(([key]) => keys.includes(key as Keys))
  ) as Pick<T, Keys>;

  // Return a new codec with the narrowed shape
  return t.object(newShape) as t.ObjectCodec<Pick<T, Keys>>;
};

/**
 * PGWire returns INTEGER columns as a `bigint`.
 * This does a decode operation to `number`.
 */
export const pgwire_number = t.codec(
  'pg_number',
  (decoded: number) => decoded,
  (encoded: bigint | number) => {
    if (typeof encoded == 'number') {
      return encoded;
    }
    if (typeof encoded !== 'bigint') {
      throw new Error(`Expected either number or bigint for value`);
    }
    if (encoded > BigInt(Number.MAX_SAFE_INTEGER) || encoded < BigInt(Number.MIN_SAFE_INTEGER)) {
      throw new RangeError('BigInt value is out of safe integer range for conversion to Number.');
    }
    return Number(encoded);
  }
);
