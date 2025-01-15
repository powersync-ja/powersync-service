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
