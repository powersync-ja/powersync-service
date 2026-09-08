/** Repeatable generated data with bounded memory. Generators must be deterministic. */
export interface Sequence<T> extends Iterable<T> {
  readonly length: number;
  at(index: number): T | undefined;
}

export function generatedSequence<T>(length: number, generate: (index: number) => T): Sequence<T> {
  return {
    length,
    at(index) {
      const offset = index < 0 ? length + index : index;
      return offset < 0 || offset >= length ? undefined : generate(offset);
    },
    *[Symbol.iterator]() {
      for (let index = 0; index < length; index++) yield generate(index);
    }
  };
}
