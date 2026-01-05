/**
 * Stable value-based hashcodes for JavaScript. The sync streams compiler uses hashmaps derived from this to efficiently
 * de-duplicate equivalent expressions and lookups.
 *
 * This is copied from Dart SDK sources, which in turn based the algorithm on Jenkins hash functions.
 *
 * Because hash codes are stable across restarts, they should not be used for untrusted inputs to avoid attacks
 * provoking hash collisions and subpar hashmap performance. The sync rules package only uses these hashes to compile
 * sync streams, but the compiled IR processing source rows does not need these hashes.
 */
export class StableHasher {
  private static readonly seed: number = 614160925; // 'PowerSync'.hashCode on the Dart VM

  private hash: number = StableHasher.seed;

  addHash(value: number) {
    let hash = 0x1fffffff & (this.hash + value);
    hash = 0x1fffffff & (hash + ((0x0007ffff & hash) << 10));
    this.hash = hash ^ (hash >> 6);
  }

  reset() {
    const old = this.hash;
    this.hash = StableHasher.seed;
    return old;
  }

  addString(value: string) {
    for (let i = 0; i < value.length; i++) {
      this.addHash(value.charCodeAt(i));
    }
  }

  buildHashCode(): number {
    let hash = this.hash;
    hash = 0x1fffffff & (hash + ((0x03ffffff & hash) << 3));
    hash = hash ^ (hash >> 11);
    return 0x1fffffff & (hash + ((0x00003fff & hash) << 15));
  }

  static hashWith<T>(equality: Equality<T>, value: T): number {
    const saved = this.hasher.reset();
    equality.hash(this.hasher, value);

    const result = this.hasher.buildHashCode();
    this.hasher.hash = saved;
    return result;
  }

  private static readonly hasher = new StableHasher();
}

/**
 * A generic equality relation on objects.
 */
export interface Equality<T> {
  /**
   * Checks whether a and b are equal according to this equality implementation.
   *
   * This must hold the usual properties for equality relations (reflexivity, symmetry and transitivety).
   */
  equals(a: T, b: T): boolean;

  /**
   * Adds a value to a stable hasher.
   *
   * The implementation of hashes must be compatible with {@link equals}, meaning that if values `a` and `b` are equal,
   * they must also generate the same hash. Two distinct values may also generate the same hashcode though.
   */
  hash(hasher: StableHasher, value: T): void;
}

/**
 * Creates an equality operator matching iterables with elements compared by an inner equality.
 *
 * Two iterables are considered equal if they contain the same elements in the same order.
 */
export function listEquality<E>(equality: Equality<E>): Equality<Iterable<E>> {
  return {
    equals: (a, b) => {
      if (a === b) return true;

      const iteratorA = a[Symbol.iterator]();
      const iteratorB = b[Symbol.iterator]();

      while (true) {
        let nextA = iteratorA.next();
        let nextB = iteratorB.next();

        if (nextA.done != nextB.done) {
          return false; // Different lengths
        } else if (nextA.done) {
          return true; // Both done
        } else {
          const elementA = nextA.value;
          const elementB = nextB.value;
          if (!equality.equals(elementA, elementB)) {
            return false;
          }
        }
      }
    },
    hash: (hasher, value) => {
      for (const e of value) {
        equality.hash(hasher, e);
      }
    }
  };
}

/**
 * Creates an equality operator matching iterables with elements compared by an inner equality.
 *
 * Two iterables are considered equal if they contain the same elements, in any order.
 */
export function unorderedEquality<E>(equality: Equality<E>): Equality<Iterable<E>> {
  return {
    equals: (a, b) => {
      if (a === b) return true;

      let counts = new HashMap<E, number>(equality);
      let length = 0;
      // Count how often each element exists in a.
      for (const e of a) {
        counts.setOrUpdate(e, (old) => (old ?? 0) + 1);
        length++;
      }

      // Then compare with b.
      for (const e of b) {
        let matched = false;
        counts.setOrUpdate(e, (old) => {
          matched = old != null && old > 0;
          if (matched) {
            return old! - 1;
          } else {
            return 0;
          }
        });

        if (!matched) {
          return false;
        }
        length--;
      }

      return length == 0;
    },
    hash: (hasher, value) => {
      // https://github.com/dart-lang/core/blob/3e55c1ce7be48b469da5be043b5931fe6405f74a/pkgs/collection/lib/src/equality.dart#L236-L248
      let hash = 0;
      for (const element of value) {
        hash = (hash + StableHasher.hashWith(equality, element)) & 0x7fffffff;
      }

      hasher.addHash(hash);
    }
  };
}

/**
 * A hash map implementation based on a custom {@link Equality}.
 *
 * Like {@link Map}, this implementation also preserves insertion-order.
 */
export class HashMap<K, V> {
  // We create one bucket per unique hash, and then store collisions in an array.
  private readonly map = new Map<number, LinkedHashMapEntry<K, V>[]>();

  private first: LinkedHashMapEntry<K, V> | undefined;
  private last: LinkedHashMapEntry<K, V> | undefined;

  constructor(private readonly equality: Equality<K>) {}

  private computeHashCode(key: K): number {
    return StableHasher.hashWith(this.equality, key);
  }

  get(key: K): V | undefined {
    const hashCode = this.computeHashCode(key);
    const bucket = this.map.get(hashCode);
    if (bucket) {
      for (const entry of bucket) {
        if (this.equality.equals(entry.key, key)) {
          return entry.value;
        }
      }
    }
  }

  set(key: K, value: V) {
    const hashCode = this.computeHashCode(key);
    let bucket = this.map.get(hashCode);
    if (!bucket) {
      bucket = [];
      this.map.set(hashCode, bucket);
    }

    this.addEntryInBucket(bucket, key, value);
  }

  setOrUpdate(key: K, updater: (old: V | undefined) => V) {
    const hashCode = this.computeHashCode(key);
    let bucket = this.map.get(hashCode);
    if (!bucket) {
      bucket = [];
      this.map.set(hashCode, bucket);
    } else {
      for (const entry of bucket) {
        if (this.equality.equals(entry.key, key)) {
          entry.value = updater(entry.value);
          return;
        }
      }
    }

    this.addEntryInBucket(bucket, key, updater(undefined));
  }

  private addEntryInBucket(bucket: LinkedHashMapEntry<K, V>[], key: K, value: V) {
    const entry = new LinkedHashMapEntry(key, value);
    bucket.push(entry);
    if (this.first) {
      this.last!.next = entry;
      entry.prev = this.last;
    } else {
      this.first = this.last = entry;
    }
  }
}

class LinkedHashMapEntry<K, V> {
  prev: LinkedHashMapEntry<K, V> | undefined;
  next: LinkedHashMapEntry<K, V> | undefined;

  value: V;

  constructor(
    readonly key: K,
    value: V
  ) {
    this.value = value;
  }
}
