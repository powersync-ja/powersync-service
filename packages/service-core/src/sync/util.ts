import * as timers from 'timers/promises';

import { SemaphoreInterface } from 'async-mutex';
import { serialize } from 'bson';
import * as util from '../util/util-index.js';
import { RequestTracker } from './RequestTracker.js';
import { safeRace } from './safeRace.js';

export type TokenStreamOptions = {
  /**
   * Adds periodic keepalive events
   */
  keep_alive: boolean;
  /**
   * Warn before the token is going to expire
   */
  expire_warning_period: number;
};

const KEEPALIVE_INTERVAL = 20_000;

const DEFAULT_TOKEN_STREAM_OPTIONS: TokenStreamOptions = {
  keep_alive: true,
  expire_warning_period: 20_000
};

/**
 * Recognize DOM AbortErrors and Node's timer/stream abort errors by name.
 */
export function isAbortError(error: unknown): boolean {
  return typeof error == 'object' && error != null && 'name' in error && error.name === 'AbortError';
}

/**
 * An iterator that periodically yields token and optionally keepalive events, and returns once the
 * provided token expiry is reached.
 *
 * @param token exp is expiry as a unix timestamp (seconds)
 * @param signal abort the iterator with this
 * @param options configure keepalive and expire warnings
 */
export async function* tokenStream(
  token: { exp: number },
  signal: AbortSignal,
  options?: Partial<TokenStreamOptions>
): AsyncGenerator<util.StreamingSyncKeepalive> {
  const resolved_options: TokenStreamOptions = {
    ...DEFAULT_TOKEN_STREAM_OPTIONS,
    ...(options ?? {})
  };

  const { keep_alive, expire_warning_period } = resolved_options;

  // Real tokens always have an integer for exp.
  // In tests, we may use fractional seconds to reduce the delay.
  // Both cases are handled here.
  const expires_at = token.exp * 1000;
  const expire_warning_at = expires_at - expire_warning_period;

  let first_expire_test = true;

  while (!signal.aborted) {
    const token_expires_in = Math.max(0, Math.ceil(token.exp - Date.now() / 1000));
    if (first_expire_test && token_expires_in > 0) {
      first_expire_test = false;
    } else {
      yield { token_expires_in: token_expires_in };
      if (token_expires_in == 0) {
        return;
      }
    }

    const keep_alive_delay = KEEPALIVE_INTERVAL * (0.9 + Math.random() * 0.2);

    // Add margin due to setTimeout inaccuracies
    const expiry_delay = Math.max(0, expires_at - Date.now() + 3);
    const expiry_warning_delay = Math.max(0, expire_warning_at - Date.now() + 3);
    // Either the warning has past or it's before
    const relevant_expiry_delay = expiry_warning_delay != 0 ? expiry_warning_delay : expiry_delay;

    const delay = keep_alive ? Math.min(relevant_expiry_delay, keep_alive_delay) : relevant_expiry_delay;
    await timers.setTimeout(delay, null, { signal }).catch(() => {
      // Ignore AbortError
    });
  }
}

export function syncLineToBson(line: string | Record<string, any>): Buffer {
  if (typeof line == 'string') {
    // Should not happen with binary_data: true
    throw new Error(`Unexpected string data: ${line}`);
  } else {
    // On NodeJS, serialize always returns a Buffer
    return serialize(line) as Buffer;
  }
}

export async function* bsonLines(iterator: AsyncIterable<string | null | Record<string, any>>): AsyncGenerator<Buffer> {
  for await (let line of iterator) {
    if (line == null) {
      // Empty value just to flush iterator memory
      continue;
    } else {
      yield syncLineToBson(line);
    }
  }
}

export async function* ndjson(iterator: AsyncIterable<string | null | Record<string, any>>): AsyncGenerator<string> {
  for await (let data of iterator) {
    if (data == null) {
      // Empty value to flush iterator memory
      continue;
    } else if (typeof data == 'string') {
      // Pre-serialized value
      yield data + '\n';
    } else {
      yield JSON.stringify(data) + '\n';
    }
  }
}

export async function* transformToBytesTracked(
  iterator: AsyncIterable<string | Buffer>,
  tracker: RequestTracker
): AsyncGenerator<Buffer> {
  for await (let data of iterator) {
    let encoded: Buffer;

    if (typeof data == 'string') {
      encoded = Buffer.from(data, 'utf8');
    } else {
      encoded = data;
    }

    tracker.addPlaintextDataSynced(encoded.length);
    yield encoded;
  }
}

/**
 * Coalesce consecutive chunks that are already ready within the same event loop turn
 * into a single Buffer, up to {@link maxBatchBytes}. This avoids emitting back-to-back
 * small frames (such as `checkpoint` followed immediately by `checkpoint_complete`).
 */
export async function* coalesceChunks(
  iterator: AsyncIterable<Buffer>,
  maxBatchBytes = 16 * 1024
): AsyncGenerator<Buffer> {
  const source = iterator[Symbol.asyncIterator]();
  let pendingNext: Promise<PromiseSettledResult<IteratorResult<Buffer>>> | undefined;

  try {
    while (true) {
      const nextPromise = pendingNext ?? settledPromise(source.next());
      pendingNext = undefined;
      const first = await nextPromise;
      if (first.status === 'rejected') {
        throw first.reason;
      }
      if (first.value.done) {
        return;
      }

      const chunks: Buffer[] = [first.value.value];
      let totalBytes = first.value.value.length;
      let doneAfterFlush = false;
      let errorAfterFlush: { error: unknown } | undefined;

      while (totalBytes < maxBatchBytes) {
        const next = settledPromise(source.next());
        let immediate: NodeJS.Immediate;
        const tick = new Promise<'tick'>((resolve) => {
          immediate = setImmediate(() => resolve('tick'));
        });

        const winner = await safeRace<
          { kind: 'next'; result: PromiseSettledResult<IteratorResult<Buffer>> } | { kind: 'tick' }
        >([next.then((result) => ({ kind: 'next' as const, result })), tick.then(() => ({ kind: 'tick' as const }))]);

        if (winner.kind === 'tick') {
          pendingNext = next;
          break;
        }

        clearImmediate(immediate!);
        const { result } = winner;
        if (result.status === 'rejected') {
          errorAfterFlush = { error: result.reason };
          break;
        }
        if (result.value.done) {
          doneAfterFlush = true;
          break;
        }
        chunks.push(result.value.value);
        totalBytes += result.value.value.length;
      }

      yield chunks.length === 1 ? chunks[0] : Buffer.concat(chunks);

      if (errorAfterFlush !== undefined) {
        throw errorAfterFlush.error;
      }
      if (doneAfterFlush) {
        return;
      }
    }
  } finally {
    await Promise.all([settledPromise(Promise.resolve().then(() => source.return?.())), pendingNext]);
  }
}

export function acquireSemaphoreAbortable(
  semaphone: SemaphoreInterface,
  abort?: AbortSignal
): Promise<[number, SemaphoreInterface.Releaser] | 'aborted'> {
  if (abort == null) {
    return semaphone.acquire();
  }
  return new Promise((resolve, reject) => {
    // An already-aborted signal never fires its listener, and we would wait for the semaphore
    // indefinitely.
    if (abort.aborted) {
      resolve('aborted');
      return;
    }

    let aborted = false;
    let hasSemaphore = false;

    const listener = () => {
      if (!hasSemaphore) {
        aborted = true;
        abort.removeEventListener('abort', listener);
        resolve('aborted');
      }
    };
    abort.addEventListener('abort', listener);

    semaphone.acquire().then(
      (acquired) => {
        hasSemaphore = true;
        if (aborted) {
          // Release semaphore, already aborted
          acquired[1]();
        } else {
          abort.removeEventListener('abort', listener);
          resolve(acquired);
        }
      },
      (error) => {
        abort.removeEventListener('abort', listener);
        reject(error);
      }
    );
  });
}

/**
 * Wrap a promise in the style of Promise.allSettled.
 *
 * This is specifically useful if rejections should not be treated as uncaught rejections
 * if it is not specifically handled.
 */
export function settledPromise<T>(promise: Promise<T>): Promise<PromiseSettledResult<T>> {
  return promise.then(
    (result) => {
      return {
        status: 'fulfilled',
        value: result
      };
    },
    (error) => {
      return {
        status: 'rejected',
        reason: error
      };
    }
  );
}

export function unsettledPromise<T>(settled: Promise<PromiseSettledResult<T>>): Promise<T> {
  return settled.then((result) => {
    if (result.status === 'fulfilled') {
      return Promise.resolve(result.value);
    } else {
      return Promise.reject(result.reason);
    }
  });
}

export type MapOrSet<T> = Map<T, any> | Set<T>;

/**
 * Check if two sets have any element(s) in common.
 */
export function hasIntersection<T>(a: MapOrSet<T>, b: MapOrSet<T>) {
  for (let _ of getIntersection(a, b)) {
    return true;
  }
  return false;
}

/**
 * Return the intersection of two sets or maps.
 */
export function* getIntersection<T>(a: MapOrSet<T>, b: MapOrSet<T>): IterableIterator<T> {
  // Iterate over the smaller set to reduce the number of lookups
  if (a.size < b.size) {
    for (let key of a.keys()) {
      if (b.has(key)) {
        yield key;
      }
    }
    return false;
  } else {
    for (let key of b.keys()) {
      if (a.has(key)) {
        yield key;
      }
    }
  }
}
