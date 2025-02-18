import * as timers from 'timers/promises';

import * as util from '../util/util-index.js';
import { RequestTracker } from './RequestTracker.js';
import { SemaphoreInterface } from 'async-mutex';

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
  iterator: AsyncIterable<string>,
  tracker: RequestTracker
): AsyncGenerator<Buffer> {
  for await (let data of iterator) {
    const encoded = Buffer.from(data, 'utf8');
    tracker.addDataSynced(encoded.length);
    yield encoded;
  }
}

export function acquireSemaphoreAbortable(
  semaphone: SemaphoreInterface,
  abort: AbortSignal
): Promise<[number, SemaphoreInterface.Releaser] | 'aborted'> {
  return new Promise((resolve, reject) => {
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

    semaphone.acquire().then((acquired) => {
      hasSemaphore = true;
      if (aborted) {
        // Release semaphore, already aborted
        acquired[1]();
      } else {
        abort.removeEventListener('abort', listener);
        resolve(acquired);
      }
    }, reject);
  });
}
