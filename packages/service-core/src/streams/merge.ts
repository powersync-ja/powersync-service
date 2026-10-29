import { safeRace } from '../sync/safeRace.js';
import type { IterableSource } from './BroadcastIterable.js';

type ReadResult<T> = { index: number } & PromiseSettledResult<IteratorResult<T>>;
type Failure = { error: unknown };

/**
 * Merge multiple source AsyncIterables into one output AsyncIterable.
 * The results from all iterables are interleaved. When any iterable is done, the output stops.
 *
 * Sources receive a signal owned by this merge. They must use it to unblock pending reads:
 * async generators queue return() behind next(), so closing an unabortable read can block forever.
 * The signal is aborted before closing all source iterators, and completion waits for their cleanup.
 * Cleanup failures propagate to the caller; multiple failures are combined in an AggregateError.
 *
 * Loosely based on IxJS merge, with these differences:
 * 1. Fix an issue with uncaught errors, partially fixed in https://github.com/ReactiveX/IxJS/pull/354.
 *    Only call next() on inner iterators when next() is called on the outer one, and handle every
 *    pending read immediately, including reads whose results are not consumed before shutdown.
 * 2. Call return() on all inner iterators when the outer one returns.
 * 3. Return when the first iterator returns.
 * Original implementation: https://github.com/ReactiveX/IxJS/blob/f07b7ef4095120f1ef21a4023030c75b36335cd1/src/asynciterable/merge.ts
 *
 * @param sources Factories for source iterables that observe the supplied abort signal.
 * @param signal Optional signal to abort iteration.
 */
export function mergeAsyncIterables<T>(sources: IterableSource<T>[], signal?: AbortSignal): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      return new MergedAsyncIterator(sources, signal);
    }
  };
}

class MergedAsyncIterator<T> implements AsyncIterator<T> {
  private readonly controller = new AbortController();
  private readonly signal: AbortSignal;
  private readonly iterators: AsyncIterator<T>[] = [];
  private readonly pending: (Promise<ReadResult<T>> | null)[] = [];
  private initialized = false;
  private closing: Promise<unknown[]> | undefined;
  private nextInFlight: Promise<void> = Promise.resolve();
  private readonly onAbort = () => {
    // Cleanup can start while the consumer is paused. Its settled errors remain available to next()/return().
    void this.close({ error: this.parentSignal!.reason });
  };

  constructor(
    private readonly sources: IterableSource<T>[],
    private readonly parentSignal?: AbortSignal
  ) {
    this.signal = parentSignal ? AbortSignal.any([parentSignal, this.controller.signal]) : this.controller.signal;
    if (parentSignal && !parentSignal.aborted) {
      parentSignal.addEventListener('abort', this.onAbort, { once: true });
    }
  }

  next(): Promise<IteratorResult<T>> {
    // Serialize next() calls, while allowing return()/throw() to abort an in-flight read immediately.
    const next = this.nextInFlight.then(() => this.read());
    this.nextInFlight = next.then(
      () => {},
      () => {}
    );
    return next;
  }

  async return(): Promise<IteratorResult<T>> {
    throwFailures(await this.close());
    return { done: true, value: undefined };
  }

  async throw(error: unknown): Promise<IteratorResult<T>> {
    throwFailures(await this.close({ error }, true));
    return { done: true, value: undefined };
  }

  private async read(): Promise<IteratorResult<T>> {
    if (this.closing) {
      throwFailures(await this.closing);
      return { done: true, value: undefined };
    }
    try {
      this.signal.throwIfAborted();
      if (!this.initialized) {
        this.initialized = true;
        for (const source of this.sources) {
          this.iterators.push(source(this.signal)[Symbol.asyncIterator]());
          this.pending.push(null);
        }
      }
      for (const [index, iterator] of this.iterators.entries()) {
        if (this.pending[index] == null) {
          // Settle errors immediately, even if another source wins and the consumer stops reading.
          this.pending[index] = Promise.resolve()
            .then(() => iterator.next())
            .then(
              (value): ReadResult<T> => ({ index, status: 'fulfilled', value }),
              (reason): ReadResult<T> => ({ index, status: 'rejected', reason })
            );
        }
      }
      if (this.pending.length === 0) {
        return await this.return();
      }
      // Like Promise.race, but avoids retaining callbacks on long-lived pending reads.
      const result = await safeRace(this.pending as Promise<ReadResult<T>>[]);
      if (this.closing) {
        throwFailures(await this.closing);
        return { done: true, value: undefined };
      }
      if (result.status === 'rejected') {
        throw result.reason;
      }
      if (result.value.done) {
        return await this.return();
      }
      this.pending[result.index] = null;
      return result.value;
    } catch (error) {
      throwFailures(await this.close({ error }));
      throw error;
    }
  }

  private close(failure?: Failure, injectError = false): Promise<unknown[]> {
    if (this.closing) {
      return this.closing;
    }
    // Publish the closing state before invoking source code, including synchronous abort listeners.
    this.closing = Promise.resolve().then(async () => {
      this.parentSignal?.removeEventListener('abort', this.onAbort);
      this.controller.abort(failure?.error);
      const errors = failure ? [failure.error] : [];
      const collect = (error: unknown) => {
        // Aborting pending reads is expected. Do not hide the original failure or unrelated cleanup errors.
        if (error !== this.signal.reason && !(error instanceof Error && error.name === 'AbortError')) {
          if (!errors.includes(error)) {
            errors.push(error);
          }
        }
      };
      const results = await Promise.allSettled(
        this.iterators.map(async (iterator) => {
          if (injectError && iterator.throw) {
            try {
              await iterator.throw(failure!.error);
            } catch (error) {
              collect(error);
            }
          }
          // Even an iterator that catches throw() and yields again must be closed.
          await iterator.return?.();
        })
      );
      for (const result of results) {
        if (result.status === 'rejected') {
          collect(result.reason);
        }
      }
      // A source can fail while another source is finishing. Observe those pending reads as well.
      for (const result of await Promise.all(this.pending)) {
        if (result?.status === 'rejected') {
          collect(result.reason);
        }
      }
      return errors;
    });
    return this.closing;
  }
}

function throwFailures(errors: unknown[]): void {
  if (errors.length === 1) {
    throw errors[0];
  }
  if (errors.length > 1) {
    throw new AggregateError(errors, 'Multiple errors while closing merged iterators', { cause: errors[0] });
  }
}
