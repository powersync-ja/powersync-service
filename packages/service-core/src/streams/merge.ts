import { throwIfAborted } from 'ix/aborterror.js';
import { AsyncIterableX } from 'ix/asynciterable/index.js';
import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';
import { safeRace } from '../sync/safeRace.js';

/**
 * Merge multiple source AsyncIterables into one output AsyncIterable.
 *
 * The results from all iterables are interleaved.
 *
 * When any iterable is done, the output stops.
 *
 * @param source The source AsyncIterables.
 * @param signal Optional signal to abort iteration.
 * @returns The merged AsyncIterable.
 */
export function mergeAsyncIterables<T>(source: AsyncIterable<T>[], signal?: AbortSignal): AsyncIterable<T> {
  return mergeAsyncIterablesNew(source, signal);
  // return mergeAsyncIterablesOld(source, signal);
}

export function mergeAsyncIterablesNew<T>(source: AsyncIterable<T>[], signal?: AbortSignal): AsyncIterable<T> {
  return new MergedAsyncIterable(source, signal);
}

export function mergeAsyncIterablesOld<T>(source: AsyncIterable<T>[], signal?: AbortSignal): AsyncIterable<T> {
  return wrapWithAbort(new FixedMergeAsyncIterable(source, { race: true }), signal);
}

const NEVER_PROMISE = new Promise<never>(() => {});

type MergeResult<T> = { value: T; index: number };

function wrapPromiseWithIndex<T>(promise: Promise<T>, index: number) {
  return promise.then((value) => ({ value, index })) as Promise<MergeResult<T>>;
}

/**
 * Re-implementation of FixedMergeAsyncIterable, without using async generators.
 *
 * The functionality should be the same in most cases.
 */
class MergedAsyncIterable<T> implements AsyncIterable<T> {
  private _source: AsyncIterable<T>[];
  private _signal?: AbortSignal;

  constructor(source: AsyncIterable<T>[], signal?: AbortSignal) {
    this._source = source;
    this._signal = signal;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    const signal = this._signal;
    throwIfAborted(signal);

    const length = this._source.length;
    const iterators = new Array<AsyncIterator<T>>(length);
    const nexts = new Array<Promise<MergeResult<IteratorResult<T>>> | null>(length);

    for (let i = 0; i < length; i++) {
      const iterator = wrapWithAbort(this._source[i], signal)[Symbol.asyncIterator]();
      iterators[i] = iterator;
      nexts[i] = null;
    }

    const returnIterators = async (_value?: any): Promise<IteratorResult<T>> => {
      for (let iter of iterators) {
        // Do not wait for return() to complete, since it may block
        iter.return?.();
      }
      return { value: undefined, done: true };
    };

    return {
      next: async (): Promise<IteratorResult<T>> => {
        for (let i = 0; i < length; i++) {
          if (nexts[i] == null) {
            nexts[i] = wrapPromiseWithIndex(iterators[i].next(), i);
          }
        }

        try {
          // IMPORTANT! safeRace here acts like Promise.race, but avoids major memory leaks.
          const {
            value: { done, value },
            index
          } = (await safeRace(nexts as Array<Promise<MergeResult<IteratorResult<T>>>>))!;
          if (done) {
            // One of the source iterators is done - return them all
            await returnIterators();

            return {
              value: undefined,
              done: true
            };
          } else {
            // Consume the result, which will cause the next one to be requested on the
            // next iteration.
            nexts[index] = null;

            return {
              value: value,
              done: false
            };
          }
        } catch (e) {
          // One of the source iterators raised an error - return all others
          // and propagate the error
          await returnIterators();
          throw e;
        }
      },
      return: () => {
        return returnIterators();
      },
      throw: async (e) => {
        for (let iter of iterators) {
          // Do not wait for throw() to complete, since it may block
          iter.throw?.(e);
        }
        throw e;
      }
    };
  }
}

/**
 * Loosely based on IxJS merge, but with some changes:
 * 1. Fix an issue with uncaught errors.
 *      Partially fixed: https://github.com/ReactiveX/IxJS/pull/354
 *      Essentially, we only want to call next() on inner iterators when next() is called on the outer one.
 * 2. Call return() on all inner iterators when the outer one returns.
 * 3. Returning when the first iterator returns.
 *
 * https://github.com/ReactiveX/IxJS/blob/f07b7ef4095120f1ef21a4023030c75b36335cd1/src/asynciterable/merge.ts
 */
export class FixedMergeAsyncIterable<T> extends AsyncIterableX<T> {
  private _source: AsyncIterable<T>[];
  private _race: boolean;

  /**
   *
   * @param source
   * @param options Specify `race: true` to stop the iterator when any inner one returns, instead of waiting for all.
   */
  constructor(source: AsyncIterable<T>[], options?: { race?: boolean }) {
    super();
    this._source = source;
    this._race = options?.race ?? false;
  }

  async *[Symbol.asyncIterator](signal?: AbortSignal): AsyncIterator<T> {
    throwIfAborted(signal);
    const length = this._source.length;
    const iterators = new Array<AsyncIterator<T>>(length);
    const nexts = new Array<Promise<MergeResult<IteratorResult<T>>>>(length);
    let active = length;
    for (let i = 0; i < length; i++) {
      const iterator = wrapWithAbort(this._source[i], signal)[Symbol.asyncIterator]();
      iterators[i] = iterator;
      nexts[i] = wrapPromiseWithIndex(iterator.next(), i);
    }

    try {
      while (active > 0) {
        // IMPORTANT! safeRace here acts like Promise.race, but avoids major memory leaks.
        const {
          value: { done, value },
          index
        } = await safeRace(nexts);
        if (done) {
          nexts[index] = NEVER_PROMISE;
          active--;
          if (this._race) {
            break;
          }
        } else {
          const iterator$ = iterators[index];
          nexts[index] = NEVER_PROMISE;
          try {
            yield value;
          } catch (e) {
            // iter.throw() was called on the merged iterator
            // Propagate the error to the source iterators
            for (let iter of iterators) {
              iter.throw?.(e);
            }
            throw e;
          }
          nexts[index] = wrapPromiseWithIndex(iterator$.next(), index);
        }
      }
    } finally {
      for (let iter of iterators) {
        // This may be an early return - return all inner iterators
        iter.return?.();
      }
    }
  }
}
