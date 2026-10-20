import { IterableSource } from '@/streams/BroadcastIterable.js';
import { mergeAsyncIterables } from '@/streams/merge.js';
import * as timers from 'timers/promises';
import { describe, expect, test, vi } from 'vitest';

// These finite legacy sources do not wait indefinitely. Cancellation-aware sources below test pending reads.
function mergeSources<T>(sources: AsyncIterable<T>[]): AsyncIterable<T> {
  return mergeAsyncIterables(sources.map((source) => () => source));
}

describe('merge iterables', () => {
  test('iterable should not pre-emptively continue (1)', async () => {
    // Test that the generator stops after the first yield
    let afterYield = false;
    let inFinally = false;
    const a = async function* (): AsyncGenerator<number, void> {
      try {
        yield 1;
        afterYield = true;
        throw new Error('foobar');
      } finally {
        inFinally = true;
      }
    };

    const merged = mergeSources([a()]);
    const iter = merged[Symbol.asyncIterator]();
    const r = await iter.next();
    expect(r).toEqual({ value: 1, done: false });
    await iter.return!();
    expect(afterYield).toEqual(false);
    expect(inFinally).toEqual(true);
  });

  test('iterable should not pre-emptively continue (2)', async () => {
    // Test that the generator stops after the first yield
    // Equivalent to the above test, but using `for await` instead of manually iterating.
    let afterYield = false;
    let inFinally = false;
    const a = async function* (): AsyncGenerator<number, void> {
      try {
        yield 1;
        afterYield = true;
        throw new Error('foobar');
      } finally {
        inFinally = true;
      }
    };

    const merged = mergeSources([a()]);

    for await (let r of merged) {
      expect(r).toEqual(1);
      break;
    }

    expect(afterYield).toEqual(false);
    expect(inFinally).toEqual(true);
  });

  test('iterable should cleanup', async () => {
    // Test that the generator is exhauseted
    let afterYield = false;
    let inFinally = false;
    const a = async function* (): AsyncGenerator<number, void> {
      try {
        yield 1;
        afterYield = true;
      } finally {
        inFinally = true;
      }
    };

    const merged = mergeSources([a()]);

    for await (let r of merged) {
      expect(r).toEqual(1);
    }
    expect(afterYield).toEqual(true);
    expect(inFinally).toEqual(true);
  });

  test('multiple iterables should cleanup', async () => {
    // Test that the generator is exhauseted
    let afterYieldA = false;
    let inFinallyA = false;
    let afterYieldB = false;
    let inFinallyB = false;
    const a = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'a';
        afterYieldA = true;
      } finally {
        inFinallyA = true;
      }
    };
    const b = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'b';
        afterYieldB = true;
      } finally {
        inFinallyB = true;
      }
    };

    const merged = mergeSources([a(), b()]);

    const iter = merged[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ value: 'a', done: false });
    expect(await iter.next()).toEqual({ value: 'b', done: false });
    expect(await iter.next()).toEqual({ value: undefined, done: true });

    expect(afterYieldA).toEqual(true);
    expect(inFinallyA).toEqual(true);
    expect(afterYieldB).toEqual(true);
    expect(inFinallyB).toEqual(true);
  });

  test('merging two (1)', async () => {
    let aDone = false;
    let bDone = false;
    let aReached = false;
    let bReached = false;

    const a = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'a';
        await timers.setTimeout(0);
        // Should reach this
        aReached = true;
        throw new Error('a');
      } finally {
        // Should reach this
        aDone = true;
      }
    };
    const b = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'b';
        // Should not reach this
        bReached = true;
        await timers.setTimeout(1);
        throw new Error('b');
      } finally {
        // Should reach this
        bDone = true;
      }
    };

    const merged = mergeSources([a(), b()]);

    const iter = merged[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ value: 'a', done: false });
    expect(await iter.next()).toEqual({ value: 'b', done: false });
    // A previously pending read fails during cleanup. Blocking cleanup must surface that error.
    await expect(iter.return!()).rejects.toThrow('a');

    await timers.setTimeout(2);

    expect(aDone).toEqual(true);
    expect(bDone).toEqual(true);
    expect(aReached).toEqual(true);
    expect(bReached).toEqual(false);
  });

  test('ending one iterator early', async () => {
    let aDone = false;
    let bDone = false;
    let b2Reached = false;
    let b3Reached = false;

    const a = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'a';
      } finally {
        aDone = true;
      }
    };
    const b = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'b1';
        // This point should be reached, even if the value is not used
        b2Reached = true;
        yield 'b2';
        // This point should not be reached
        b3Reached = true;
        yield 'b3';
      } finally {
        bDone = true;
      }
    };

    const merged = mergeSources([a(), b()]);

    const iter = merged[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ value: 'a', done: false });
    expect(await iter.next()).toEqual({ value: 'b1', done: false });
    expect(await iter.next()).toEqual({ value: undefined, done: true });

    await timers.setTimeout(2);

    expect(aDone).toEqual(true);
    expect(bDone).toEqual(true);
    expect(b2Reached).toEqual(true);
    expect(b3Reached).toEqual(false);
  });

  test('propagating errors from source', async () => {
    // An error in one source iterator should:
    // 1. Propagate to the merged iterator.
    // 2. Return the other source iterator(s).
    let bDone = false;
    let bError = null;
    let b2Reached = false;
    let b3Reached = false;

    const a = async function* (): AsyncGenerator<string, void> {
      yield 'a';
      await timers.setTimeout(0);
      throw new Error('aError');
    };

    const b = async function* (): AsyncGenerator<string, void> {
      try {
        yield 'b1';
        await timers.setTimeout(0);
        // This point should be reached, even if the value is not used
        b2Reached = true;
        yield 'b2';
        await timers.setTimeout(0);
        // This point should not be reached
        b3Reached = true;
        yield 'b3';
      } catch (e) {
        bError = e;
      } finally {
        bDone = true;
      }
    };

    const merged = mergeSources([a(), b()]);

    const iter = merged[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ value: 'a', done: false });
    expect(await iter.next()).toEqual({ value: 'b1', done: false });
    let error = null;
    try {
      await iter.next();
    } catch (e) {
      error = e;
    }
    expect(error).toEqual(new Error('aError'));

    await timers.setTimeout(2);

    expect(bDone).toEqual(true);
    expect(bError).toEqual(null);
    expect(b2Reached).toEqual(true);
    expect(b3Reached).toEqual(false);
  });

  test('propagating errors from result', async () => {
    // We cannot trigger this using a `for await` loop,
    // only by using the iterator directly.
    // Skipped since the old iterator doesn't implement this.
    let aReached = false;
    let aFinally = false;
    let aError = null;

    const a = async function* (): AsyncGenerator<number, void> {
      try {
        yield 1;
        aReached = true;
      } catch (e) {
        aError = e;
      } finally {
        aFinally = true;
      }
    };

    const merged = mergeSources([a()]);
    const iter = merged[Symbol.asyncIterator]();
    const r = await iter.next();
    expect(r).toEqual({ value: 1, done: false });
    let iterError = null;
    try {
      await iter.throw!(new Error('loop error'));
    } catch (e) {
      iterError = e;
    }

    expect(iterError).toEqual(new Error('loop error'));
    expect(aReached).toEqual(false);
    expect(aFinally).toEqual(true);
    expect(aError).toEqual(new Error('loop error'));
  });
});

describe('blocking merge cleanup', () => {
  test('aborts a pending source read when another source completes', async () => {
    let closed = false;
    const waiting: IterableSource<number> = async function* (signal) {
      try {
        await timers.setTimeout(60_000, undefined, { signal });
        yield 2;
      } finally {
        closed = true;
      }
    };
    const iter = mergeAsyncIterables([
      async function* () {
        yield 1;
      },
      waiting
    ])[Symbol.asyncIterator]();

    expect(await iter.next()).toEqual({ done: false, value: 1 });
    expect(await iter.next()).toEqual({ done: true, value: undefined });
    expect(closed).toBe(true);
  });

  test.each(['return', 'throw', 'abort'] as const)('unblocks an in-flight next() on %s', async (method) => {
    const started = Promise.withResolvers<void>();
    const controller = new AbortController();
    const original = new Error('consumer failed');
    let closed = false;
    const source: IterableSource<number> = async function* (signal) {
      try {
        started.resolve();
        await timers.setTimeout(60_000, undefined, { signal });
        yield 1;
      } finally {
        closed = true;
      }
    };
    const iter = mergeAsyncIterables([source], controller.signal)[Symbol.asyncIterator]();
    const next = iter.next();
    const outcome = next.then(
      (value) => value,
      (error) => error
    );
    await started.promise;

    if (method === 'throw') {
      await expect(iter.throw!(original)).rejects.toBe(original);
      expect(await outcome).toBe(original);
    } else if (method === 'abort') {
      controller.abort();
      expect(await outcome).toMatchObject({ name: 'AbortError' });
    } else {
      await expect(iter.return!()).resolves.toEqual({ done: true, value: undefined });
      expect(await outcome).toEqual({ done: true, value: undefined });
    }
    expect(closed).toBe(true);
  });

  test('waits for every cleanup and starts cleanups concurrently', async () => {
    const gates = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
    const started = gates.map(() => Promise.withResolvers<void>());
    const closed: number[] = [];
    const sources = gates.map(
      (gate, index): IterableSource<number> =>
        async function* () {
          try {
            yield index;
          } finally {
            started[index].resolve();
            await gate.promise;
            closed.push(index);
          }
        }
    );
    const iter = mergeAsyncIterables(sources)[Symbol.asyncIterator]();
    await iter.next();
    let finished = false;
    const cleanup = iter.return!().then(() => {
      finished = true;
    });
    await Promise.all(started.map((item) => item.promise));
    expect(finished).toBe(false);
    gates[0].resolve();
    await timers.setImmediate();
    expect(closed).toEqual([0]);
    expect(finished).toBe(false);
    gates[1].resolve();
    await cleanup;
    expect(closed).toEqual([0, 1]);
  });

  test.each(['return', 'throw'] as const)(
    'surfaces cleanup failures on %s without hiding the consumer error',
    async (method) => {
      const original = new Error('consumer failed');
      const cleanupError = new Error('cleanup failed');
      let otherClosed = false;
      const failing: IterableSource<number> = async function* () {
        try {
          yield 1;
        } finally {
          throw cleanupError;
        }
      };
      const other: IterableSource<number> = async function* () {
        try {
          yield 2;
        } finally {
          otherClosed = true;
        }
      };
      const iter = mergeAsyncIterables([failing, other])[Symbol.asyncIterator]();
      await iter.next();

      if (method === 'return') {
        await expect(iter.return!()).rejects.toBe(cleanupError);
      } else {
        await expect(iter.throw!(original)).rejects.toMatchObject({
          errors: [original, cleanupError],
          cause: original
        });
      }
      expect(otherClosed).toBe(true);
    }
  );

  test('preserves the iteration error when another source fails cleanup', async () => {
    const original = new Error('source failed');
    const cleanupError = new Error('cleanup failed');
    const failing: IterableSource<number> = async function* () {
      throw original;
    };
    const other: IterableSource<number> = async function* () {
      try {
        yield 1;
      } finally {
        throw cleanupError;
      }
    };
    const iter = mergeAsyncIterables([failing, other])[Symbol.asyncIterator]();
    await expect(iter.next()).rejects.toMatchObject({ errors: [original, cleanupError], cause: original });
  });

  test('handles a losing read that rejects while the consumer is paused', async () => {
    const error = new Error('background read failed');
    const failure = Promise.withResolvers<void>();
    const source: IterableSource<number> = async function* () {
      yield 1;
    };
    const failing: IterableSource<number> = async function* () {
      await failure.promise;
      throw error;
    };
    const iter = mergeAsyncIterables([source, failing])[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ done: false, value: 1 });
    failure.resolve();
    // No next() consumer is attached during this turn. Vitest detects any unhandled rejection.
    await timers.setImmediate();
    await expect(iter.return!()).rejects.toBe(error);
  });

  test('closes a source even if it catches an injected error and yields again', async () => {
    const original = new Error('consumer failed');
    let closed = false;
    const source: IterableSource<number> = async function* () {
      try {
        try {
          yield 1;
        } catch {
          yield 2;
        }
      } finally {
        closed = true;
      }
    };
    const iter = mergeAsyncIterables([source])[Symbol.asyncIterator]();
    await iter.next();
    await expect(iter.throw!(original)).rejects.toBe(original);
    expect(closed).toBe(true);
  });

  test('cleans up initialized sources if a later factory throws', async () => {
    const error = new Error('factory failed');
    const close = vi.fn(async () => ({ done: true as const, value: undefined }));
    const source: IterableSource<number> = () => ({
      [Symbol.asyncIterator]: () => ({ next: async () => ({ done: false, value: 1 }), return: close })
    });
    const iter = mergeAsyncIterables([
      source,
      () => {
        throw error;
      }
    ])[Symbol.asyncIterator]();
    await expect(iter.next()).rejects.toBe(error);
    expect(close).toHaveBeenCalledOnce();
  });

  test('does not create sources after cancellation or return before the first read', async () => {
    const source = vi.fn(async function* () {
      yield 1;
    });
    const controller = new AbortController();
    controller.abort();
    await expect(mergeAsyncIterables([source], controller.signal)[Symbol.asyncIterator]().next()).rejects.toMatchObject(
      { name: 'AbortError' }
    );
    await mergeAsyncIterables([source])[Symbol.asyncIterator]().return!();
    expect(source).not.toHaveBeenCalled();
  });

  test('completes an empty merge', async () => {
    await expect(mergeAsyncIterables([])[Symbol.asyncIterator]().next()).resolves.toEqual({
      done: true,
      value: undefined
    });
  });

  test('collects multiple cleanup errors, including synchronous failures', async () => {
    const errors = [new Error('first cleanup failed'), new Error('second cleanup failed')];
    const closed: number[] = [];
    const sources = errors.map(
      (error, index): IterableSource<number> =>
        () => ({
          [Symbol.asyncIterator]: () => ({
            next: async () => ({ done: false, value: index }),
            return: () => {
              closed.push(index);
              if (index === 0) throw error;
              return Promise.reject(error);
            }
          })
        })
    );
    const iter = mergeAsyncIterables(sources)[Symbol.asyncIterator]();
    await iter.next();
    await expect(iter.return!()).rejects.toMatchObject({ errors, cause: errors[0] });
    expect(closed).toEqual([0, 1]);
  });

  test('cleans up on external abort while the consumer is paused and retains cleanup failures', async () => {
    const controller = new AbortController();
    const closed = Promise.withResolvers<void>();
    const error = new Error('cleanup failed');
    const source: IterableSource<number> = async function* () {
      try {
        yield 1;
      } finally {
        closed.resolve();
        throw error;
      }
    };
    const iter = mergeAsyncIterables([source], controller.signal)[Symbol.asyncIterator]();
    await iter.next();
    controller.abort();
    await closed.promise;
    // No consumer is awaiting cleanup yet. The stored cleanup result must not reject in the background.
    await timers.setImmediate();
    await expect(iter.return!()).rejects.toMatchObject({
      errors: [expect.objectContaining({ name: 'AbortError' }), error]
    });
  });
});
