import { mergeAsyncIterablesNew, mergeAsyncIterablesOld } from '@/streams/merge.js';
import * as timers from 'timers/promises';
import { describe, expect, test } from 'vitest';

type MergeIteratorFunction = <T>(source: AsyncIterable<T>[]) => AsyncIterable<T>;

describe('merge iterables', () => {
  makeTests((source) => mergeAsyncIterablesNew(source));
});

describe('merge iterables - legacy implementation', () => {
  makeTests((source) => mergeAsyncIterablesOld(source));
});

function makeTests(mergeIterables: MergeIteratorFunction) {
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

    const merged = mergeIterables([a()]);
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

    const merged = mergeIterables([a()]);

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

    const merged = mergeIterables([a()]);

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

    const merged = mergeIterables([a(), b()]);

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

    const merged = mergeIterables([a(), b()]);

    const iter = merged[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ value: 'a', done: false });
    expect(await iter.next()).toEqual({ value: 'b', done: false });
    await iter.return!();

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

    const merged = mergeIterables([a(), b()]);

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

    const merged = mergeIterables([a(), b()]);

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

    const merged = mergeIterables([a()]);
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

  test('do not block return', async () => {
    let aDone = false;
    let bDone = false;
    let b2Reached = false;
    let b2Timedout = false;
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
        // The final iter.next() should not wait for this to complete
        await timers.setTimeout(50);
        b2Timedout = true;
        yield 'b2';
        // This point should not be reached
        b3Reached = true;
      } finally {
        bDone = true;
      }
    };

    const merged = mergeIterables([a(), b()]);

    const iter = merged[Symbol.asyncIterator]();
    expect(await iter.next()).toEqual({ value: 'a', done: false });
    expect(await iter.next()).toEqual({ value: 'b1', done: false });
    expect(await iter.next()).toEqual({ value: undefined, done: true });

    expect(aDone).toEqual(true);
    expect(b2Reached).toEqual(true);
    expect(b2Reached).toEqual(true);
    expect(b2Timedout).toEqual(false);
    expect(bDone).toEqual(false);

    await timers.setTimeout(60);

    expect(b2Timedout).toEqual(true);
    expect(bDone).toEqual(true);
    expect(b3Reached).toEqual(false);
  });
}
