import { BroadcastIterable, IterableSource } from '@/streams/BroadcastIterable.js';
import { AsyncIterableX, interval } from 'ix/asynciterable/index.js';
import { delayEach } from 'ix/asynciterable/operators/delayeach.js';
import { take } from 'ix/asynciterable/operators/take.js';
import { wrapWithAbort } from 'ix/asynciterable/operators/withabort.js';
import { toArray } from 'ix/asynciterable/toarray.js';
import * as timers from 'timers/promises';
import { describe, expect, it } from 'vitest';

describe('BroadcastIterable', () => {
  it('should iterate', async () => {
    const range = AsyncIterableX.from([1, 2, 3]);
    const broadcast = new BroadcastIterable(() => range);

    const results = await toArray(broadcast);
    expect(results).toEqual([1, 2, 3]);
    expect(broadcast.active).toBe(false);
  });

  it('should skip values if sink is slow', async () => {
    const range = AsyncIterableX.from([1, 2, 3]);
    const broadcast = new BroadcastIterable(() => range);

    // Delay reading each element
    const delayed = delayEach(1)(broadcast);
    const results = await toArray(delayed);

    // We only get a single value, then done.
    expect(results).toEqual([1]);
    expect(broadcast.active).toBe(false);
  });

  it('should abort', async () => {
    const range = AsyncIterableX.from([1, 2, 3]);
    let recordedSignal: AbortSignal | undefined;
    const broadcast = new BroadcastIterable((signal) => {
      recordedSignal = signal;
      return range;
    });

    const controller = new AbortController();
    const iter = broadcast[Symbol.asyncIterator](controller.signal);
    await iter.next();
    controller.abort();
    await expect(iter.next()).rejects.toThrow('The operation has been aborted');
    expect(recordedSignal!.aborted).toEqual(true);
  });

  it('should handle indefinite sources', async () => {
    const source: IterableSource<number> = (signal) => {
      return wrapWithAbort(interval(1), signal);
    };

    const broadcast = new BroadcastIterable(source);

    const delayed = delayEach(10)(broadcast);
    expect(broadcast.active).toBe(false);
    const results = await toArray(take(5)(delayed));

    expect(results.length).toEqual(5);
    expect(results[0]).toEqual(0);
    expect(results[4]).toBeGreaterThan(10);
    expect(results[4]).toBeLessThan(40);

    expect(broadcast.active).toBe(false);
  });

  it('should handle multiple subscribers', async () => {
    let sourceIndex = 0;
    const source = async function* (signal: AbortSignal) {
      // Test value out by 1000 means it may have used the wrong iteration of the source
      const base = (sourceIndex += 1000);
      const abortedPromise = new Promise((resolve) => {
        signal.addEventListener('abort', resolve, { once: true });
      });
      for (let i = 0; !signal.aborted; i++) {
        yield base + i;
        await Promise.race([abortedPromise, timers.setTimeout(1)]);
      }
      // Test value out by 100 means this wasn't reached
      sourceIndex += 100;
    };

    const broadcast = new BroadcastIterable(source);

    const delayed1 = delayEach(9)(broadcast);
    const delayed2 = delayEach(10)(broadcast);
    expect(broadcast.active).toBe(false);
    const results1Promise = toArray(take(5)(delayed1));
    const results2Promise = toArray(take(5)(delayed2));
    const [results1, results2] = [await results1Promise, await results2Promise];

    expect(broadcast.active).toBe(false);

    expect(results1.length).toEqual(5);
    expect(results1[0]).toEqual(1000);
    expect(results1[4]).toBeGreaterThan(1010);
    expect(results1[4]).toBeLessThan(1045);

    expect(results2.length).toEqual(5);
    expect(results2[0]).toEqual(1000);
    expect(results2[4]).toBeGreaterThan(1010);
    expect(results2[4]).toBeLessThan(1045);

    // This starts a new source
    const delayed3 = delayEach(10)(broadcast);
    const results3 = await toArray(take(5)(delayed3));
    expect(results3.length).toEqual(5);
    expect(results3[0]).toEqual(2100);
    expect(results3[4]).toBeGreaterThan(2110);
    expect(results3[4]).toBeLessThan(2145);
  });

  it('should handle errors on multiple subscribers', async () => {
    let sourceIndex = 0;
    const source = async function* (signal: AbortSignal) {
      // Test value out by 1000 means it may have used the wrong iteration of the source
      const base = (sourceIndex += 1000);
      const abortedPromise = new Promise((resolve) => {
        signal.addEventListener('abort', resolve, { once: true });
      });
      for (let i = 0; !signal.aborted; i++) {
        if (base + i == 1005) {
          throw new Error('simulated failure');
        }
        yield base + i;
        await Promise.race([abortedPromise, timers.setTimeout(1)]);
      }
      // Test value out by 100 means this wasn't reached
      sourceIndex += 100;
    };

    const broadcast = new BroadcastIterable(source);

    const delayed1 = delayEach(9)(broadcast);
    const delayed2 = delayEach(10)(broadcast);
    expect(broadcast.active).toBe(false);
    const results1Promise = toArray(take(5)(delayed1)) as Promise<number[]>;
    const results2Promise = toArray(take(5)(delayed2)) as Promise<number[]>;

    const [r1, r2] = await Promise.allSettled([results1Promise, results2Promise]);

    expect(r1).toEqual({ status: 'rejected', reason: new Error('simulated failure') });
    expect(r2).toEqual({ status: 'rejected', reason: new Error('simulated failure') });

    expect(broadcast.active).toBe(false);

    // This starts a new source
    const delayed3 = delayEach(10)(broadcast);
    const results3 = await toArray(take(5)(delayed3));
    expect(results3.length).toEqual(5);
    expect(results3[0]).toEqual(2000);
    expect(results3[4]).toBeGreaterThan(2010);
    expect(results3[4]).toBeLessThan(2045);
  });
});
