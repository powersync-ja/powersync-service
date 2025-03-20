// Vitest Unit Tests
import { Demultiplexer, DemultiplexerSource, DemultiplexerSourceFactory, DemultiplexerValue } from '@/index.js';
import { delayEach } from 'ix/asynciterable/operators/delayeach.js';
import { take } from 'ix/asynciterable/operators/take.js';
import { toArray } from 'ix/asynciterable/toarray.js';
import * as timers from 'node:timers/promises';
import { describe, expect, it } from 'vitest';

describe('Demultiplexer', () => {
  it('should start subscription lazily and provide first value', async () => {
    const mockSource: DemultiplexerSourceFactory<string> = (signal: AbortSignal) => {
      const iterator = (async function* (): AsyncIterable<DemultiplexerValue<string>> {})();
      return {
        iterator,
        getFirstValue: async (key: string) => `first-${key}`
      };
    };

    const demux = new Demultiplexer(mockSource);
    const signal = new AbortController().signal;

    const iter = demux.subscribe('user1', signal)[Symbol.asyncIterator]();
    const result = await iter.next();
    expect(result.value).toBe('first-user1');
  });

  it('should handle multiple subscribers to the same key', async () => {
    const iter = (async function* () {
      yield { key: 'user1', value: 'value1' };
      yield { key: 'user1', value: 'value2' };
    })();
    const source: DemultiplexerSource<string> = {
      iterator: iter,
      getFirstValue: async (key: string) => `first-${key}`
    };

    const demux = new Demultiplexer(() => source);
    const signal = new AbortController().signal;

    const iter1 = demux.subscribe('user1', signal)[Symbol.asyncIterator]();
    const iter2 = demux.subscribe('user1', signal)[Symbol.asyncIterator]();

    // Due to only keeping the last value, some values are skipped
    expect(await iter1.next()).toEqual({ value: 'first-user1', done: false });
    expect(await iter1.next()).toEqual({ value: 'value1', done: false });
    expect(await iter1.next()).toEqual({ value: undefined, done: true });

    expect(await iter2.next()).toEqual({ value: 'first-user1', done: false });
    expect(await iter2.next()).toEqual({ value: undefined, done: true });
  });

  it('should handle multiple subscribers to the same key (2)', async () => {
    const p1 = Promise.withResolvers<void>();
    const p2 = Promise.withResolvers<void>();
    const p3 = Promise.withResolvers<void>();

    const iter = (async function* () {
      await p1.promise;
      yield { key: 'user1', value: 'value1' };
      await p2.promise;
      yield { key: 'user1', value: 'value2' };
      await p3.promise;
    })();

    const source: DemultiplexerSource<string> = {
      iterator: iter,
      getFirstValue: async (key: string) => `first-${key}`
    };

    const demux = new Demultiplexer(() => source);
    const signal = new AbortController().signal;

    const iter1 = demux.subscribe('user1', signal)[Symbol.asyncIterator]();
    const iter2 = demux.subscribe('user1', signal)[Symbol.asyncIterator]();

    // Due to only keeping the last value, some values are skilled
    expect(await iter1.next()).toEqual({ value: 'first-user1', done: false });
    expect(await iter2.next()).toEqual({ value: 'first-user1', done: false });
    p1.resolve();

    expect(await iter1.next()).toEqual({ value: 'value1', done: false });
    expect(await iter2.next()).toEqual({ value: 'value1', done: false });
    p2.resolve();

    expect(await iter1.next()).toEqual({ value: 'value2', done: false });
    p3.resolve();

    expect(await iter1.next()).toEqual({ value: undefined, done: true });
    expect(await iter2.next()).toEqual({ value: undefined, done: true });
  });

  it('should handle multiple subscribers to different keys', async () => {
    const p1 = Promise.withResolvers<void>();
    const p2 = Promise.withResolvers<void>();
    const p3 = Promise.withResolvers<void>();

    const iter = (async function* () {
      await p1.promise;
      yield { key: 'user1', value: 'value1' };
      await p2.promise;
      yield { key: 'user2', value: 'value2' };
      await p3.promise;
    })();

    const source: DemultiplexerSource<string> = {
      iterator: iter,
      getFirstValue: async (key: string) => `first-${key}`
    };

    const demux = new Demultiplexer(() => source);
    const signal = new AbortController().signal;

    const iter1 = demux.subscribe('user1', signal)[Symbol.asyncIterator]();
    const iter2 = demux.subscribe('user2', signal)[Symbol.asyncIterator]();

    // Due to only keeping the last value, some values are skilled
    expect(await iter1.next()).toEqual({ value: 'first-user1', done: false });
    expect(await iter2.next()).toEqual({ value: 'first-user2', done: false });
    p1.resolve();

    expect(await iter1.next()).toEqual({ value: 'value1', done: false });
    p2.resolve();

    expect(await iter2.next()).toEqual({ value: 'value2', done: false });
    p3.resolve();

    expect(await iter1.next()).toEqual({ value: undefined, done: true });
    expect(await iter2.next()).toEqual({ value: undefined, done: true });
  });

  it('should abort', async () => {
    const iter = (async function* () {
      yield { key: 'user1', value: 'value1' };
      yield { key: 'user1', value: 'value2' };
    })();

    const source: DemultiplexerSource<string> = {
      iterator: iter,
      getFirstValue: async (key: string) => `first-${key}`
    };

    const demux = new Demultiplexer(() => source);
    const controller = new AbortController();

    const iter1 = demux.subscribe('user1', controller.signal)[Symbol.asyncIterator]();

    expect(await iter1.next()).toEqual({ value: 'first-user1', done: false });
    controller.abort();

    await expect(iter1.next()).rejects.toThrow('The operation has been aborted');
  });

  it('should handle errors on multiple subscribers', async () => {
    let sourceIndex = 0;
    const sourceFn = async function* (signal: AbortSignal): AsyncIterable<DemultiplexerValue<number>> {
      // Test value out by 1000 means it may have used the wrong iteration of the source
      const base = (sourceIndex += 1000);
      const abortedPromise = new Promise((resolve) => {
        signal.addEventListener('abort', resolve, { once: true });
      });
      for (let i = 0; !signal.aborted; i++) {
        if (base + i == 1005) {
          throw new Error('simulated failure');
        }
        yield { key: 'u1', value: base + i };
        await Promise.race([abortedPromise, timers.setTimeout(1)]);
      }
      // Test value out by 100 means this wasn't reached
      sourceIndex += 100;
    };

    const sourceFactory: DemultiplexerSourceFactory<number> = (signal) => {
      const source: DemultiplexerSource<number> = {
        iterator: sourceFn(signal),
        getFirstValue: async (key: string) => -1
      };
      return source;
    };
    const demux = new Demultiplexer(sourceFactory);

    const controller = new AbortController();

    const delayed1 = delayEach(9)(demux.subscribe('u1', controller.signal));
    const delayed2 = delayEach(10)(demux.subscribe('u1', controller.signal));
    expect(demux.active).toBe(false);
    const results1Promise = toArray(take(5)(delayed1)) as Promise<number[]>;
    const results2Promise = toArray(take(5)(delayed2)) as Promise<number[]>;

    const [r1, r2] = await Promise.allSettled([results1Promise, results2Promise]);

    expect(r1).toEqual({ status: 'rejected', reason: new Error('simulated failure') });
    expect(r2).toEqual({ status: 'rejected', reason: new Error('simulated failure') });

    expect(demux.active).toBe(false);

    // This starts a new source
    const delayed3 = delayEach(10)(demux.subscribe('u1', controller.signal));
    const results3 = await toArray(take(6)(delayed3));
    expect(results3.length).toEqual(6);
    expect(results3[0]).toEqual(-1); // Initial value
    // There should be approximately 10ms between each value, but we allow for some slack
    expect(results3[5]).toBeGreaterThan(2005);
    expect(results3[5]).toBeLessThan(2200);
  });
});
