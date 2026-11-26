import { acquireSemaphoreAbortable, coalesceChunks, isAbortError } from '@/index.js';
import { Semaphore, SemaphoreInterface } from 'async-mutex';
import { setTimeout } from 'node:timers/promises';
import { describe, expect, test, vi } from 'vitest';

describe('isAbortError', () => {
  test('recognizes DOM and Node abort errors', () => {
    const controller = new AbortController();
    controller.abort();

    expect(isAbortError(new DOMException('Cancelled', 'AbortError'))).toBe(true);
    expect(isAbortError(Object.assign(new Error('Cancelled'), { name: 'AbortError' }))).toBe(true);
    expect(isAbortError(controller.signal.reason)).toBe(true);
    expect(isAbortError(new Error('not aborted'))).toBe(false);
  });

  test('recognizes timer cancellation with the original reason as its cause', async () => {
    const controller = new AbortController();
    const reason = new Error('cancelled');
    const timer = setTimeout(60_000, undefined, { signal: controller.signal });
    controller.abort(reason);
    const error = await timer.catch((error) => error);
    expect(isAbortError(error)).toBe(true);
    expect(error.cause).toBe(reason);
  });
});

describe('acquireSemaphoreAbortable', () => {
  test('waits for an available slot without a signal', async () => {
    const semaphore = new Semaphore(1);
    const [, release] = await semaphore.acquire();
    const resolved = vi.fn();
    const waiting = acquireSemaphoreAbortable(semaphore).then((result) => {
      resolved();
      return result;
    });
    await Promise.resolve();
    expect(resolved).not.toHaveBeenCalled();

    release();
    const acquired = await waiting;
    expect(acquired).not.toBe('aborted');
    expect(semaphore.getValue()).toBe(0);
    (acquired as [number, SemaphoreInterface.Releaser])[1]();
    expect(semaphore.getValue()).toBe(1);
  });

  test('can acquire', async () => {
    const semaphore = new Semaphore(1);
    const controller = new AbortController();

    expect(await acquireSemaphoreAbortable(semaphore, controller.signal)).not.toBe('aborted');
  });

  test('can cancel', async () => {
    const semaphore = new Semaphore(1);
    const controller = new AbortController();

    const resolve = vi.fn();
    const reject = vi.fn();

    // First invocation: Lock the semaphore
    const result = await acquireSemaphoreAbortable(semaphore, controller.signal);
    expect(result).not.toBe('aborted');
    const [count, release] = result as [number, SemaphoreInterface.Releaser];

    acquireSemaphoreAbortable(semaphore, controller.signal).then(resolve, reject);
    controller.abort();
    await Promise.resolve();
    expect(reject).not.toHaveBeenCalled();
    expect(resolve).toHaveBeenCalledWith('aborted');

    // Releasing the semaphore should not invoke resolve again
    release();
  });

  test('does not wait when the signal is already aborted', async () => {
    const semaphore = new Semaphore(1);
    const controller = new AbortController();

    // Hold the only slot, so acquiring would block indefinitely.
    const result = await acquireSemaphoreAbortable(semaphore, controller.signal);
    expect(result).not.toBe('aborted');
    const [, release] = result as [number, SemaphoreInterface.Releaser];

    controller.abort();
    expect(await acquireSemaphoreAbortable(semaphore, controller.signal)).toBe('aborted');

    release();
  });
});

describe('coalesceChunks', () => {
  test('coalesces chunks ready in the same event loop turn into a single Buffer', async () => {
    async function* source() {
      yield Buffer.from('{"checkpoint":1}\n');
      await Promise.resolve();
      await Promise.resolve();
      yield Buffer.from('{"checkpoint_complete":1}\n');
      await new Promise((resolve) => globalThis.setTimeout(resolve, 25));
      yield Buffer.from('{"token_expires_in":60}\n');
    }

    const batches: string[] = [];
    for await (const chunk of coalesceChunks(source())) {
      batches.push(chunk.toString('utf8'));
    }

    expect(batches).toEqual(['{"checkpoint":1}\n{"checkpoint_complete":1}\n', '{"token_expires_in":60}\n']);
  });

  test('flushes when maxBatchBytes is reached without waiting for next tick', async () => {
    async function* source() {
      yield Buffer.from('aaaa');
      yield Buffer.from('bbbb');
      yield Buffer.from('cccc');
    }

    const batches: string[] = [];
    for await (const chunk of coalesceChunks(source(), 6)) {
      batches.push(chunk.toString('utf8'));
    }

    expect(batches).toEqual(['aaaabbbb', 'cccc']);
  });

  test('flushes buffered chunks before propagating an error from the next item', async () => {
    async function* source() {
      yield Buffer.from('line1\n');
      throw new Error('boom');
    }

    const iter = coalesceChunks(source())[Symbol.asyncIterator]();
    await expect(iter.next()).resolves.toEqual({ done: false, value: Buffer.from('line1\n') });
    await expect(iter.next()).rejects.toThrow('boom');
  });

  test('closes the underlying iterator when stopped early', async () => {
    let closed = false;
    let yieldedThird = false;
    async function* source() {
      try {
        yield Buffer.from('line1\n');
        await setTimeout(20);
        yield Buffer.from('line2\n');
        yieldedThird = true;
        yield Buffer.from('line3\n');
      } finally {
        closed = true;
      }
    }

    const iter = coalesceChunks(source())[Symbol.asyncIterator]();
    const first = await iter.next();
    expect(first.value?.toString('utf8')).toBe('line1\n');
    await iter.return?.(undefined);
    expect(closed).toBe(true);
    expect(yieldedThird).toBe(false);
  });
});
