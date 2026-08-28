import { acquireSemaphoreAbortable, isAbortError } from '@/index.js';
import { Semaphore, SemaphoreInterface } from 'async-mutex';
import { AbortError } from 'ix/aborterror.js';
import { describe, expect, test, vi } from 'vitest';

describe('isAbortError', () => {
  test('recognizes ix and native abort errors', () => {
    const controller = new AbortController();
    controller.abort();

    expect(isAbortError(new AbortError())).toBe(true);
    expect(isAbortError(controller.signal.reason)).toBe(true);
    expect(isAbortError(new Error('not aborted'))).toBe(false);
  });
});

describe('acquireSemaphoreAbortable', () => {
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
