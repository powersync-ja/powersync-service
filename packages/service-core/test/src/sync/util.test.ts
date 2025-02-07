import { acquireSemaphoreAbortable } from '@/index.js';
import { Semaphore, SemaphoreInterface } from 'async-mutex';
import { describe, expect, test, vi } from 'vitest';

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
});
