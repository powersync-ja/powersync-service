import { LastValueSink } from '@/streams/LastValueSink.js';
import { describe, expect, test } from 'vitest';

describe('LastValueSink cancellation', () => {
  test.each([
    { reason: undefined },
    { reason: new DOMException('Cancelled', 'AbortError') },
    { reason: new Error('Cancelled') },
    { reason: 'cancelled' },
    { reason: 0 },
    { reason: false },
    { reason: '' },
    { reason: null }
  ])('preserves abort reason $reason before and during a read', async ({ reason }) => {
    const controller = new AbortController();
    const sink = new LastValueSink<number>(undefined);
    const iterator = sink.withSignal(controller.signal)[Symbol.asyncIterator]();
    const pending = iterator.next();
    controller.abort(reason);
    await expect(pending).rejects.toBe(controller.signal.reason);

    const alreadyAborted = new LastValueSink<number>(1).withSignal(controller.signal)[Symbol.asyncIterator]();
    await expect(alreadyAborted.next()).rejects.toBe(controller.signal.reason);
  });

  test('uses a DOM AbortError for default cancellation', async () => {
    const signal = AbortSignal.abort();
    const iterator = new LastValueSink<number>(undefined).withSignal(signal)[Symbol.asyncIterator]();
    await expect(iterator.next()).rejects.toBeInstanceOf(DOMException);
    await expect(new LastValueSink<number>(1).withSignal(signal)[Symbol.asyncIterator]().next()).rejects.toMatchObject({
      name: 'AbortError'
    });
  });
});
