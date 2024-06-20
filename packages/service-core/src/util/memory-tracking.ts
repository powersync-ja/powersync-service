import { container } from '@powersync/lib-services-framework';

/**
 * Track and log memory usage.
 *
 * Only for debugging purposes. This could add significant overhead and/or side-effects,
 * and should not be used in production.
 */
export function trackMemoryUsage() {
  let lastMem = process.memoryUsage();

  let bufferMemory = {
    alloc: 0,
    allocUnsafe: 0,
    allocUnsafeSlow: 0,
    from: 0,
    concat: 0
  };

  const bufferRegistry = new FinalizationRegistry<[keyof typeof bufferMemory, number]>((v) => {
    const [key, value] = v;
    bufferMemory[key] -= value;
  });

  for (let key of Object.keys(bufferMemory)) {
    const typedKey = key as keyof typeof bufferMemory;
    const originalFunction = Buffer[typedKey] as (...args: any[]) => Buffer;
    Buffer[typedKey] = function (...args: any[]) {
      const buffer = originalFunction.apply(this, args);
      bufferMemory[typedKey] += buffer.byteLength;
      bufferRegistry.register(buffer, [typedKey, buffer.byteLength]);
      return buffer;
    };
  }

  setInterval(() => {
    const mem = process.memoryUsage();

    let isDifferent = false;
    for (const key in mem) {
      if (Math.abs((mem as any)[key] - (lastMem as any)[key]) > 5_000_000) {
        isDifferent = true;
      }
    }

    if (isDifferent) {
      lastMem = mem;

      const output = `RSS ${mb(mem.rss)} (
        HAT ${mb(mem.heapTotal)} (
          HU ${mb(mem.heapUsed)}
        ),
        buffers ${mb(mem.arrayBuffers)} (
          alloc ${mb(bufferMemory.alloc + bufferMemory.allocUnsafe + bufferMemory.allocUnsafeSlow)},
          from ${mb(bufferMemory.from)}
          concat ${mb(bufferMemory.concat)}
        )
      )`.replaceAll(/\s+/g, ' ');

      container.logger.info(output);
    }
  }, 50);
}

function mb(n: number) {
  return Math.round(n / 1024 / 1024) + 'mb';
}
