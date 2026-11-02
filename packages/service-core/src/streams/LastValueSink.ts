/**
 * An AsyncIterable that allows pushing values.
 *
 * If the consumer is slower than the source, only the last value is buffered.
 *
 * Similar to ix AsyncSink, except that we only keep the last value.
 */
export class LastValueSink<T> implements AsyncIterable<T> {
  buffer: NextResult<T> | undefined;
  nextResolve: undefined | (() => void);

  constructor(initial: T | undefined) {
    if (initial != null) {
      this.buffer = { value: initial, done: false };
    }
  }

  write(value: T) {
    this.push({
      value,
      done: false
    });
  }

  end() {
    this.push({
      value: undefined,
      done: true
    });
  }

  error(e: unknown) {
    this.push({
      value: undefined,
      done: true,
      error: e
    });
  }

  private push(r: NextResult<T>) {
    if (this.buffer?.done) {
      return;
    }
    this.buffer = r;
    this.nextResolve?.();
  }

  async *withSignal(signal?: AbortSignal): AsyncIterable<T> {
    if (!signal) {
      yield* this;
      return;
    }

    signal.throwIfAborted();

    const onAbort = () => {
      this.error(signal.reason);
    };
    signal.addEventListener('abort', onAbort, { once: true });

    try {
      yield* this;
    } finally {
      signal.removeEventListener('abort', onAbort);
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    while (true) {
      if (this.buffer == null) {
        const promise = new Promise<void>((resolve) => {
          this.nextResolve = resolve;
        });
        await promise;
      }
      const n = this.buffer!;
      this.buffer = undefined;

      if ('error' in n) {
        throw n.error;
      } else if (n.done) {
        return;
      } else {
        yield n.value!;
      }
    }
  }
}

interface NextResult<T> {
  value: T | undefined;
  done: boolean;
  error?: unknown;
}
