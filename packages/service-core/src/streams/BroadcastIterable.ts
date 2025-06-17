import { AbortError } from 'ix/aborterror.js';
import { LastValueSink } from './LastValueSink.js';

export type IterableSource<T> = (signal: AbortSignal) => AsyncIterable<T>;

/**
 * Loosely based on Dart's broadcast streams:
 * https://api.flutter.dev/flutter/dart-async/StreamController/StreamController.broadcast.html
 *
 * 1. Exposes an AsyncIterable interface.
 * 2. Allows multiple concurrent subscribers.
 * 3. Any new subscriber gets new events as they are published.
 * 4. The source iterable is only created once there are subscribers.
 *
 * One notable difference: The last value is buffered and immediately returned for any new subscribers.
 *
 * When all subscribers are stopped, this source iterable is stopped.
 *
 * Any error on the source is passed on to all subscribers. This generally stops all
 * subscribers. Once a new subscriber is then added, a new source is started.
 *
 * Slow subscribers will skip events.
 *
 * Implementation note: It is possible to do this using:
 * 1. rxjs `share` for multicasting and auto-starting + stopping the source.
 * 2. rxjs `BehaviorSubject` to keep the last value.
 * 3. Use `LastValueSink` to convert to an AsyncIterable.
 */
export class BroadcastIterable<T> implements AsyncIterable<T> {
  private last: T | undefined = undefined;
  private subscribers: Set<LastValueSink<T>> | undefined = undefined;
  private abortController: AbortController | undefined = undefined;

  constructor(private source: IterableSource<T>) {}

  private start(sink: LastValueSink<T>) {
    const abortController = new AbortController();
    const listeners = new Set<LastValueSink<T>>();
    listeners.add(sink);

    this.abortController = abortController;
    this.subscribers = listeners;

    this.loop(abortController, listeners);
  }

  private async loop(abortController: AbortController, sinks: Set<LastValueSink<T>>) {
    try {
      for await (let doc of this.source(abortController.signal)) {
        if (abortController.signal.aborted || sinks.size == 0) {
          throw new AbortError();
        }
        this.last = doc;
        for (let sink of sinks) {
          sink.write(doc);
        }
      }

      // End of stream
      for (let sink of sinks) {
        sink.end();
      }
    } catch (e) {
      // Just in case the error is not from the source
      abortController.abort();

      for (let listener of sinks) {
        listener.error(e);
      }
    } finally {
      // Clear state, so that a new subscription may be started
      if (this.subscribers === sinks) {
        this.subscribers = undefined;
        this.abortController = undefined;
        this.last = undefined;
      }
    }
  }

  private removeSink(listener: LastValueSink<T>) {
    this.subscribers?.delete(listener);
    if (this.subscribers?.size == 0) {
      // This is not immediate - there may be a delay until it is fully stopped,
      // depending on the underlying source.
      this.abortController?.abort();
      this.last = undefined;
      this.subscribers = undefined;
      this.abortController = undefined;
    }
  }

  private addSink(listener: LastValueSink<T>) {
    if (this.subscribers == null) {
      this.start(listener);
    } else {
      this.subscribers.add(listener);
    }
  }

  async *[Symbol.asyncIterator](signal?: AbortSignal): AsyncIterableIterator<T> {
    const sink = new LastValueSink(this.last);
    this.addSink(sink);
    try {
      yield* sink.withSignal(signal);
    } finally {
      this.removeSink(sink);
    }
  }

  get active() {
    return this.subscribers != null;
  }
}

// For reference, this is an alternative implementation using rxjs.
// It still relies on LastValueSink.
//
// import { BehaviorSubject, Observable, filter, from, share } from 'rxjs';
//
// export class RxBroadcastIterable<T> implements AsyncIterable<T> {
//   private observable: Observable<T>;
//
//   constructor(source: IterableSource<T>) {
//     const obsSource = new Observable<T>((subscriber) => {
//       const controller = new AbortController();
//       const s = source(controller.signal);
//       const inner = from(s);
//       const subscription = inner.subscribe(subscriber);
//       subscription.add({
//         unsubscribe() {
//           controller.abort();
//         }
//       });
//       return subscription;
//     });
//
//     const obs = obsSource.pipe(
//       share({ connector: () => new BehaviorSubject(undefined as T) }),
//       filter((v) => v != undefined)
//     );
//     this.observable = obs;
//   }
//
//   async *[Symbol.asyncIterator](signal?: AbortSignal): AsyncIterator<T> {
//     if (signal?.aborted) {
//       return;
//     }
//     const sink = new LastValueSink<T>(undefined);
//     const subscription = this.observable.subscribe(sink);
//
//     try {
//       yield* sink.withSignal(signal);
//     } finally {
//       subscription.unsubscribe();
//     }
//   }
//
//   get active() {
//     return false;
//   }
// }
