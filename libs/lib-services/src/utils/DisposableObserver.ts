import { BaseObserver, ObserverClient } from './BaseObserver.js';

export interface DisposableListener {
  /**
   * Event which is fired when the `[Symbol.disposed]` method is called.
   */
  disposed: () => void;
}

export interface ManagedObserverClient<T extends DisposableListener> extends ObserverClient<T> {
  /**
   * Registers a listener that is automatically disposed when the parent is disposed.
   * This is useful for disposing nested listeners.
   */
  registerManagedListener: (parent: DisposableObserverClient<DisposableListener>, cb: Partial<T>) => () => void;
}

export interface DisposableObserverClient<T extends DisposableListener> extends ManagedObserverClient<T>, Disposable {}
export interface AsyncDisposableObserverClient<T extends DisposableListener>
  extends ManagedObserverClient<T>,
    AsyncDisposable {}

export class DisposableObserver<T extends DisposableListener>
  extends BaseObserver<T>
  implements DisposableObserverClient<T>
{
  registerManagedListener(parent: DisposableObserverClient<DisposableListener>, cb: Partial<T>) {
    const disposer = this.registerListener(cb);
    parent.registerListener({
      disposed: () => {
        disposer();
      }
    });
    return disposer;
  }

  [Symbol.dispose]() {
    this.iterateListeners((cb) => cb.disposed?.());
    // Delete all callbacks
    Object.keys(this.listeners).forEach((key) => delete this.listeners[key]);
  }
}
