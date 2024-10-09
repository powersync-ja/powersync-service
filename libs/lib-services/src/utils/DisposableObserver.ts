import { BaseObserver, ObserverClient } from './BaseObserver.js';

export interface DisposableListener {
  disposed: () => void;
}

export interface DisposableObserverClient<T extends DisposableListener> extends ObserverClient<T>, Disposable {}

export class DisposableObserver<T extends DisposableListener>
  extends BaseObserver<T>
  implements DisposableObserverClient<T>
{
  [Symbol.dispose]() {
    this.iterateListeners((cb) => cb.disposed?.());
    // Delete all callbacks
    Object.keys(this.listeners).forEach((key) => delete this.listeners[key]);
  }
}
