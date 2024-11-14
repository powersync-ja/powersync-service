import { v4 as uuid } from 'uuid';

export interface ObserverClient<T> {
  registerListener(listener: Partial<T>): () => void;
}

export class BaseObserver<T> implements ObserverClient<T> {
  protected listeners: { [id: string]: Partial<T> };

  constructor() {
    this.listeners = {};
  }

  registerListener(listener: Partial<T>): () => void {
    const id = uuid();
    this.listeners[id] = listener;
    return () => {
      delete this.listeners[id];
    };
  }

  iterateListeners(cb: (listener: Partial<T>) => any) {
    for (let i in this.listeners) {
      cb(this.listeners[i]);
    }
  }

  async iterateAsyncListeners(cb: (listener: Partial<T>) => Promise<any>) {
    for (let i in this.listeners) {
      await cb(this.listeners[i]);
    }
  }
}
