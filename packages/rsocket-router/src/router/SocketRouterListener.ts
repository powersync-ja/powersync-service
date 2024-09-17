import { BaseObserver } from '../utils/BaseObserver.js';

export interface SocketRouterListener {
  onExtension: () => void;
  request: (n: number) => void;
}

export class SocketRouterObserver extends BaseObserver<SocketRouterListener> {
  triggerExtension() {
    this.iterateListeners((l) => l.onExtension?.());
  }

  triggerRequest(n: number) {
    this.iterateListeners((l) => l.request?.(n));
  }
}
