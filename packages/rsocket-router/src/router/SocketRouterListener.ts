import { BaseObserver } from '../utils/BaseObserver.js';

export interface SocketRouterListener {
  cancel: () => void;
  onExtension: () => void;
  request: (n: number) => void;
}

export class SocketRouterObserver extends BaseObserver<SocketRouterListener> {
  triggerCancel() {
    this.iterateListeners((l) => l.cancel?.());
  }

  triggerExtension() {
    this.iterateListeners((l) => l.onExtension?.());
  }

  triggerRequest(n: number) {
    this.iterateListeners((l) => l.request?.(n));
  }
}
