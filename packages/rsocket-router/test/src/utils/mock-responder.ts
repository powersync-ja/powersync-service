import { SocketRouterObserver } from '../../../src/router/SocketRouterListener.js';
import { SocketResponder } from '../../../src/router/types.js';

export function createMockResponder(): SocketResponder {
  return {
    onComplete: () => {},
    onError: (error) => {},
    onExtension: () => {},
    onNext: (payload, isComplete) => {}
  };
}

export function createMockObserver(): SocketRouterObserver {
  return new SocketRouterObserver();
}
