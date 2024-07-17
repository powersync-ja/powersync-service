/**
 * An interface that can be used to create a stateful System. A System is an entity
 * which contains state, generally in the form of connections, that must be stated
 * and stopped gracefully along with a services lifecycle.
 *
 * A System can contain anything but should offer a `start` and `stop` operation
 */

import { container } from '../container.js';

export type LifecycleCallback<T> = (singleton: T) => Promise<void> | void;

export type PartialLifecycle<T> = {
  start?: LifecycleCallback<T>;
  stop?: LifecycleCallback<T>;
};

export type ComponentLifecycle<T> = PartialLifecycle<T> & {
  component: T;
};
export type LifecycleHandler<T> = () => ComponentLifecycle<T>;

export abstract class LifeCycledSystem {
  components: ComponentLifecycle<any>[] = [];

  constructor() {
    container.terminationHandler.handleTerminationSignal(() => this.stop());
  }

  withLifecycle = <T>(component: T, lifecycle: PartialLifecycle<T>): T => {
    this.components.push({
      component: component,
      ...lifecycle
    });
    return component;
  };

  start = async () => {
    for (const lifecycle of this.components) {
      await lifecycle.start?.(lifecycle.component);
    }
  };

  stop = async () => {
    for (const lifecycle of this.components.reverse()) {
      await lifecycle.stop?.(lifecycle.component);
    }
  };
}
