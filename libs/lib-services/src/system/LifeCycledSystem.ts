/**
 * An interface that can be used to create a stateful System. A System is an entity
 * which contains state, generally in the form of connections, that must be stated
 * and stopped gracefully along with a services lifecycle.
 *
 * A System can contain anything but should offer a `start` and `stop` operation
 */

import { ServiceError } from '@powersync/service-errors';
import { container } from '../container.js';
import { logger } from '../logger/Logger.js';
// TODO: REMOVE THIS COMMENT
export type LifecycleCallback<T> = (singleton: T) => Promise<void> | void;

export type PartialLifecycle<T> = {
  start?: LifecycleCallback<T>;
  stop?: LifecycleCallback<T>;
};

export type ComponentLifecycle<T> = PartialLifecycle<T> & {
  component: T;
};
export type LifecycleHandler<T> = () => ComponentLifecycle<T>;

export class LifeCycledSystem {
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

  stopWithError = async (error: ServiceError) => {
    try {
      logger.error('Stopping process due to fatal error', error);
      await this.stop();
    } catch (e) {
      logger.error('Error while stopping', e);
    } finally {
      // Custom error code to distinguish from other common errors
      logger.warn(`Exiting with code 151`);
      process.exit(151);
    }
  };
}
