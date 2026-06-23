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

export type LifecycleCallback<T> = (singleton: T) => Promise<void> | void;

export type PartialLifecycle<T> = {
  start?: LifecycleCallback<T>;
  stop?: LifecycleCallback<T>;
  stopAccepting?: LifecycleCallback<T>;
  completed?: (singleton: T) => Promise<void>;
};

export type ComponentLifecycle<T> = PartialLifecycle<T> & {
  component: T;
};
export type LifecycleHandler<T> = () => ComponentLifecycle<T>;

export const STOP_ACCEPTING_SIGNAL: NodeJS.Signals = 'SIGUSR2';

export class LifeCycledSystem {
  components: ComponentLifecycle<any>[] = [];
  private stopPromise: Promise<void> | null = null;
  private completionMonitor: Promise<void> | null = null;
  private accepting = true;
  private stopAcceptingSignalRegistered = false;

  private readonly stopAcceptingSignalHandler = () => {
    logger.info(`Received ${STOP_ACCEPTING_SIGNAL}; stopping system components from accepting new work.`);
    this.stopAccepting().catch((error) => {
      logger.error('Failed to stop accepting new work', error);
    });
  };

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
    if (!this.stopAcceptingSignalRegistered) {
      process.on(STOP_ACCEPTING_SIGNAL, this.stopAcceptingSignalHandler);
      this.stopAcceptingSignalRegistered = true;
    }

    const completions: Promise<void>[] = [];
    for (const lifecycle of this.components) {
      await lifecycle.start?.(lifecycle.component);
      if (lifecycle.completed != null) {
        completions.push(lifecycle.completed(lifecycle.component));
      }
    }

    if (completions.length > 0) {
      this.completionMonitor = Promise.all(completions)
        .then(async () => {
          logger.info('All running lifecycle components completed. Stopping system.');
          await this.stop();
        })
        .catch(async (error) => {
          logger.error('Lifecycle component completion failed', error);
          await this.stop();
        });
    }
  };

  stopAccepting = async () => {
    if (!this.accepting) {
      return;
    }

    this.accepting = false;
    for (const lifecycle of [...this.components].reverse()) {
      await lifecycle.stopAccepting?.(lifecycle.component);
    }
  };

  stop = async () => {
    if (this.stopPromise != null) {
      return this.stopPromise;
    }

    this.stopPromise = (async () => {
      if (this.stopAcceptingSignalRegistered) {
        process.off(STOP_ACCEPTING_SIGNAL, this.stopAcceptingSignalHandler);
        this.stopAcceptingSignalRegistered = false;
      }

      for (const lifecycle of [...this.components].reverse()) {
        await lifecycle.stop?.(lifecycle.component);
      }
    })();

    return this.stopPromise;
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
