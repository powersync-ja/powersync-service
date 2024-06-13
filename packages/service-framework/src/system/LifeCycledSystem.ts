/**
 * An interface that can be used to create a stateful System. A System is an entity
 * which contains state, generally in the form of connections, that must be stated
 * and stopped gracefully along with a services lifecycle.
 *
 * A System can contain anything but should offer a `start` and `stop` operation
 */

import { createFSProbe } from '../signals/probes/fs-probes';
import { ProbeModule } from '../signals/probes/probes';
import { TerminationHandler, createTerminationHandler } from '../signals/termination-handler';

export type LifecycleCallback<T> = (singleton: T) => Promise<void> | void;

export type PartialLifecycle<T> = {
  start?: LifecycleCallback<T>;
  stop?: LifecycleCallback<T>;
};

export type ComponentLifecycle<T> = PartialLifecycle<T> & {
  component: T;
};
export type LifecycleHandler<T> = () => ComponentLifecycle<T>;

export type LifeCycledSystemOptions = {
  /**
   * Optional termination handler. Defaults to a NodeJS process listener handler
   * if not provided.
   */
  terminationHandler?: TerminationHandler;

  /**
   * Probe handler for system readiness and liveliness state management.
   * Defaults to a FileSystem based probe if not provided.
   */
  probe?: ProbeModule;
};

export abstract class LifeCycledSystem {
  components: ComponentLifecycle<any>[] = [];
  terminationHandler: TerminationHandler;
  probe: ProbeModule;

  constructor(options?: LifeCycledSystemOptions) {
    this.terminationHandler = options?.terminationHandler ?? createTerminationHandler();
    this.terminationHandler.handleTerminationSignal(() => this.stop());
    this.probe = options?.probe ?? createFSProbe();
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
