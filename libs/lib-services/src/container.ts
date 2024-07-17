import _ from 'lodash';
import { ErrorReporter } from './alerts/definitions.js';
import { NoOpReporter } from './alerts/no-op-reporter.js';
import { ProbeModule, TerminationHandler, createFSProbe, createTerminationHandler } from './signals/signals-index.js';

export enum ContainerImplementation {
  REPORTER = 'reporter',
  PROBES = 'probes',
  TERMINATION_HANDLER = 'termination-handler'
}

export type ContainerImplementationTypes = {
  [ContainerImplementation.REPORTER]: ErrorReporter;
  [ContainerImplementation.PROBES]: ProbeModule;
  [ContainerImplementation.TERMINATION_HANDLER]: TerminationHandler;
};

export type RegisterDefaultsOptions = {
  skip?: ContainerImplementation[];
};

export type ContainerImplementationDefaultGenerators = {
  [type in ContainerImplementation]: () => ContainerImplementationTypes[type];
};

export interface Abstract<T> {
  prototype: T;
}
export type Newable<T> = new (...args: never[]) => T;
export type ServiceIdentifier<T = unknown> = string | symbol | Newable<T> | Abstract<T> | ContainerImplementation;

const DEFAULT_GENERATORS: ContainerImplementationDefaultGenerators = {
  [ContainerImplementation.REPORTER]: () => NoOpReporter,
  [ContainerImplementation.PROBES]: () => createFSProbe(),
  [ContainerImplementation.TERMINATION_HANDLER]: () => createTerminationHandler()
};

export class Container {
  protected implementations: Map<ServiceIdentifier<any>, any>;

  /**
   * Manager for system health probes
   */
  get probes() {
    return this.getImplementation(ContainerImplementation.PROBES);
  }

  /**
   * Error reporter. Defaults to a no-op reporter
   */
  get reporter() {
    return this.getImplementation(ContainerImplementation.REPORTER);
  }

  /**
   * Handler for termination of the Node process
   */
  get terminationHandler() {
    return this.getImplementation(ContainerImplementation.TERMINATION_HANDLER);
  }

  constructor() {
    this.implementations = new Map();
  }

  getImplementation<T>(identifier: Newable<T> | Abstract<T>): T;
  getImplementation<T extends ContainerImplementation>(identifier: T): ContainerImplementationTypes[T];
  getImplementation<T>(identifier: ServiceIdentifier<T>): T;
  getImplementation<T>(identifier: ServiceIdentifier<T>): T {
    const implementation = this.implementations.get(identifier);
    if (!implementation) {
      throw new Error(`Implementation for ${String(identifier)} has not been registered.`);
    }
    return implementation;
  }

  getOptional<T>(identifier: Newable<T> | Abstract<T>): T | null;
  getOptional<T extends ContainerImplementation>(identifier: T): ContainerImplementationTypes[T] | null;
  getOptional<T>(identifier: ServiceIdentifier<T>): T | null;
  getOptional<T>(identifier: ServiceIdentifier<T>): T | null {
    const implementation = this.implementations.get(identifier);
    if (!implementation) {
      return null;
    }
    return implementation;
  }

  /**
   * Registers default implementations
   */
  registerDefaults(options?: RegisterDefaultsOptions) {
    _.difference(Object.values(ContainerImplementation), options?.skip ?? []).forEach((type) => {
      const generator = DEFAULT_GENERATORS[type];
      this.register(type, generator());
    });
  }

  /**
   * Allows for overriding a default implementation
   */
  register<T>(identifier: ServiceIdentifier<T>, implementation: T) {
    this.implementations.set(identifier, implementation);
  }
}

export const container = new Container();
