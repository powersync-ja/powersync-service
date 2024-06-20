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

const DEFAULT_GENERATORS: ContainerImplementationDefaultGenerators = {
  [ContainerImplementation.REPORTER]: () => NoOpReporter,
  [ContainerImplementation.PROBES]: () => createFSProbe(),
  [ContainerImplementation.TERMINATION_HANDLER]: () => createTerminationHandler()
};

export class Container {
  protected implementations: Partial<ContainerImplementationTypes>;

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
    this.implementations = {};
  }

  getImplementation<Type extends ContainerImplementation>(type: Type) {
    const implementation = this.implementations[type];
    if (!implementation) {
      throw new Error(`Implementation for ${type} has not been registered.`);
    }
    return implementation;
  }

  /**
   * Registers default implementations
   */
  registerDefaults(options?: RegisterDefaultsOptions) {
    _.difference(Object.values(ContainerImplementation), options?.skip ?? []).forEach((type) => {
      const generator = DEFAULT_GENERATORS[type];
      this.implementations[type] = generator() as any; // :(
    });
  }

  /**
   * Allows for overriding a default implementation
   */
  register<Type extends ContainerImplementation>(type: Type, implementation: ContainerImplementationTypes[Type]) {
    this.implementations[type] = implementation;
  }
}

export const container = new Container();
