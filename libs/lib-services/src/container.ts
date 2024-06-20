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

export class Container {
  protected implementations: ContainerImplementationTypes;

  /**
   * Manager for system health probes
   */
  get probes() {
    return this.implementations[ContainerImplementation.PROBES];
  }

  /**
   * Error reporter. Defaults to a no-op reporter
   */
  get reporter() {
    return this.implementations[ContainerImplementation.REPORTER];
  }

  /**
   * Handler for termination of the Node process
   */
  get terminationHandler() {
    return this.implementations[ContainerImplementation.TERMINATION_HANDLER];
  }

  constructor() {
    this.implementations = {
      [ContainerImplementation.REPORTER]: NoOpReporter,
      [ContainerImplementation.PROBES]: createFSProbe(),
      [ContainerImplementation.TERMINATION_HANDLER]: createTerminationHandler()
    };
  }

  /**
   * Allows for overriding a default implementation
   */
  register<Type extends ContainerImplementation>(type: Type, implementation: ContainerImplementationTypes[Type]) {
    this.implementations[type] = implementation;
  }
}

export const container = new Container();
