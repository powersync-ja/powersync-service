import { ServiceAssertionError } from '@powersync/service-errors';
import _ from 'lodash';
import { ErrorReporter } from './alerts/definitions.js';
import { NoOpReporter } from './alerts/no-op-reporter.js';
import { MigrationManager } from './migrations/MigrationManager.js';
import {
  createInMemoryProbe,
  createTerminationHandler,
  ProbeModule,
  TerminationHandler
} from './signals/signals-index.js';

export enum ContainerImplementation {
  REPORTER = 'reporter',
  PROBES = 'probes',
  TERMINATION_HANDLER = 'termination-handler',
  MIGRATION_MANAGER = 'migration-manager'
}

export type ContainerImplementationTypes = {
  [ContainerImplementation.REPORTER]: ErrorReporter;
  [ContainerImplementation.PROBES]: ProbeModule;
  [ContainerImplementation.TERMINATION_HANDLER]: TerminationHandler;
  [ContainerImplementation.MIGRATION_MANAGER]: MigrationManager;
};

export type RegisterDefaultsOptions = {
  skip?: ContainerImplementation[];
};

export type ContainerImplementationDefaultGenerators = {
  [type in ContainerImplementation]: () => ContainerImplementationTypes[type];
};

/**
 * Helper for identifying constructors
 */
export interface Abstract<T> {
  prototype: T;
}
/**
 * A basic class constructor
 */
export type Newable<T> = new (...args: never[]) => T;

/**
 * Identifier used to get and register implementations
 */
export type ServiceIdentifier<T = unknown> = string | symbol | Newable<T> | Abstract<T> | ContainerImplementation;
const DEFAULT_GENERATORS: ContainerImplementationDefaultGenerators = {
  [ContainerImplementation.REPORTER]: () => NoOpReporter,
  [ContainerImplementation.PROBES]: () => createInMemoryProbe(),
  [ContainerImplementation.TERMINATION_HANDLER]: () => createTerminationHandler(),
  [ContainerImplementation.MIGRATION_MANAGER]: () => new MigrationManager()
};

/**
 * A container which provides means for registering and getting various
 * function implementations.
 */
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

  /**
   * Manager for system migrations.
   */
  get migrationManager() {
    return this.getImplementation(ContainerImplementation.MIGRATION_MANAGER);
  }

  constructor() {
    this.implementations = new Map();
  }

  /**
   * Gets an implementation given an identifier.
   * An exception is thrown if the implementation has not been registered.
   * Core [ContainerImplementation] identifiers are mapped to their respective implementation types.
   * This also allows for getting generic implementations (unknown to the core framework) which have been registered.
   */
  getImplementation<T>(identifier: Newable<T> | Abstract<T>): T;
  getImplementation<T extends ContainerImplementation>(identifier: T): ContainerImplementationTypes[T];
  getImplementation<T>(identifier: ServiceIdentifier<T>): T;
  getImplementation<T>(identifier: ServiceIdentifier<T>): T {
    const implementation = this.implementations.get(identifier);
    if (!implementation) {
      throw new ServiceAssertionError(`Implementation for ${String(identifier)} has not been registered.`);
    }
    return implementation;
  }

  /**
   * Gets an implementation given an identifier.
   * Null is returned if the implementation has not been registered yet.
   * Core [ContainerImplementation] identifiers are mapped to their respective implementation types.
   * This also allows for getting generic implementations (unknown to the core framework) which have been registered.
   */
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
   * Allows for registering core and generic implementations of services/helpers.
   */
  register<T>(identifier: ServiceIdentifier<T>, implementation: T) {
    this.implementations.set(identifier, implementation);
  }
}

export const container = new Container();
