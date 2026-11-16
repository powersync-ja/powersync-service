import { createRequire } from 'node:module';
import path from 'node:path';

import { Logger } from '@powersync/lib-services-framework';
import { ServiceContextContainer } from '../system/ServiceContext.js';
import { AbstractModule, AbstractModuleOptions, TearDownOptions } from './AbstractModule.js';

export interface RustModuleOptions extends AbstractModuleOptions {
  addonPath: string;
}

/**
 * Loads Rust code for the PowerSync Service.
 *
 * The `.node` addon file to load is configurable, as different editions of the service use different Rust entrypoints.
 */
export class RustModule extends AbstractModule {
  readonly #require = createRequire(import.meta.url);
  readonly #addonPath: string;

  constructor(options: RustModuleOptions) {
    super(options);
    this.#addonPath = options.addonPath;
  }

  public override async initialize(_context: ServiceContextContainer): Promise<void> {
    await this.loadRust();

    // Currently, we just initialize the module for testing purposes. In the future, we'd register Rust-backed
    // implementations on the service container.
  }

  async loadRust(): Promise<InitializedRustModule> {
    const addon = this.#require(path.resolve(this.#addonPath)) as RustAddon;
    return addon.initializeRust({ logger: this.logger });
  }

  public override async teardown(_options: TearDownOptions): Promise<void> {}
}

interface RustInitializationOptions {
  logger: Logger;
}

interface RustAddon {
  initializeRust(options: RustInitializationOptions): InitializedRustModule;
}

// We use external in napi-rs, https://napi.rs/docs/concepts/external
class InitializedRustModule {
  readonly '': {
    readonly '': unique symbol;
  };
}
