import { loadRustAddon, RustAddon } from '@powersync/service-rust-types';

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

  async loadRust(): Promise<RustAddon.InitializedRustModule> {
    const addon = loadRustAddon(this.#addonPath);
    return addon.initializeRust({ logger: this.logger });
  }

  public override async teardown(_options: TearDownOptions): Promise<void> {}
}
