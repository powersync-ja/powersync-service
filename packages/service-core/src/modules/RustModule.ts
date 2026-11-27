import { loadRustAddon, RustAddon } from '@powersync/service-rust-types';

import { ServiceIdentifier } from '@powersync/lib-services-framework';
import { ServiceContextContainer } from '../system/ServiceContext.js';
import { AbstractModule, AbstractModuleOptions, TearDownOptions } from './AbstractModule.js';

export type { RustAddon };

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

  public override async initialize(context: ServiceContextContainer): Promise<void> {
    const [addon, _] = await this.loadRust();

    // Currently, we just initialize the module for testing purposes. In the future, we'd register Rust-backed
    // implementations on the service container.
    context.register(RustModule.id, addon);
  }

  async loadRust(): Promise<[typeof RustAddon, RustAddon.InitializedRustModule]> {
    const addon = loadRustAddon(this.#addonPath);
    return [addon, addon.initializeRust({ logger: this.logger })];
  }

  public override async teardown(_options: TearDownOptions): Promise<void> {}

  static readonly id: ServiceIdentifier<typeof RustAddon> = 'rust-addon';
}
