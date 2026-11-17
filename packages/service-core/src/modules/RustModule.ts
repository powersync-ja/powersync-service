import { createRequire } from 'node:module';
import path from 'node:path';

import { Logger } from '@powersync/lib-services-framework';
import type { SqliteRow } from '@powersync/service-sync-rules';
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
    const addon = this.loadAddon();
    return addon.initializeRust({ logger: this.logger });
  }

  /** Creates the opt-in experimental MongoDB converter provided by the addon. */
  createMongoSourceRowConverter(dateRenderMode: number, fixedBooleansInJson: boolean): RustMongoSourceRowConverter {
    const addon = this.loadAddon();
    return new addon.MongoSourceRowConverter(dateRenderMode, fixedBooleansInJson);
  }

  private loadAddon(): RustAddon {
    return this.#require(path.resolve(this.#addonPath)) as RustAddon;
  }

  public override async teardown(_options: TearDownOptions): Promise<void> {}
}

interface RustInitializationOptions {
  logger: Logger;
}

interface RustAddon {
  initializeRust(options: RustInitializationOptions): InitializedRustModule;
  MongoSourceRowConverter: new (dateRenderMode: number, fixedBooleansInJson: boolean) => RustMongoSourceRowConverter;
}

export interface RustMongoSourceRowConverter {
  rawToSqliteRow(source: Buffer): { row: SqliteRow; idDocument: Buffer };
}

// We use external in napi-rs, https://napi.rs/docs/concepts/external
class InitializedRustModule {
  readonly '': {
    readonly '': unique symbol;
  };
}
