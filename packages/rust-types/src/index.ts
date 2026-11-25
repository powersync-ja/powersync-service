import { createRequire } from 'node:module';
import path from 'node:path';
import type { Logger } from 'winston';

const require = createRequire(import.meta.url);

/**
 * Loads the PowerSync Service Node addon from the given `.node` file.
 */
export function loadRustAddon(addonPath: string): typeof RustAddon {
  return require(path.resolve(addonPath)) as typeof RustAddon;
}

/**
 * Exports of the PowerSync Service Node addon.
 *
 * These are written by hand and need to be kept in sync with the `#[napi]` exports of the addon entrypoint, see
 * `crates/open_edition_js_entrypoint`.
 */
export declare namespace RustAddon {
  function initializeRust(options: RustInitializationOptions): InitializedRustModule;

  interface RustInitializationOptions {
    /**
     * The winston logger to forward Rust log events to.
     */
    logger: Logger;
  }

  /**
   * An opaque handle to the initialized service.
   *
   * We use externals in napi-rs, https://napi.rs/docs/concepts/external
   */
  class InitializedRustModule {
    private constructor();

    readonly '': {
      readonly '': unique symbol;
    };
  }
}
