import { execFile as execFileCallback } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { modules } from '@powersync/service-core';

const execFile = promisify(execFileCallback);

/**
 * Creates a module loading the Open Edition Rust addon.
 *
 * This uses the addon in `POWERSYNC_RUST_ADDON` if set (CI downloads a prebuilt one), or otherwise builds the addon
 * with `scripts/build-rust-addon.mts` in the Cargo workspace containing this package.
 */
export async function rustModuleForTesting(): Promise<modules.RustModule> {
  const addonPath = process.env.POWERSYNC_RUST_ADDON ?? (await buildRustAddon());
  return new modules.RustModule({ name: 'Rust', addonPath });
}

async function buildRustAddon(): Promise<string> {
  const cwd = path.dirname(fileURLToPath(import.meta.url));
  const { stdout: manifestPath } = await execFile(
    'cargo',
    ['locate-project', '--workspace', '--message-format', 'plain'],
    { cwd }
  );
  const script = path.join(path.dirname(manifestPath.trim()), 'scripts', 'build-rust-addon.mts');

  // Cargo is quick when the addon is up to date, and its lock on the build directory makes concurrent builds safe.
  const { stdout } = await execFile(process.execPath, [script], { cwd });
  return stdout.trim();
}
