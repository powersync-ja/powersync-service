import { execFile as execFileCallback } from 'node:child_process';
import { rmSync } from 'node:fs';
import { copyFile, mkdtemp } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFile = promisify(execFileCallback);

/**
 * A test helper building the Open Edition Rust addon in the PowerSync Service repository.
 *
 * This uses the addon in `POWERSYNC_RUST_ADDON` if set (CI downloads a prebuilt one), or otherwise builds the addon
 * with `scripts/build-rust-addon.mts` in the Cargo workspace containing this package.
 *
 * A built addon is copied to a fresh temporary directory, the returned cleanup function deletes it. Node can't unload
 * addons and reuses one that is already loaded from the same file, so a unique copy per call makes reloaded tests pick
 * up Rust changes.
 */
export async function resolveRustAddonPath(): Promise<[string, () => void]> {
  const prebuilt = process.env.POWERSYNC_RUST_ADDON;
  if (prebuilt) {
    return [prebuilt, () => {}];
  }

  const source = await buildRustAddon();
  const directory = await mkdtemp(path.join(os.tmpdir(), 'powersync-rust-addon-'));
  const addonPath = path.join(directory, path.basename(source));
  await copyFile(source, addonPath);
  return [addonPath, () => removeAddonDirectory(directory)];
}

function removeAddonDirectory(directory: string) {
  try {
    rmSync(directory, { recursive: true, force: true });
  } catch (e) {
    // Windows doesn't allow deleting a DLL that is still loaded. The OS cleans up the temporary directory eventually.
    if (process.platform == 'win32' && (e as NodeJS.ErrnoException).code == 'EPERM') {
      return;
    }
    throw e;
  }
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
