/**
 * Builds the Open Edition Rust addon for local development and tests.
 *
 * Node only loads addons with a `.node` extension, so this copies the shared library cargo produces to
 * `open_edition_js_entrypoint.node` in the same directory. Additional arguments (e.g. `--release`) are passed to
 * `cargo build`. The path of the addon is printed to stdout, everything else is written to stderr.
 */
import { spawn } from 'node:child_process';
import { copyFile, rename, stat } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline';

const CRATE_NAME = 'open_edition_js_entrypoint';

const cargo = spawn(
  'cargo',
  ['build', '-p', CRATE_NAME, '--message-format=json-render-diagnostics', ...process.argv.slice(2)],
  { stdio: ['ignore', 'pipe', 'inherit'] }
);
const exited = new Promise<number | null>((resolve, reject) => {
  cargo.on('error', reject);
  cargo.on('close', resolve);
});

// With a JSON message format, cargo reports the exact files it produced instead of us having to guess
// platform-specific library names.
let libraryPath: string | undefined;
for await (const line of createInterface({ input: cargo.stdout })) {
  const message = JSON.parse(line);
  if (
    message.reason == 'compiler-artifact' &&
    message.target.name == CRATE_NAME &&
    message.target.kind.includes('cdylib')
  ) {
    // On Windows, this also includes the import library and debug symbols.
    libraryPath = (message.filenames as string[]).find((f) => /\.(so|dylib|dll)$/.test(f));
  }
}

const status = await exited;
if (status != 0) {
  process.exit(status ?? 1);
}
if (libraryPath == null) {
  console.error(`cargo did not report a shared library for ${CRATE_NAME}`);
  process.exit(1);
}

const addonPath = path.join(path.dirname(libraryPath), `${CRATE_NAME}.node`);
// copyFile overwrites an existing destination in place, which crashes processes that have the previous addon loaded
// (e.g. a Vitest process re-running tests in watch mode). Replacing the file with a rename keeps their copy intact, we
// also avoid writes when we know the .node copy is up-to-date.
const [library, addon] = await Promise.all([stat(libraryPath), stat(addonPath).catch(() => null)]);
if (addon == null || addon.mtimeMs <= library.mtimeMs) {
  const tempPath = `${addonPath}.${process.pid}.tmp`;
  await copyFile(libraryPath, tempPath);
  await rename(tempPath, addonPath);
}
console.log(addonPath);
