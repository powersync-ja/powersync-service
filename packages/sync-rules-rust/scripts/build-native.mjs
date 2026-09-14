import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
execFileSync('cargo', ['build', '--release', '--locked', '--manifest-path', 'native/Cargo.toml'], {
  cwd: root,
  stdio: 'inherit'
});
const library = {
  linux: 'libpowersync_sync_rules_rust.so',
  darwin: 'libpowersync_sync_rules_rust.dylib',
  win32: 'powersync_sync_rules_rust.dll'
}[process.platform];
if (!library) throw new Error(`Unsupported native build platform: ${process.platform}`);
mkdirSync(`${root}/dist`, { recursive: true });
copyFileSync(`${root}/native/target/release/${library}`, `${root}/dist/evaluator.node`);
