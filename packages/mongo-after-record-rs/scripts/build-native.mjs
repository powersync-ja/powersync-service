import { spawnSync } from 'node:child_process';
import { cpSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const profile = process.argv.includes('--release') ? 'release' : 'debug';
const targetName =
  process.platform === 'win32'
    ? 'mongo_after_record_rs.dll'
    : process.platform === 'darwin'
      ? 'libmongo_after_record_rs.dylib'
      : 'libmongo_after_record_rs.so';

const build = spawnSync('cargo', ['build', '--features', 'node', ...(profile === 'release' ? ['--release'] : [])], {
  cwd: root,
  stdio: 'inherit'
});
if (build.status !== 0) {
  process.exit(build.status ?? 1);
}

const builtLibrary = path.join(root, 'target', profile, targetName);
if (!existsSync(builtLibrary)) {
  console.error(`Expected native library not found: ${builtLibrary}`);
  process.exit(1);
}

cpSync(builtLibrary, path.join(root, 'mongo-after-record-rs.node'));
