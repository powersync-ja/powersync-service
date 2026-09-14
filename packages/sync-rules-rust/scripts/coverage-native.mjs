import { spawn } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
// Async pipes work in restricted environments where synchronous output capture can fail.
async function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: root, stdio: ['ignore', 'pipe', 'inherit'], ...options });
    let output = '';
    child.stdout?.setEncoding('utf8');
    child.stdout?.on('data', (chunk) => {
      output += chunk;
    });
    child.on('error', reject);
    child.on('close', (code, signal) => {
      if (code === 0) resolve(output);
      else reject(new Error(`${command} failed (${signal ?? code})`));
    });
  });
}
const version = await run('rustc', ['-vV']);
const major = version.match(/LLVM version: (\d+)/)?.[1];
if (!major) throw new Error('Cannot determine rustc LLVM version');
const sysroot = (await run('rustc', ['--print', 'sysroot'])).trim();
const host = version.match(/host: (.+)/)?.[1];
const llvmBin = path.join(sysroot, 'lib', 'rustlib', host, 'bin');
function tool(name, override) {
  if (process.env[override]) return process.env[override];
  const bundled = path.join(llvmBin, name);
  return existsSync(bundled) ? bundled : `${name}-${major}`;
}
const profdata = tool('llvm-profdata', 'LLVM_PROFDATA');
const cov = tool('llvm-cov', 'LLVM_COV');
await run(profdata, ['--version']);
await run(cov, ['--version']);
mkdirSync(path.join(root, 'coverage'), { recursive: true });
const output = mkdtempSync(path.join(root, 'coverage/native-'));
const env = {
  ...process.env,
  RUSTFLAGS: '-C instrument-coverage --cfg native_coverage',
  LLVM_PROFILE_FILE: `${output}/%p-%m.profraw`
};
const cargoArgs = ['--locked', '--manifest-path', 'native/Cargo.toml', '--target-dir', 'native/target/coverage'];
await run('cargo', ['build', ...cargoArgs], { env, stdio: 'inherit' });
const extension = { linux: 'so', darwin: 'dylib' }[process.platform];
if (!extension) throw new Error('Native coverage script currently supports Linux and macOS');
const library = path.join(root, `native/target/coverage/debug/libpowersync_sync_rules_rust.${extension}`);
const addon = path.join(root, 'dist/evaluator.node');
const backup = path.join(output, 'release.node');
copyFileSync(addon, backup);
try {
  copyFileSync(library, addon);
  await run('pnpm', ['--config.verify-deps-before-run=false', 'exec', 'vitest', 'run'], { env, stdio: 'inherit' });
} finally {
  // Never leave an instrumented debug addon in place for subsequent benchmarks.
  copyFileSync(backup, addon);
}
const buildOutput = await run('cargo', ['test', '--no-run', '--message-format=json', ...cargoArgs], { env });
const binaries = buildOutput
  .split('\n')
  .filter(Boolean)
  .map((line) => JSON.parse(line))
  .filter((event) => event.reason === 'compiler-artifact' && event.profile.test && event.executable)
  .map((event) => event.executable);
for (const binary of binaries) await run(binary, [], { env, stdio: 'inherit' });
const profiles = readdirSync(output)
  .filter((name) => name.endsWith('.profraw'))
  .map((name) => path.join(output, name));
const merged = path.join(output, 'merged.profdata');
await run(profdata, ['merge', '-sparse', ...profiles, '-o', merged]);
// Include new native modules automatically so coverage cannot silently omit them.
const sourceFiles = readdirSync(path.join(root, 'native/src'), { recursive: true })
  .filter((name) => name.endsWith('.rs'))
  .sort()
  .map((name) => path.join(root, 'native/src', name));
const reportArgs = [
  library,
  ...binaries.flatMap((binary) => ['-object', binary]),
  `-instr-profile=${merged}`,
  ...sourceFiles
];
const report = await run(cov, ['report', ...reportArgs]);
console.log(report);
writeFileSync(path.join(output, 'summary.txt'), report);
writeFileSync(path.join(output, 'coverage.json'), await run(cov, ['export', ...reportArgs]));
await run(cov, ['show', ...reportArgs, '-format=html', `-output-dir=${output}/html`]);
console.log(`Native coverage: ${output}`);
