import { execFileSync } from 'node:child_process';
import { appendFile, readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { artifactRoot, finishSuite, startSuite } from '../utils/output.js';

export async function setup() {
  const args = process.argv.slice(2);
  if (args.some((arg) => ['--list-tags', '--help', '-h', '--version', '-v'].includes(arg))) return;

  let gitSha: string | null = process.env.CUSTOM_SHA ?? null;
  if (gitSha == null) {
    try {
      gitSha = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
    } catch {
      // Git metadata is optional outside a checkout.
    }
  }

  const runIndex = args.indexOf('--run');
  const selectionArgs = runIndex === -1 ? args : args.slice(runIndex + 1);

  const suite = await startSuite(artifactRoot, {
    args: selectionArgs,
    git_sha: gitSha,
    tags_filter: process.env.BENCHMARK_TAGS_FILTER
  });

  process.env.BENCHMARK_RUN_DIRECTORY = suite.directory;
  console.info(`Benchmark suite: ${suite.manifest.run_id}`);

  if (process.env.GITHUB_OUTPUT) {
    await appendFile(process.env.GITHUB_OUTPUT, `run-id=${suite.manifest.run_id}\nrun-directory=${suite.directory}\n`);
  }

  return async () => {
    const artifacts = await readdir(join(suite.directory, 'json'));
    const results = await Promise.all(
      artifacts
        .filter((filename) => filename.endsWith('.json'))
        .map(async (filename) => JSON.parse(await readFile(join(suite.directory, 'json', filename), 'utf8')))
    );
    const vitestExitCode = typeof process.exitCode === 'number' ? process.exitCode : 0;
    const failed = results.some((result) => result.status !== 'passed') || vitestExitCode !== 0;
    const exitCode = vitestExitCode || (failed ? 1 : 0);
    const signal = exitCode === 130 ? 'SIGINT' : exitCode === 143 ? 'SIGTERM' : null;
    await finishSuite(suite.directory, exitCode, signal);
    delete process.env.BENCHMARK_RUN_DIRECTORY;
  };
}
