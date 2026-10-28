import { randomUUID } from 'node:crypto';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { BenchmarkResult } from '../types/BenchmarkResult.js';

export const artifactRoot = fileURLToPath(new URL('../../benchmark-artifacts/', import.meta.url));

export interface SuiteManifest {
  schema_version: 1;
  run_id: string;
  git_sha: string | null;
  started_at: string;
  finished_at: string | null;
  args: string[];
  tags_filter: string | null;
  status: 'running' | 'passed' | 'failed' | 'interrupted';
  exit_code: number | null;
  signal: string | null;
}

export async function atomicWrite(path: string, content: string): Promise<void> {
  const temporary = `${path}.${randomUUID()}.tmp`;
  await writeFile(temporary, content, 'utf8');
  await rename(temporary, path);
}

export async function startSuite(
  root: string,
  options: { args: string[]; git_sha: string | null; tags_filter?: string }
): Promise<{ directory: string; manifest: SuiteManifest }> {
  const runId = `${new Date().toISOString().replaceAll(':', '-')}-${randomUUID()}`;
  const directory = join(root, 'runs', runId);
  await mkdir(join(directory, 'json'), { recursive: true });
  await mkdir(join(directory, 'report'));
  const manifest: SuiteManifest = {
    schema_version: 1,
    run_id: runId,
    git_sha: options.git_sha,
    started_at: new Date().toISOString(),
    finished_at: null,
    args: options.args,
    tags_filter: options.tags_filter ?? null,
    status: 'running',
    exit_code: null,
    signal: null
  };
  await atomicWrite(join(directory, 'run.json'), JSON.stringify(manifest, null, 2));
  await atomicWrite(join(root, 'latest.json'), JSON.stringify({ run_id: runId }));
  return { directory, manifest };
}

export async function finishSuite(directory: string, exitCode: number | null, signal: string | null): Promise<void> {
  const manifest: SuiteManifest = JSON.parse(await readFile(join(directory, 'run.json'), 'utf8'));
  manifest.finished_at = new Date().toISOString();
  manifest.exit_code = exitCode;
  manifest.signal = signal;
  manifest.status = signal ? 'interrupted' : exitCode === 0 ? 'passed' : 'failed';
  await atomicWrite(join(directory, 'run.json'), JSON.stringify(manifest, null, 2));
}

export async function readSuite(
  root = artifactRoot,
  runId?: string
): Promise<{ directory: string; manifest: SuiteManifest }> {
  const selected: unknown = runId ?? JSON.parse(await readFile(join(root, 'latest.json'), 'utf8')).run_id;
  if (typeof selected !== 'string' || !/^[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*$/.test(selected)) {
    throw new Error('Invalid run ID');
  }
  const directory = join(root, 'runs', selected);
  const manifest: SuiteManifest = JSON.parse(await readFile(join(directory, 'run.json'), 'utf8'));
  if (manifest.schema_version !== 1 || manifest.run_id !== selected)
    throw new Error(`Invalid run manifest: ${directory}`);
  return { directory, manifest };
}

export async function writeBenchmarkResult(result: BenchmarkResult): Promise<void> {
  const directory = process.env.BENCHMARK_RUN_DIRECTORY;
  if (!directory) throw new Error('Run benchmarks using pnpm benchmark:test to initialize an isolated suite run');
  const filename = `${encodeURIComponent(result.scenario.id)}.json`;
  await atomicWrite(join(directory, 'json', filename), `${JSON.stringify(result)}\n`);
}
