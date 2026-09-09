import { spawn } from 'node:child_process';
import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

// Run sequentially so the benchmark processes do not compete for CPU or storage.
const sizes = process.argv.slice(2).map(Number);
if (sizes.length === 0) sizes.push(6000, 12000, 24000, 6000);
if (sizes.some((size) => !Number.isSafeInteger(size) || size <= 0)) {
  throw new Error('Usage: publication-sizes.ts [positive page sizes...]');
}
const directory = './benchmark-artifacts/json';
await mkdir(directory, { recursive: true });
const results = [];
for (const size of sizes) {
  const before = new Set(await readdir(directory));
  console.log(`\nPublication sizing: ${size} events per page`);
  const status = await new Promise<number | null>((resolve, reject) => {
    const child = spawn('pnpm', ['benchmark:changes'], {
      stdio: 'inherit',
      env: {
        ...process.env,
        BENCHMARK_ROWS: process.env.BENCHMARK_ROWS ?? '200000',
        BENCHMARK_ITERATIONS: process.env.BENCHMARK_ITERATIONS ?? '2',
        BENCHMARK_PROFILE: process.env.BENCHMARK_PROFILE ?? 'timings',
        BENCHMARK_BATCH_SIZE: String(size),
        BENCHMARK_LABEL: `${process.env.BENCHMARK_LABEL ?? 'publication-sizing'}-page-${size}`
      }
    });
    child.on('error', reject);
    child.on('exit', resolve);
  });
  if (status !== 0) throw new Error(`Benchmark failed for page size ${size} (${status})`);
  const added = (await readdir(directory)).filter((file) => !before.has(file) && file.includes('change-batches'));
  if (added.length !== 1)
    throw new Error('Expected one new result; do not run other change benchmarks during this sweep');
  const file = join(directory, added[0]);
  const result = JSON.parse(await readFile(file, 'utf8'));
  if (result.status !== 'passed') throw new Error(`Correctness failed: ${file}`);
  const counters = result.summary.counters;
  results.push({
    page_size: size,
    rows: counters.source_rows.median,
    mib_per_second: counters.logical_mib_per_second.median,
    s3_uploads: counters.s3_uploads.median,
    file
  });
  console.table(results.map(({ file, ...metrics }) => metrics));
}
const file = `./benchmark-artifacts/publication-sizing-${Date.now()}.json`;
await writeFile(file, JSON.stringify(results, null, 2) + '\n');
console.log(`Sizing summary: ${file}`);
